/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.transport.client.Client;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link LegConcurrencyLimitingClient}: the non-blocking leg-concurrency gate honors the
 * cap (a queued search does not dispatch until an in-flight one completes), passes non-search actions
 * through ungated, and is unbounded at cap 0.
 */
public class LegConcurrencyLimitingClientTests extends OpenSearchTestCase {

    /**
     * A client that records how many SearchActions have been dispatched to it, and holds their listeners
     * open (never completing them) until the test explicitly completes one — so we can observe how many
     * the limiter let through concurrently.
     */
    private static final class RecordingClient extends NoOpClient {
        final AtomicInteger searchesDispatched = new AtomicInteger();
        final AtomicInteger otherDispatched = new AtomicInteger();
        final Deque<ActionListener<?>> openSearchListeners = new ArrayDeque<>();

        RecordingClient(String testName) {
            super(testName);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == SearchAction.INSTANCE) {
                searchesDispatched.incrementAndGet();
                openSearchListeners.add(listener); // hold it open — do not complete
            } else {
                otherDispatched.incrementAndGet();
                listener.onResponse(null);
            }
        }

        @SuppressWarnings("unchecked")
        void completeOneSearch() {
            ActionListener<Object> l = (ActionListener<Object>) openSearchListeners.poll();
            assertNotNull("expected an in-flight search to complete", l);
            l.onResponse(null); // triggers the limiter's release + drain
        }
    }

    public void testCapHonoredAndDrained() {
        RecordingClient recording = new RecordingClient(getTestName());
        Client limiter = new LegConcurrencyLimitingClient(recording, 1);

        // Fire 3 leg searches through the cap-1 limiter.
        for (int i = 0; i < 3; i++) {
            limiter.search(new SearchRequest("idx"), ActionListener.wrap(r -> {}, e -> {}));
        }
        // Only 1 may be in flight at the cap.
        assertEquals("cap 1 → only 1 dispatched", 1, recording.searchesDispatched.get());

        // Completing the in-flight one drains exactly one queued search.
        recording.completeOneSearch();
        assertEquals("draining lets the 2nd through", 2, recording.searchesDispatched.get());
        recording.completeOneSearch();
        assertEquals("draining lets the 3rd through", 3, recording.searchesDispatched.get());
        recording.completeOneSearch(); // queue empty now; no more dispatch
        assertEquals(3, recording.searchesDispatched.get());

        recording.close();
    }

    public void testCapTwoAllowsTwoConcurrent() {
        RecordingClient recording = new RecordingClient(getTestName());
        Client limiter = new LegConcurrencyLimitingClient(recording, 2);
        for (int i = 0; i < 5; i++) {
            limiter.search(new SearchRequest("idx"), ActionListener.wrap(r -> {}, e -> {}));
        }
        assertEquals("cap 2 → 2 in flight", 2, recording.searchesDispatched.get());
        recording.completeOneSearch();
        assertEquals(3, recording.searchesDispatched.get());
        recording.close();
    }

    public void testNonSearchActionsPassThroughUngated() {
        RecordingClient recording = new RecordingClient(getTestName());
        Client limiter = new LegConcurrencyLimitingClient(recording, 1);
        // A search occupies the single permit...
        limiter.search(new SearchRequest("idx"), ActionListener.wrap(r -> {}, e -> {}));
        assertEquals(1, recording.searchesDispatched.get());
        // ...but a non-search action (e.g. CreatePit) is NOT gated and dispatches immediately.
        limiter.execute(CreatePitAction.INSTANCE, new CreatePitRequest(null, false, "idx"), ActionListener.wrap(r -> {}, e -> {}));
        assertEquals("non-search passes through even with the search permit taken", 1, recording.otherDispatched.get());
        recording.close();
    }

    public void testCapZeroIsUnbounded() {
        RecordingClient recording = new RecordingClient(getTestName());
        Client limiter = new LegConcurrencyLimitingClient(recording, 0);
        for (int i = 0; i < 4; i++) {
            limiter.search(new SearchRequest("idx"), ActionListener.wrap(r -> {}, e -> {}));
        }
        assertEquals("cap 0 → unbounded, all dispatched", 4, recording.searchesDispatched.get());
        recording.close();
    }
}
