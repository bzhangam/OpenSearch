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
import org.opensearch.action.search.SearchAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.FilterClient;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A per-request {@link Client} wrapper that bounds how many of a retriever's <b>leg searches</b>
 * ({@link SearchAction}) are in flight at once — the {@code search.retriever.max_concurrent_leg_searches}
 * cap. Only leg searches are gated; all other actions (e.g. {@code CreatePit}/{@code DeletePit}) pass
 * straight through.
 * <p>
 * <b>Non-blocking gate (never parks a search thread).</b> This is deliberately <b>not</b> a blocking
 * {@code Semaphore.acquire()} — blocking on a search-response thread while waiting for a permit would
 * violate the framework's "no thread blocked for async ops" invariant and could deadlock the search
 * threadpool. Instead: if a permit is free, the search dispatches immediately; if not, it is <b>enqueued</b>
 * as a deferred task. When an in-flight leg completes it releases its permit and drains the next queued
 * search. All permit bookkeeping is under a short intrinsic lock; no I/O happens under the lock.
 * <p>
 * A cap of {@code 0} (or negative) means <b>unbounded</b> — the wrapper passes everything through
 * immediately (today's behavior when the cap is unset). Scope is one request: a fresh limiter wraps the
 * client for each retriever request, so one request cannot starve another's legs.
 *
 * @opensearch.internal
 */
final class LegConcurrencyLimitingClient extends FilterClient {

    private final int maxConcurrent;
    private final Object lock = new Object();
    private int inFlight = 0;
    private final Queue<Runnable> pending = new ArrayDeque<>();

    LegConcurrencyLimitingClient(Client in, int maxConcurrent) {
        super(in);
        this.maxConcurrent = maxConcurrent;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        // Only leg searches are throttled; everything else (PIT create/delete, etc.) passes through.
        if (maxConcurrent <= 0 || action != SearchAction.INSTANCE) {
            super.doExecute(action, request, listener);
            return;
        }

        // Release the permit on completion (success or failure) and drain the next queued search.
        ActionListener<Response> releasing = ActionListener.runAfter(listener, this::releaseAndDrain);

        Runnable dispatch = () -> super.doExecute(action, request, releasing);

        synchronized (lock) {
            if (inFlight < maxConcurrent) {
                inFlight++;
                // fall through to dispatch outside the lock
            } else {
                pending.add(dispatch);
                return;
            }
        }
        dispatch.run();
    }

    private void releaseAndDrain() {
        Runnable next;
        synchronized (lock) {
            next = pending.poll();
            if (next == null) {
                inFlight--;
                return;
            }
            // Hand the released permit directly to the next queued search (inFlight stays the same).
        }
        next.run();
    }
}
