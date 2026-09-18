/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link RetrieverExecutor}'s fail-fast paths — the validation and safety-cap checks that
 * run <b>before</b> any {@code multiSearch} dispatch, so they need only a no-op client. The dispatch →
 * extract → resolve happy path (which requires real leg {@code SearchResponse}s with shard targets) is
 * proven end-to-end in {@code RetrieverEngineIT} on a real cluster, where mocking would only test the mock.
 */
public class RetrieverExecutorTests extends OpenSearchTestCase {

    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Fail-fast paths reject before dispatch, so a no-op client suffices (its multiSearch is never hit).
        client = new NoOpClient(getTestName());
        SearchSourceBuilderRetrieverIntegration.configureLimits(Settings.EMPTY);
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
        super.tearDown();
    }

    private Exception executeAndCaptureFailure(RetrieverBuilder root) {
        RetrieverExecutor executor = new RetrieverExecutor(
            root,
            new String[] { "idx" },
            null,
            null,
            false,
            null,
            new RetrieverResolutionContext(false),
            false,
            TimeValue.timeValueSeconds(30)
        );
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<Boolean> success = new AtomicReference<>(false);
        executor.execute(client, ActionListener.wrap(v -> success.set(true), failure::set));
        assertFalse("expected failure, not success", success.get());
        assertNotNull("expected a failure to be captured", failure.get());
        return failure.get();
    }

    public void testEmptyTreeRejected() {
        // A compound with no leaves → "no leaves".
        Exception e = executeAndCaptureFailure(new TestCompound(Collections.emptyList()));
        assertTrue(e.getMessage(), e.getMessage().contains("no leaves"));
    }

    public void testLeafCountCapExceeded() {
        SearchSourceBuilderRetrieverIntegration.configureLimits(
            Settings.builder().put(SearchSourceBuilderRetrieverIntegration.MAX_LEAF_COUNT_SETTING.getKey(), 2).build()
        );
        List<RetrieverBuilder> children = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            children.add(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        }
        Exception e = executeAndCaptureFailure(new TestCompound(children));
        assertTrue(e.getMessage(), e.getMessage().contains("max_leaf_count"));
        SearchSourceBuilderRetrieverIntegration.configureLimits(Settings.EMPTY);
    }

    public void testDepthCapExceeded() {
        SearchSourceBuilderRetrieverIntegration.configureLimits(
            Settings.builder().put(SearchSourceBuilderRetrieverIntegration.MAX_DEPTH_SETTING.getKey(), 2).build()
        );
        // depth 3: compound(compound(standard))
        RetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        RetrieverBuilder mid = new TestCompound(Collections.singletonList(leaf));
        RetrieverBuilder root = new TestCompound(Collections.singletonList(mid));
        Exception e = executeAndCaptureFailure(root);
        assertTrue(e.getMessage(), e.getMessage().contains("max_depth"));
        SearchSourceBuilderRetrieverIntegration.configureLimits(Settings.EMPTY);
    }

    public void testLeafValidationFailurePropagates() {
        // A standard leaf with no query fails validate() before any dispatch.
        RetrieverBuilder root = new TestCompound(Collections.singletonList(new StandardRetrieverBuilder()));
        Exception e = executeAndCaptureFailure(root);
        assertTrue(e.getMessage(), e.getMessage().contains("[standard] requires [query]"));
    }

    // ---- PIT lifecycle matrix (A3c) ----
    // The executor OPENS the framework PIT (state 3), sets it on the request source, and records the pit id
    // on the context — but does NOT release it (release is relocated to TransportSearchAction's wrapped
    // response listener, because the PIT must outlive tree resolution and cover the final search). So these
    // unit tests assert create / no-create / fail-closed + context wiring; the no-leak (release) behavior is
    // proven end-to-end in RetrieverPitIT.testDefaultOnCreatesAndReleasesPitNoLeak.

    public void testFrameworkPitCreatedAndRecordedOnContext() {
        PitCountingClient pitClient = new PitCountingClient(getTestName(), false);
        RetrieverResolutionContext context = new RetrieverResolutionContext(false);
        SearchRequest original = new SearchRequest("idx");
        original.source(new SearchSourceBuilder());
        RetrieverExecutor executor = executorFor(new TestCompound(Collections.singletonList(new TestLeaf())), original, context, true);
        AtomicReference<Boolean> success = new AtomicReference<>(false);
        executor.execute(pitClient, ActionListener.wrap(v -> success.set(true), e -> {}));
        assertTrue("cascade should succeed", success.get());
        assertEquals("one CreatePit", 1, pitClient.createCount.get());
        assertEquals("executor does NOT release (relocated to the transport response wrap)", 0, pitClient.deleteCount.get());
        assertEquals("pit id recorded on the context for later release", PitCountingClient.CREATED_PIT_ID, context.getFrameworkManagedPitId());
        pitClient.close();
    }

    public void testFrameworkPitFailClosedAtCeiling() {
        // CreatePit fails (e.g. too_many_pit_contexts) → fail closed: surface the error, no degrade to live readers.
        PitCountingClient pitClient = new PitCountingClient(getTestName(), true);
        RetrieverResolutionContext context = new RetrieverResolutionContext(false);
        SearchRequest original = new SearchRequest("idx");
        original.source(new SearchSourceBuilder());
        RetrieverExecutor executor = executorFor(new TestCompound(Collections.singletonList(new TestLeaf())), original, context, true);
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<Boolean> success = new AtomicReference<>(false);
        executor.execute(pitClient, ActionListener.wrap(v -> success.set(true), failure::set));
        assertFalse("must not fall back to live readers on CreatePit failure", success.get());
        assertNotNull("CreatePit failure surfaced", failure.get());
        assertEquals("CreatePit attempted", 1, pitClient.createCount.get());
        assertNull("no pit recorded when create failed", context.getFrameworkManagedPitId());
        pitClient.close();
    }

    public void testUserSuppliedPitNotTouchedByFramework() {
        PitCountingClient pitClient = new PitCountingClient(getTestName(), false);
        RetrieverResolutionContext context = new RetrieverResolutionContext(false);
        // originalRequest carries a user pit; frameworkPit=true but the user pit takes precedence.
        SearchRequest original = new SearchRequest("idx");
        original.source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("user-pit")));
        RetrieverExecutor executor = executorFor(new TestCompound(Collections.singletonList(new TestLeaf())), original, context, true);
        AtomicReference<Boolean> success = new AtomicReference<>(false);
        executor.execute(pitClient, ActionListener.wrap(v -> success.set(true), e -> {}));
        assertTrue("cascade should succeed under the user pit", success.get());
        assertEquals("framework never creates a PIT when the user supplied one", 0, pitClient.createCount.get());
        assertNull("no framework pit recorded for a user-supplied pit", context.getFrameworkManagedPitId());
        assertEquals("user pit left intact", "user-pit", original.source().pointInTimeBuilder().getId());
        pitClient.close();
    }

    public void testOptOutCreatesNoPit() {
        PitCountingClient pitClient = new PitCountingClient(getTestName(), false);
        RetrieverResolutionContext context = new RetrieverResolutionContext(false);
        SearchRequest original = new SearchRequest("idx");
        original.source(new SearchSourceBuilder());
        // frameworkPit=false (retriever_pit:false) → run on live readers, no PIT.
        RetrieverExecutor executor = executorFor(new TestCompound(Collections.singletonList(new TestLeaf())), original, context, false);
        AtomicReference<Boolean> success = new AtomicReference<>(false);
        executor.execute(pitClient, ActionListener.wrap(v -> success.set(true), e -> {}));
        assertTrue("cascade should succeed on live readers", success.get());
        assertEquals("no CreatePit when opted out", 0, pitClient.createCount.get());
        assertNull("no framework pit recorded when opted out", context.getFrameworkManagedPitId());
        assertNull("no pit set on request source when opted out", original.source().pointInTimeBuilder());
        pitClient.close();
    }

    public void testFrameworkPitSetOnRequestSourceForLegs() {
        PitCountingClient pitClient = new PitCountingClient(getTestName(), false);
        SearchRequest original = new SearchRequest("idx");
        original.source(new SearchSourceBuilder());
        RetrieverExecutor executor = executorFor(
            new TestCompound(Collections.singletonList(new TestLeaf())),
            original,
            new RetrieverResolutionContext(false),
            true
        );
        executor.execute(pitClient, ActionListener.wrap(v -> {}, e -> {}));
        // The framework-managed pit id was set on the original request source, which every leg inherits.
        assertNotNull("pit set on request source", original.source().pointInTimeBuilder());
        assertEquals(PitCountingClient.CREATED_PIT_ID, original.source().pointInTimeBuilder().getId());
        pitClient.close();
    }

    public void testFrameworkPitReleasedByExecutorOnResolutionFailure() {
        // Resolution fails AFTER the PIT is opened (a leg fails). The final search will never run, so the
        // transport-wrap release never fires — the executor MUST release the PIT itself (no leak) and clear
        // the id from the context so the transport wrap cannot double-release. This is the pressure test for
        // the resolution-failure-after-PIT-open path.
        PitCountingClient pitClient = new PitCountingClient(getTestName(), false);
        RetrieverResolutionContext context = new RetrieverResolutionContext(false);
        SearchRequest original = new SearchRequest("idx");
        original.source(new SearchSourceBuilder());
        RetrieverExecutor executor = executorFor(new TestCompound(Collections.singletonList(new FailingLeaf())), original, context, true);
        AtomicReference<Exception> failure = new AtomicReference<>();
        executor.execute(pitClient, ActionListener.wrap(v -> {}, failure::set));
        assertNotNull("resolution should fail", failure.get());
        assertEquals("PIT opened", 1, pitClient.createCount.get());
        assertEquals("executor releases the PIT on the failure path (no leak)", 1, pitClient.deleteCount.get());
        assertNull("pit id cleared so the transport wrap does not double-release", context.getFrameworkManagedPitId());
        pitClient.close();
    }

    private RetrieverExecutor executorFor(
        RetrieverBuilder root,
        SearchRequest original,
        RetrieverResolutionContext context,
        boolean frameworkPit
    ) {
        return new RetrieverExecutor(
            root,
            new String[] { "idx" },
            original,
            null,
            false,
            null,
            context,
            frameworkPit,
            TimeValue.timeValueSeconds(30)
        );
    }

    /** A minimal compound test double: N children; validates/prepares/collects recursively. */
    private static final class TestCompound extends RetrieverBuilder {
        private final List<RetrieverBuilder> children;

        TestCompound(List<RetrieverBuilder> children) {
            this.children = children;
        }

        @Override
        public List<StandardRetrieverBuilder> collectLeaves() {
            List<StandardRetrieverBuilder> leaves = new ArrayList<>();
            for (RetrieverBuilder c : children) {
                leaves.addAll(c.collectLeaves());
            }
            return leaves;
        }

        @Override
        public List<RetrieverBuilder> getChildRetrievers() {
            return children;
        }

        @Override
        public void validate() {
            for (RetrieverBuilder c : children) {
                c.validate();
            }
        }

        @Override
        public void prepareLeaves() {
            for (RetrieverBuilder c : children) {
                c.prepareLeaves();
            }
        }

        @Override
        void doResolve() {
            this.resolvedResult = Collections.emptyList();
        }

        @Override
        void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
            resolveChildren(client, indices, original, ActionListener.wrap(v -> {
                doResolve();
                whenDone.onResponse(null);
            }, whenDone::onFailure));
        }

        @Override
        public QueryBuilder toQueryBuilder() {
            return new RankDocsQueryBuilder(Collections.emptyList());
        }

        @Override
        public QueryBuilder extractAggregationQuery() {
            return new MatchAllQueryBuilder();
        }

        @Override
        public String getName() {
            return "test_compound";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    }

    /** An in-memory leaf that resolves synchronously without any client.search — isolates PIT lifecycle. */
    private static class TestLeaf extends RetrieverBuilder {
        @Override
        public List<StandardRetrieverBuilder> collectLeaves() {
            return Collections.singletonList(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        }

        @Override
        public List<RetrieverBuilder> getChildRetrievers() {
            return Collections.emptyList();
        }

        @Override
        public void validate() {}

        @Override
        public void prepareLeaves() {}

        @Override
        void doResolve() {
            this.resolvedResult = Collections.emptyList();
        }

        @Override
        void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
            doResolve();
            whenDone.onResponse(null);
        }

        @Override
        public QueryBuilder toQueryBuilder() {
            return new RankDocsQueryBuilder(Collections.emptyList());
        }

        @Override
        public QueryBuilder extractAggregationQuery() {
            return new MatchAllQueryBuilder();
        }

        @Override
        public String getName() {
            return "test_leaf";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    }

    /** A leaf that fails during resolve — to exercise the resolution-failure-after-PIT-open release path. */
    private static final class FailingLeaf extends TestLeaf {
        @Override
        void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
            whenDone.onFailure(new IllegalStateException("injected leg failure"));
        }
    }

    /**
     * A client that counts {@link CreatePitAction}/{@link DeletePitAction} executions (and can simulate a
     * CreatePit failure at the context ceiling). All other executes are no-ops (the in-memory leaves never
     * call {@code client.search}).
     */
    private static final class PitCountingClient extends NoOpClient {
        static final String CREATED_PIT_ID = "framework-pit-1";
        final AtomicInteger createCount = new AtomicInteger();
        final AtomicInteger deleteCount = new AtomicInteger();
        private final boolean failCreate;

        PitCountingClient(String testName, boolean failCreate) {
            super(testName);
            this.failCreate = failCreate;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == CreatePitAction.INSTANCE) {
                createCount.incrementAndGet();
                if (failCreate) {
                    listener.onFailure(new OpenSearchException("too_many_pit_contexts"));
                } else {
                    listener.onResponse((Response) new CreatePitResponse(CREATED_PIT_ID, 0L, 1, 1, 0, 0, new ShardSearchFailure[0]));
                }
                return;
            }
            if (action == DeletePitAction.INSTANCE) {
                deleteCount.incrementAndGet();
                listener.onResponse(null);
                return;
            }
            listener.onResponse(null);
        }
    }
}
