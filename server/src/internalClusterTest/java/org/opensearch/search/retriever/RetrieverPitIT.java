/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Integration tests for framework-managed PIT (A3c) on a real multi-shard cluster. Kept in their own
 * class (decoupled from {@link RetrieverEngineIT}) and reusing the shared corpus/helpers from
 * {@link AbstractRetrieverIT}.
 * <p>
 * These prove PIT actually <b>works</b>, not merely that the code path doesn't error:
 * <ul>
 *   <li><b>Observability</b> — a default request opens a framework PIT and releases it (open-PIT gauge
 *       {@code search.pitCurrent} is 0 before and back to 0 after: no leak).</li>
 *   <li><b>Snapshot consistency (the point of PIT)</b> — a matched doc deleted+refreshed <b>between</b>
 *       the leg round and the final {@code RankDocsQuery} fetch is still returned under the default PIT,
 *       but <b>drops</b> with {@code retriever_pit:false}. The contrast is the proof the PIT froze the
 *       snapshot for the final fetch (not just the legs).</li>
 *   <li><b>User PIT honored</b> — an explicit user PIT is used and not deleted by the framework.</li>
 * </ul>
 * <b>Deterministic interception.</b> A test plugin's {@link SearchOperationListener} blocks the <b>leg</b>
 * query phase on a latch and signals the test thread; the <b>test thread</b> (not a search thread)
 * performs the delete+refresh and then releases the latch. This makes the mid-request change land
 * deterministically between the leg round and the final fetch, without doing a blocking write on the
 * search threadpool. The release-exactly-once / fail-closed lifecycle matrix is proven at the unit level
 * ({@code RetrieverExecutorTests}).
 */
public class RetrieverPitIT extends AbstractRetrieverIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(LegBarrierPlugin.class);
        return plugins;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LegBarrierPlugin.reset();
    }

    @Override
    public void tearDown() throws Exception {
        LegBarrierPlugin.reset();
        // Never let a leaked/lingering PIT bleed into another test in this shared-cluster class: delete all
        // PITs and wait for the open-PIT gauge to return to 0 before the next test starts.
        try {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest("_all")).actionGet();
        } catch (Exception ignored) {
            // fall through to the gauge wait below
        }
        try {
            assertBusy(() -> assertEquals(0L, openPitCount()), 30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // don't mask a real test failure with a teardown assertion; the per-test before-check guards dirtiness
        }
        super.tearDown();
    }

    private SearchResponse retrieverSearchWithRawBody(String rawBody, SearchType searchType) throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, rawBody));
        return client().prepareSearch(INDEX).setSearchType(searchType).setSource(source).get();
    }

    /** Open-PIT gauge for the index: search.pitCurrent aggregated over all shards (0 == none open). */
    private long openPitCount() {
        IndicesStatsRequest request = new IndicesStatsRequest().indices(INDEX);
        request.all();
        IndicesStatsResponse stats = client().admin().indices().stats(request).actionGet();
        if (stats.getIndex(INDEX) == null) {
            return 0L; // index not present (e.g. wiped between tests) → no open PITs on it
        }
        return stats.getIndex(INDEX).getTotal().search.getTotal().getPitCurrent();
    }

    public void testDefaultOnCreatesAndReleasesPitNoLeak() throws Exception {
        createProducts(3);
        assertBusy(() -> assertEquals("no PITs open before the request", 0L, openPitCount()));
        // Default path: the framework opens a short-lived PIT, runs the leg + final RankDocsQuery fetch
        // under it, then releases it.
        //
        // INPUT: {"retriever":{"standard":{"query":{"match":{"title":"headphones"}}}},"size":10}
        // OUTPUT: 0 failed shards; 4 hits (a,b,d,f); and NO PIT leaked (pitCurrent back to 0 afterward).
        SearchResponse r = retrieverSearch("{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}", 10, SearchType.QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        assertEquals(4, r.getHits().getHits().length);
        // Release is fire-and-forget after the response is built, so assertBusy for the gauge to return to 0.
        assertBusy(() -> assertEquals("framework PIT released after the request (no leak)", 0L, openPitCount()));
    }

    public void testSnapshotConsistencyDefaultRetainsMidRequestDeletedDoc() throws Exception {
        createProducts(3);
        // Under the DEFAULT framework PIT, a doc deleted+refreshed between the leg round and the final
        // fetch is STILL returned — the final fetch reads the frozen snapshot.
        //
        // INPUT: {"retriever":{"standard":{"query":{"match":{"title":"headphones"}}}},"size":10}
        //   with doc "d" deleted+refreshed (on the test thread) between the leg and the final fetch.
        // OUTPUT: doc "d" retained → 4 hits including "d".
        SearchResponse r = runWithMidRequestDelete(
            "{\"retriever\":{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},\"size\":10}",
            "d"
        );
        assertEquals(0, r.getFailedShards());
        assertTrue("doc [d] retained under the frozen PIT snapshot", ids(r).contains("d"));
        assertEquals("all 4 headphones docs retained (PIT froze the snapshot)", 4, r.getHits().getHits().length);
    }

    public void testSnapshotConsistencyOptOutDropsMidRequestDeletedDoc() throws Exception {
        createProducts(3);
        // Same mid-request delete, but retriever_pit:false → the final fetch runs on a LIVE reader, so the
        // deleted doc is gone. The contrast with the test above proves the PIT (not something else) is the cause.
        //
        // INPUT: {"retriever":{"standard":{"query":{"match":{"title":"headphones"}}}},"retriever_pit":false,"size":10}
        //   with doc "d" deleted+refreshed (on the test thread) between the leg and the final fetch.
        // OUTPUT: doc "d" absent → 3 hits.
        SearchResponse r = runWithMidRequestDelete(
            "{\"retriever\":{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},\"retriever_pit\":false,\"size\":10}",
            "d"
        );
        assertEquals(0, r.getFailedShards());
        assertFalse("doc [d] dropped on the live reader (no PIT)", ids(r).contains("d"));
        assertEquals("only the 3 surviving headphones docs", 3, r.getHits().getHits().length);
    }

    /**
     * Issue the retriever search asynchronously; when the plugin signals the leg query phase has started,
     * delete + refresh {@code targetId} on the <b>test thread</b>, then release the leg so the final fetch
     * proceeds. Returns the final response. The delete deterministically lands between the leg and the
     * final {@code RankDocsQuery} fetch.
     */
    private SearchResponse runWithMidRequestDelete(String rawBody, String targetId) throws Exception {
        LegBarrierPlugin.arm();
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, rawBody));
        SearchRequest request = new SearchRequest(INDEX).searchType(SearchType.QUERY_THEN_FETCH).source(source);

        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        client().search(request, ActionListener.wrap(resp -> {
            responseRef.set(resp);
            done.countDown();
        }, e -> {
            failureRef.set(e);
            done.countDown();
        }));

        // Wait until a leg query phase has started, then mutate on the TEST thread and release the leg.
        assertTrue("leg query phase should have started", LegBarrierPlugin.awaitLegStarted(60));
        client().prepareDelete(INDEX, targetId).get();
        refresh();
        LegBarrierPlugin.releaseLeg();

        assertTrue("search should complete", done.await(60, TimeUnit.SECONDS));
        if (failureRef.get() != null) {
            throw failureRef.get();
        }
        return responseRef.get();
    }

    public void testExplicitUserPitHonoredAndNotDeletedByFramework() throws Exception {
        createProducts(3);
        // A user opens their own PIT and passes it. The framework uses it and must NOT delete it — the
        // test deletes it afterward, proving it was still valid post-request.
        CreatePitResponse pit = client().execute(
            CreatePitAction.INSTANCE,
            new CreatePitRequest(TimeValue.timeValueMinutes(1), false, INDEX)
        ).actionGet();
        try {
            String body = "{\"retriever\":{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
                + "\"pit\":{\"id\":\""
                + pit.getId()
                + "\",\"keep_alive\":\"1m\"},\"size\":10}";
            SearchResponse r = retrieverSearchWithRawBody(body, SearchType.QUERY_THEN_FETCH);
            assertEquals(0, r.getFailedShards());
            assertEquals(4, r.getHits().getHits().length);
        } finally {
            DeletePitResponse deleteResponse = client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pit.getId()))
                .actionGet();
            assertFalse("user PIT should still exist (framework must not delete it)", deleteResponse.getDeletePitResults().isEmpty());
            assertTrue(
                "user PIT delete should succeed (it was still valid)",
                deleteResponse.getDeletePitResults().get(0).isSuccessful()
            );
        }
    }

    /**
     * A test plugin that turns the <b>leg</b> query phase into a barrier: when armed, on the first
     * non-{@link RankDocsQuery} query phase (the leg, not the final fetch) it signals {@code legStarted}
     * and blocks until the test thread calls {@link #releaseLeg()}. The delete is performed by the test
     * thread — never on the search thread — so there is no blocking write on the search threadpool.
     */
    public static class LegBarrierPlugin extends Plugin {
        private static final AtomicReference<CountDownLatch> legStarted = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> proceed = new AtomicReference<>();
        private static volatile boolean armed = false;
        private static volatile boolean fired = false;

        static void reset() {
            armed = false;
            fired = false;
            legStarted.set(new CountDownLatch(1));
            proceed.set(new CountDownLatch(1));
        }

        static void arm() {
            armed = true;
            fired = false;
            legStarted.set(new CountDownLatch(1));
            proceed.set(new CountDownLatch(1));
        }

        static boolean awaitLegStarted(long seconds) throws InterruptedException {
            return legStarted.get().await(seconds, TimeUnit.SECONDS);
        }

        static void releaseLeg() {
            proceed.get().countDown();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    if (armed == false) {
                        return;
                    }
                    // The final fetch runs a RankDocsQuery; the leg runs the user's query. Only barrier the
                    // leg — and only once — so the test thread can mutate before the final fetch runs.
                    if (searchContext.query() instanceof RankDocsQuery) {
                        return;
                    }
                    synchronized (LegBarrierPlugin.class) {
                        if (fired) {
                            return;
                        }
                        fired = true;
                    }
                    legStarted.get().countDown();
                    try {
                        // Bounded wait — the test thread releases after its delete+refresh.
                        proceed.get().await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
