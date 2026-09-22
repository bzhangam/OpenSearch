/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Integration test that {@code search.retriever.max_concurrent_leg_searches} actually serializes a
 * request's leg searches. With the cap set to 1, no two leg query phases may overlap; a
 * {@link SearchOperationListener} records the maximum number of concurrent leg query phases observed,
 * which must be exactly 1.
 * <p>
 * Legs are the user's queries; the final {@code RankDocsQuery} search is skipped by the counter (it is
 * not a leg). To make overlap observable without it, each leg query phase briefly blocks on a barrier so
 * that, absent the cap, they WOULD overlap — the cap is what forces max-concurrency 1.
 */
public class RankFusionConcurrencyIT extends AbstractRetrieverIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchSourceBuilderRetrieverIntegration.MAX_CONCURRENT_LEG_SEARCHES_SETTING.getKey(), 1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(LegConcurrencyProbePlugin.class);
        return plugins;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LegConcurrencyProbePlugin.reset();
    }

    @Override
    public void tearDown() throws Exception {
        LegConcurrencyProbePlugin.reset();
        super.tearDown();
    }

    public void testMaxConcurrentLegSearchesSerializesLegs() throws Exception {
        // Single shard so each leg is exactly one query phase (deterministic concurrency counting).
        createProducts(1);
        LegConcurrencyProbePlugin.arm(2); // two legs expected
        String body = "{\"retriever\":{\"rank_fusion\":{\"retrievers\":["
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}"
            + "]}},\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch(INDEX).setSearchType(SearchType.QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        assertTrue("both legs should have run", LegConcurrencyProbePlugin.legsSeen() >= 2);
        assertEquals("cap 1 → at most one leg query phase concurrent", 1, LegConcurrencyProbePlugin.maxConcurrent());
    }

    /**
     * Records the maximum number of leg query phases running concurrently. Each leg briefly waits on a
     * short barrier so that, without the concurrency cap, two legs would overlap and the observed max would
     * be 2; with the cap at 1 it stays 1. The final RankDocsQuery search is excluded (not a leg).
     */
    public static class LegConcurrencyProbePlugin extends Plugin {
        private static volatile boolean armed = false;
        private static final AtomicInteger current = new AtomicInteger();
        private static final AtomicInteger maxConcurrent = new AtomicInteger();
        private static final AtomicInteger legsSeen = new AtomicInteger();

        static void arm(int expectedLegs) {
            armed = true;
            current.set(0);
            maxConcurrent.set(0);
            legsSeen.set(0);
        }

        static void reset() {
            armed = false;
            current.set(0);
            maxConcurrent.set(0);
            legsSeen.set(0);
        }

        static int maxConcurrent() {
            return maxConcurrent.get();
        }

        static int legsSeen() {
            return legsSeen.get();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    if (armed == false || searchContext.query() instanceof RankDocsQuery) {
                        return; // only count leg query phases
                    }
                    legsSeen.incrementAndGet();
                    int now = current.incrementAndGet();
                    maxConcurrent.accumulateAndGet(now, Math::max);
                    try {
                        // Hold the phase briefly. If two legs ran concurrently, `current` would reach 2
                        // during this window. Under cap 1 the second leg hasn't been dispatched yet, so it
                        // can't be here. Bounded, short — never blocks the whole suite.
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        current.decrementAndGet();
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
