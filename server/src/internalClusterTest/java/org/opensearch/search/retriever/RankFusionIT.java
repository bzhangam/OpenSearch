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
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Integration tests for {@code rank_fusion} on a real multi-shard cluster — the first genuine
 * multi-leg tree. Reuses the shared corpus/helpers from {@link AbstractRetrieverIT}.
 * <p>
 * RRF is rank-based: for each leg a doc contributes {@code 1/(rank_constant + rank)} (1-based rank in
 * that leg), summed across legs, keyed by {@code (index,_id)}. These ITs use two legs with deliberately
 * different rankings so the fused order is hand-computable and differs from either leg alone.
 * <p>
 * <b>Parity vs {@code hybrid}+RRF (plan RF-IT-3) is deferred</b>: the {@code hybrid} query lives in the
 * neural-search plugin, which is not on core's internalClusterTest classpath. The RRF math itself is
 * pinned deterministically in {@code RankFusionRetrieverBuilderTests}; cross-checking against the
 * {@code hybrid}+RRF pipeline belongs in a neural-search IT and is tracked there.
 */
public class RankFusionIT extends AbstractRetrieverIT {

    private SearchResponse rankFusionSearch(String body, SearchType searchType) throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        return client().prepareSearch(INDEX).setSearchType(searchType).setSource(source).get();
    }

    /** rank_fusion of two standard legs (bm25 on title, term on brand), with the given tail fields. */
    private static String twoLegRankFusion(String tail) {
        return "{\"retriever\":{\"rank_fusion\":{\"retrievers\":["
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}"
            + "]"
            + tail
            + "}},\"size\":10}";
    }

    public void testTwoLegRrfFusionReturnsUnionRanked() throws Exception {
        createProducts(3);
        // Leg 1 (title:headphones) matches a, b, d, f. Leg 2 (brand:acme) matches a, b, e.
        // RRF fuses the union {a, b, d, e, f}; a and b appear in BOTH legs so they must outrank
        // docs that appear in only one leg (d, e, f). Exact per-rank order depends on bm25 scoring
        // within a leg, but the both-legs docs (a, b) ranking above the single-leg docs is deterministic.
        SearchResponse r = rankFusionSearch(twoLegRankFusion(""), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals("union of both legs: a,b,d,e,f", 5, ids.size());
        // a and b (in both legs) rank above d, e, f (each in one leg only).
        int posA = ids.indexOf("a");
        int posB = ids.indexOf("b");
        for (String single : List.of("d", "e", "f")) {
            int p = ids.indexOf(single);
            assertTrue("[a] (both legs) outranks single-leg [" + single + "]", posA < p);
            assertTrue("[b] (both legs) outranks single-leg [" + single + "]", posB < p);
        }
    }

    public void testMultiNodeFusion() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        createProducts(3);
        // Same fusion, candidates spread across 3 nodes; coordinator reassembles + fuses correctly.
        SearchResponse r = rankFusionSearch(twoLegRankFusion(""), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals(5, ids.size());
        assertTrue(ids.contains("a") && ids.contains("b") && ids.contains("e"));
    }

    public void testRankWindowSizeTruncatesFusedOutput() throws Exception {
        createProducts(3);
        // rank_window_size governs BOTH each leg's fetch depth AND the final fused truncation. With a
        // window of 2, each leg contributes only its top-2 candidates, so which documents reach fusion is
        // bm25/tie-break dependent (the brand:acme term leg scores a, b, e identically, so its top-2 is an
        // arbitrary 2 of them). The deterministic, cluster-independent guarantees of truncation are:
        // (1) the fused output is capped at rank_window_size (2), even though top-level size is 10;
        // (2) over-reading (size 10 > window 2) returns the available slice, not a 400 / shard failure;
        // (3) every returned document is a real member of the union of the two legs' matches.
        // Which specific docs survive a size-2 window is NOT asserted — it is not a stable property. The
        // both-legs-outrank-single-leg ordering is covered by testTwoLegRrfFusionReturnsUnionRanked (which
        // uses the default window, so both legs contribute their full match set).
        SearchResponse r = rankFusionSearch(twoLegRankFusion(",\"rank_window_size\":2"), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        assertEquals("fused window capped at 2", 2, r.getHits().getHits().length);
        List<String> ids = ids(r);
        List<String> union = List.of("a", "b", "d", "e", "f"); // title:headphones {a,b,d,f} ∪ brand:acme {a,b,e}
        for (String id : ids) {
            assertTrue("returned doc [" + id + "] is a member of the fused union", union.contains(id));
        }
    }

    public void testFusedMinScoreDropsLowScorers() throws Exception {
        createProducts(3);
        // A fused min_score above the single-leg RRF term (1/(60+rank)) but below the both-legs sum keeps
        // only a and b. 1/(60+1)=0.0164 (best single-leg); a both-legs doc sums two such terms (~0.03+).
        SearchResponse r = rankFusionSearch(twoLegRankFusion(",\"min_score\":0.02"), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertTrue("both-legs docs a,b retained", ids.contains("a") && ids.contains("b"));
        assertFalse("single-leg d dropped below fused min_score", ids.contains("d"));
        assertFalse("single-leg e dropped below fused min_score", ids.contains("e"));
        assertFalse("single-leg f dropped below fused min_score", ids.contains("f"));
    }

    public void testNestedRankFusionResolvesBottomUp() throws Exception {
        createProducts(3);
        // rank_fusion( rank_fusion(bm25, brand:acme), standard(title:earbuds) ) — arbitrary nesting.
        String body = "{\"retriever\":{\"rank_fusion\":{\"retrievers\":["
            + "{\"rank_fusion\":{\"retrievers\":["
            + "  {\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "  {\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}},"
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"earbuds\"}}}}"
            + "]}},\"size\":10}";
        SearchResponse r = rankFusionSearch(body, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        // union across the nested tree: headphones {a,b,d,f} ∪ acme {a,b,e} ∪ earbuds {c}
        assertTrue("nested fusion reaches c (earbuds leg)", ids.contains("c"));
        assertTrue(ids.contains("a") && ids.contains("b"));
    }

    public void testCrossIndexIdKeyedDistinct() throws Exception {
        createProducts(3);
        // A second index sharing an _id with the first; (index,_id) keying keeps them distinct through fusion.
        client().admin().indices().prepareCreate("more").setMapping("title", "type=text", "brand", "type=keyword").get();
        client().prepareIndex("more").setId("a").setSource("title", "acme headphones deluxe", "brand", "acme").get();
        client().admin().indices().prepareRefresh("more").get();

        String body = "{\"retriever\":{\"rank_fusion\":{\"retrievers\":["
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}"
            + "]}},\"size\":20}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch(INDEX, "more").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        // _id "a" exists in BOTH products and more → two distinct hits, not merged into one.
        long countA = java.util.Arrays.stream(r.getHits().getHits()).filter(h -> h.getId().equals("a")).count();
        assertEquals("same _id in two indices stays distinct through fusion", 2, countA);
    }
}
