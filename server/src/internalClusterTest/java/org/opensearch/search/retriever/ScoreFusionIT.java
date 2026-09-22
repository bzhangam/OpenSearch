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
 * Integration tests for {@code score_fusion} on a real multi-shard cluster. score_fusion min-max normalizes
 * each leg's {@code _score}s, then combines them with a weighted arithmetic mean over the legs that ranked a
 * document, keyed by {@code (index,_id)}. These ITs use two legs with overlapping matches so the fused order
 * is deterministic in its structural properties (both-legs docs outrank single-leg docs), and a weights test
 * shows re-weighting shifts the ranking.
 * <p>
 * Exact normalization/combination arithmetic is pinned deterministically in
 * {@code ScoreFusionRetrieverBuilderTests}; parity against the {@code hybrid}+{@code min_max} pipeline lives
 * in a neural-search IT (the {@code hybrid} query is not on core's internalClusterTest classpath).
 */
public class ScoreFusionIT extends AbstractRetrieverIT {

    private SearchResponse scoreFusionSearch(String body, SearchType searchType) throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        return client().prepareSearch(INDEX).setSearchType(searchType).setSource(source).get();
    }

    /** score_fusion of two standard legs (bm25 on title, term on brand), with the given tail fields. */
    private static String twoLegScoreFusion(String tail) {
        return "{\"retriever\":{\"score_fusion\":{\"retrievers\":["
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}"
            + "]"
            + tail
            + "}},\"size\":10}";
    }

    public void testTwoLegScoreFusionReturnsUnion() throws Exception {
        createProducts(3);
        // Leg 1 (title:headphones) matches a, b, d, f. Leg 2 (brand:acme, a term query) matches a, b, e.
        // The fused output is the union {a,b,d,e,f}.
        SearchResponse r = scoreFusionSearch(twoLegScoreFusion(""), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals("union of both legs: a,b,d,e,f", 5, ids.size());
        assertTrue(ids.containsAll(List.of("a", "b", "d", "e", "f")));
    }

    public void testMinMaxNormalizationPutsLegMaxAtOneAndLegMinAtFloor() throws Exception {
        createProducts(3);
        // Verified leg scores on this corpus:
        // brand leg (term:acme): a=b=e equal -> all normalize to 1.0 (all-equal case)
        // title leg (bm25): a=b=f are the max -> 1.0 ; d is the min -> normalized 0 -> floored 0.001
        // Fused = weighted mean over ALL legs (equal weights, divide by 2; absent leg contributes 0):
        // a: (1.0 + 1.0)/2 = 1.0 (both legs)
        // b: (1.0 + 1.0)/2 = 1.0 (both legs)
        // e: (0 + 1.0)/2 = 0.5 (brand only)
        // f: (1.0 + 0)/2 = 0.5 (title only, leg max)
        // d: (0.001 + 0)/2 = 0.0005 (title only, floored leg min)
        SearchResponse r = scoreFusionSearch(twoLegScoreFusion(""), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        java.util.Map<String, Float> scores = scoreById(r);
        assertEquals("both-legs doc a", 1.0f, scores.get("a"), 1e-4f);
        assertEquals("both-legs doc b", 1.0f, scores.get("b"), 1e-4f);
        assertEquals("brand-only e -> 0.5", 0.5f, scores.get("e"), 1e-4f);
        assertEquals("title-only leg-max f -> 0.5", 0.5f, scores.get("f"), 1e-4f);
        assertEquals("title-only floored min d -> 0.0005", 0.0005f, scores.get("d"), 1e-4f);
        // both-legs docs (1.0) rank above single-leg docs (0.5), d last.
        List<String> ids = ids(r);
        assertTrue(ids.indexOf("a") < ids.indexOf("e"));
        assertTrue(ids.indexOf("b") < ids.indexOf("f"));
        assertEquals("d is last", "d", ids.get(ids.size() - 1));
    }

    public void testWeightsShiftRanking() throws Exception {
        createProducts(3);
        // Up-weight the brand leg (weights [1,10], total 11). e is brand-only:
        // equal weights -> e = (0 + 1.0)/2 = 0.5 ; up-weighted -> e = (1*0 + 10*1.0)/11 = 0.909
        // d is title-only floored: (1*0.001 + 10*0)/11 = ~0.00009. e clearly outranks d, and the boost
        // lifts brand-only e above title-only f (which drops to (1*1.0 + 10*0)/11 = 0.0909).
        String body = twoLegScoreFusion(",\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[1.0,10.0]}}");
        SearchResponse r = scoreFusionSearch(body, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertTrue("brand-weighted e outranks title-only f", ids.indexOf("e") < ids.indexOf("f"));
        assertTrue("brand-weighted e outranks title-only d", ids.indexOf("e") < ids.indexOf("d"));
    }

    public void testMultiNodeFusion() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        createProducts(3);
        SearchResponse r = scoreFusionSearch(twoLegScoreFusion(""), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals(5, ids.size());
        assertTrue(ids.contains("a") && ids.contains("b") && ids.contains("e"));
    }

    public void testRankWindowSizeTruncatesFusedOutput() throws Exception {
        createProducts(3);
        SearchResponse r = scoreFusionSearch(twoLegScoreFusion(",\"rank_window_size\":2"), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        assertEquals("fused window capped at 2", 2, r.getHits().getHits().length);
        // The window keeps the top-2 by fused score; d (the unique 0.001 doc) must NOT be among them.
        assertFalse("lowest doc d is truncated out of a size-2 window", ids(r).contains("d"));
    }

    public void testFusedMinScoreDropsLowScorers() throws Exception {
        createProducts(3);
        // d fuses to 0.001; a fused min_score of 0.5 drops d and keeps the 1.0-scoring docs.
        SearchResponse r = scoreFusionSearch(twoLegScoreFusion(",\"min_score\":0.5"), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertFalse("floored doc d dropped below fused min_score", ids.contains("d"));
        assertTrue("1.0-scoring docs retained", ids.contains("a") && ids.contains("e"));
    }

    public void testNestedScoreFusionResolvesBottomUp() throws Exception {
        createProducts(3);
        String body = "{\"retriever\":{\"score_fusion\":{\"retrievers\":["
            + "{\"score_fusion\":{\"retrievers\":["
            + "  {\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "  {\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}},"
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"earbuds\"}}}}"
            + "]}},\"size\":10}";
        SearchResponse r = scoreFusionSearch(body, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertTrue("nested fusion reaches c (earbuds leg)", ids.contains("c"));
        assertTrue(ids.contains("a") && ids.contains("b"));
    }
}
