/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Integration tests for retriever {@code explain} on a real multi-shard cluster. When {@code explain: true}
 * is set, each hit's {@code _explanation} is the coordinator-assembled tree: a leaf passes through the
 * Lucene BM25 explanation; a compound describes its fusion with the per-leg contributions (and the leaf
 * explanations) nested beneath. These tests assert the tree <b>structure</b> and the invariant that the
 * root explanation value equals the hit's {@code _score}; exact BM25/RRF/score-fusion arithmetic is pinned
 * in the unit tests ({@link RetrieverExplainTests} and the per-type builder tests).
 */
public class RetrieverExplainIT extends AbstractRetrieverIT {

    private SearchResponse explainSearch(String retrieverBody, int size, SearchType searchType) throws IOException {
        String body = "{\"retriever\":" + retrieverBody + ",\"explain\":true,\"size\":" + size + "}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        return client().prepareSearch(INDEX).setSearchType(searchType).setSource(source).get();
    }

    private static final String RANK_FUSION_TWO_LEG = "{\"rank_fusion\":{\"retrievers\":["
        + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
        + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}}";

    private static final String SCORE_FUSION_TWO_LEG = "{\"score_fusion\":{\"retrievers\":["
        + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
        + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}],"
        + "\"normalization\":{\"technique\":\"min_max\"},"
        + "\"combination\":{\"technique\":\"arithmetic_mean\"}}}";

    private static Explanation explanationOf(SearchResponse r, String id) {
        for (SearchHit h : r.getHits().getHits()) {
            if (h.getId().equals(id)) {
                return h.getExplanation();
            }
        }
        throw new AssertionError("id not found: " + id);
    }

    private static float scoreOf(SearchResponse r, String id) {
        for (SearchHit h : r.getHits().getHits()) {
            if (h.getId().equals(id)) {
                return h.getScore();
            }
        }
        throw new AssertionError("id not found: " + id);
    }

    /** EX-IT-1: a standard leg's _explanation is the real Lucene BM25 tree. */
    public void testStandardExplainPassesThroughLuceneTree() throws Exception {
        createProducts(3);
        SearchResponse r = explainSearch(
            "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}",
            10,
            SearchType.DFS_QUERY_THEN_FETCH
        );
        assertEquals(0, r.getFailedShards());
        assertTrue("has hits", r.getHits().getHits().length > 0);
        for (SearchHit h : r.getHits().getHits()) {
            Explanation e = h.getExplanation();
            assertNotNull("every hit has an explanation", e);
            assertEquals("explanation value equals score", h.getScore(), e.getValue().floatValue(), 1e-4f);
            assertTrue("BM25 weight description", e.toString().contains("weight(title:headphones"));
        }
    }

    /** EX-IT-2: rank_fusion explain shows per-leg RRF contributions; absent legs are marked. */
    public void testRankFusionExplainShowsPerLegContributions() throws Exception {
        createProducts(3);
        SearchResponse r = explainSearch(RANK_FUSION_TWO_LEG, 10, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());

        // Both-legs doc a: root is rank_fusion, 2 leg details, root value == score, contributions sum to it.
        Explanation a = explanationOf(r, "a");
        assertNotNull(a);
        assertTrue(a.getDescription(), a.getDescription().contains("rank_fusion"));
        assertEquals(scoreOf(r, "a"), a.getValue().floatValue(), 1e-4f);
        assertEquals("two leg details", 2, a.getDetails().length);
        double sum = 0;
        int present = 0;
        for (Explanation leg : a.getDetails()) {
            if (leg.isMatch()) {
                sum += leg.getValue().doubleValue();
                present++;
            }
        }
        assertEquals("a is in both legs", 2, present);
        assertEquals("present-leg contributions sum to the root", a.getValue().doubleValue(), sum, 1e-4);

        // Single-leg doc d (title only): exactly one present leg, the other marked not present.
        Explanation d = explanationOf(r, "d");
        int presentD = 0;
        boolean sawNotPresent = false;
        for (Explanation leg : d.getDetails()) {
            if (leg.isMatch()) {
                presentD++;
            } else if (leg.getDescription().contains("not present")) {
                sawNotPresent = true;
            }
        }
        assertEquals("d is in exactly one leg", 1, presentD);
        assertTrue("the other leg is marked not present", sawNotPresent);
    }

    /** EX-IT-3: score_fusion explain shows min_max normalization detail and weight per leg. */
    public void testScoreFusionExplainShowsNormalizationDetail() throws Exception {
        createProducts(3);
        SearchResponse r = explainSearch(SCORE_FUSION_TWO_LEG, 10, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());

        Explanation a = explanationOf(r, "a");
        assertNotNull(a);
        assertTrue(a.getDescription(), a.getDescription().contains("score_fusion"));
        assertTrue(a.getDescription(), a.getDescription().contains("min_max"));
        assertTrue(a.getDescription(), a.getDescription().contains("arithmetic_mean"));
        assertEquals(scoreOf(r, "a"), a.getValue().floatValue(), 1e-4f);
        assertEquals(2, a.getDetails().length);
        boolean sawNormAndWeight = false;
        for (Explanation leg : a.getDetails()) {
            if (leg.isMatch() && leg.getDescription().contains("norm=") && leg.getDescription().contains("weight=")) {
                sawNormAndWeight = true;
            }
        }
        assertTrue("a present leg shows norm=... and weight=...", sawNormAndWeight);
    }

    /** EX-IT-4: nested fusion explains bottom-up (depth >= 3). */
    public void testNestedFusionExplainRecurses() throws Exception {
        createProducts(3);
        String nested = "{\"rank_fusion\":{\"retrievers\":["
            + "{\"rank_fusion\":{\"retrievers\":["
            + "  {\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "  {\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}},"
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"earbuds\"}}}}"
            + "]}}";
        SearchResponse r = explainSearch(nested, 10, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());

        Explanation a = explanationOf(r, "a"); // reachable through the inner fusion
        assertNotNull(a);
        assertTrue(a.getDescription().contains("rank_fusion"));
        // Find the outer leg whose nested node is itself a rank_fusion (the inner fusion).
        boolean sawNestedFusion = false;
        for (Explanation leg : a.getDetails()) {
            for (Explanation nestedChild : leg.getDetails()) {
                if (nestedChild.getDescription().contains("rank_fusion")) {
                    sawNestedFusion = true;
                }
            }
        }
        assertTrue("explanation nests an inner rank_fusion node", sawNestedFusion);
    }

    /** EX-IT-5: explain is stable across a multi-node, multi-shard cluster. */
    public void testExplainStableMultiNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        createProducts(3);
        SearchResponse r = explainSearch(RANK_FUSION_TWO_LEG, 10, SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        assertTrue(r.getHits().getHits().length > 0);
        for (SearchHit h : r.getHits().getHits()) {
            Explanation e = h.getExplanation();
            assertNotNull("every hit explained", e);
            assertEquals(h.getScore(), e.getValue().floatValue(), 1e-4f);
        }
    }

    /** EX-IT-6: explain:true is honored (regression gate) — every hit carries an explanation. */
    public void testExplainNoLongerIgnored() throws Exception {
        createProducts(3);
        SearchResponse r = explainSearch(RANK_FUSION_TWO_LEG, 10, SearchType.QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        assertTrue("has hits", r.getHits().getHits().length > 0);
        for (SearchHit h : r.getHits().getHits()) {
            assertNotNull("explain:true produces an _explanation on every hit", h.getExplanation());
        }
    }
}
