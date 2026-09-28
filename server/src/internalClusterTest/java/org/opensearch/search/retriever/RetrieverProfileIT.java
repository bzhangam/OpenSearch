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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Integration tests for retriever {@code profile} on a real multi-shard cluster. When {@code profile: true}
 * is set, the response {@code profile} section is the retriever profile tree: a {@code retriever} node tree
 * with per-node {@code total_time_in_nanos} and per-leg {@code shards} query profiles, an optional
 * {@code global_leg} section (when aggregations / {@code track_total_hits} ran), a {@code rank_docs_query}
 * section (the final fetch), and a top-level {@code total_time_in_nanos}. The exact per-shard profile
 * contents come from the search layer; these tests assert the retriever-specific tree structure and that
 * the leg queries actually show up (so the profile reflects the real work, not the RankDocsQuery replay).
 */
public class RetrieverProfileIT extends AbstractRetrieverIT {

    private String profileSearchJson(String retrieverBody, int size, SearchType searchType, boolean trackTotalHits) throws IOException {
        String body = "{\"retriever\":"
            + retrieverBody
            + ",\"profile\":true,\"size\":"
            + size
            + (trackTotalHits ? ",\"track_total_hits\":true" : "")
            + "}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch(INDEX).setSearchType(searchType).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        return renderToJson(r);
    }

    private static String renderToJson(SearchResponse r) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        r.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    private static final String RANK_FUSION_TWO_LEG = "{\"rank_fusion\":{\"retrievers\":["
        + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
        + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}}";

    /** profile:true returns a retriever tree with a rank_fusion node, per-leg shards, and rank_docs_query. */
    public void testRankFusionProfileTree() throws Exception {
        createProducts(3);
        String json = profileSearchJson(RANK_FUSION_TWO_LEG, 10, SearchType.DFS_QUERY_THEN_FETCH, false);
        assertTrue(json, json.contains("\"profile\":{"));
        // self_resolve phase wraps the retriever tree (and, when present, the global leg).
        assertTrue("self_resolve phase present", json.contains("\"self_resolve\":{"));
        assertTrue(json, json.contains("\"retriever\":{"));
        assertTrue(json, json.contains("\"retriever\":{"));
        assertTrue("root node type is rank_fusion", json.contains("\"type\":\"rank_fusion\""));
        assertTrue("child leaves are standard", json.contains("\"type\":\"standard\""));
        assertTrue("compound reports a fuse breakdown", json.contains("\"breakdown\":{\"fuse\":"));
        assertTrue("per-node total_time_in_nanos present", json.contains("\"total_time_in_nanos\":"));
        // The leg query profiles must show the real leg query, proving the profile reflects the leg work
        // and not just the RankDocsQuery replay. A leaf node embeds a "searches" array of per-shard query
        // profiles, each carrying the Lucene query "description".
        assertTrue("leaf embeds a searches array of shard query profiles", json.contains("\"searches\":["));
        assertTrue("shard profile carries a query description mentioning the leg term", json.contains("headphones"));
        assertTrue("children array present", json.contains("\"children\":["));
        // rank_docs_query section is the final fetch search's profile.
        assertTrue("rank_docs_query section present", json.contains("\"rank_docs_query\":{"));
    }

    /** With aggregations, a global_leg section appears in the profile. */
    public void testProfileIncludesGlobalLegWithAggs() throws Exception {
        createProducts(3);
        String body = "{\"retriever\":"
            + RANK_FUSION_TWO_LEG
            + ",\"profile\":true,\"size\":10,"
            + "\"aggs\":{\"brands\":{\"terms\":{\"field\":\"brand\"}}}}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        String json = renderToJson(r);
        assertTrue("global_leg section present when aggs ran", json.contains("\"global_leg\":{"));
        assertTrue(json, json.contains("\"retriever\":{"));
    }

    /** track_total_hits triggers the global leg, so global_leg appears even without aggs. */
    public void testProfileIncludesGlobalLegWithTrackTotalHits() throws Exception {
        createProducts(3);
        String json = profileSearchJson(RANK_FUSION_TWO_LEG, 10, SearchType.DFS_QUERY_THEN_FETCH, true);
        assertTrue("global_leg present with track_total_hits", json.contains("\"global_leg\":{"));
    }

    /** Nested fusion produces a nested retriever profile tree (children within children). */
    public void testNestedProfileTree() throws Exception {
        createProducts(3);
        String nested = "{\"rank_fusion\":{\"retrievers\":["
            + "{\"rank_fusion\":{\"retrievers\":["
            + "  {\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "  {\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}},"
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"earbuds\"}}}}"
            + "]}}";
        String json = profileSearchJson(nested, 10, SearchType.DFS_QUERY_THEN_FETCH, false);
        // Two rank_fusion nodes (outer + inner) must appear in the tree.
        int first = json.indexOf("\"type\":\"rank_fusion\"");
        int second = json.indexOf("\"type\":\"rank_fusion\"", first + 1);
        assertTrue("two rank_fusion nodes in the nested profile tree", first >= 0 && second > first);
    }

    /** score_fusion also produces a profile tree with a score_fusion node. */
    public void testScoreFusionProfileTree() throws Exception {
        createProducts(3);
        String scoreFusion = "{\"score_fusion\":{\"retrievers\":["
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}],"
            + "\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}}}";
        String json = profileSearchJson(scoreFusion, 10, SearchType.DFS_QUERY_THEN_FETCH, false);
        assertTrue("score_fusion node present", json.contains("\"type\":\"score_fusion\""));
        assertTrue(json.contains("\"total_time_in_nanos\":"));
    }
}
