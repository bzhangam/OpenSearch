/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for the retriever engine on a real, multi-shard cluster.
 * <p>
 * A3a proved the request is accepted / validated. A3b proves it <b>executes</b>: a top-level
 * {@code standard} retriever resolves (per-node listener-based resolution: each leaf dispatches its own
 * {@code client.search}, results become {@code RetrieverCandidate}s, the tree resolves into a
 * {@code RankDocsQuery}) and returns results identical to the equivalent plain {@code _search} — the top
 * correctness gate — across shards and nodes.
 * <p>
 * <b>Parity is exact, per-id, on multiple shards.</b> The leg sub-search inherits the original request's
 * {@link SearchType} (the fix for the leg-scoring bug: without it a DFS request scored its legs with
 * per-shard {@code QUERY_THEN_FETCH} IDF), so for a given search type the retriever reproduces the plain
 * query's per-doc scores exactly — proven here for both {@code QUERY_THEN_FETCH} and
 * {@code DFS_QUERY_THEN_FETCH}. Blocked-combo validation is parse-time and lives in
 * {@link SearchSourceBuilderRetrieverIntegrationTests} (unit).
 * <p>
 * Shared corpus + helpers live in {@link AbstractRetrieverIT}; PIT lifecycle tests live in their own
 * {@code RetrieverPitIT}.
 */
public class RetrieverEngineIT extends AbstractRetrieverIT {

    public void testTopLevelStandardRetrieverAcceptedAndExecutes() throws Exception {
        createProducts(3);
        SearchResponse r = retrieverSearch(
            "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}",
            10,
            SearchType.QUERY_THEN_FETCH
        );
        assertEquals(0, r.getFailedShards());
        // 4 docs contain "headphones" (a, b, d, f)
        assertEquals(4, r.getHits().getHits().length);
    }

    /**
     * The top correctness gate: on a multi-shard index, for a given {@link SearchType}, the retriever
     * reproduces the plain query's per-doc scores AND order exactly. Asserted for both search types,
     * because the leg now inherits the request's search type (the leg-scoring fix).
     */
    public void testParityWithPlainSearchExactPerId() throws Exception {
        createProducts(3);
        for (SearchType st : new SearchType[] { SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH }) {
            SearchResponse viaRetriever = retrieverSearch("{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}", 10, st);
            SearchResponse viaPlain = client().prepareSearch("products")
                .setSearchType(st)
                .setSource(new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "headphones")).size(10))
                .get();

            Map<String, Float> plainScores = scoreById(viaPlain);
            Map<String, Float> retrieverScores = scoreById(viaRetriever);
            assertEquals("[" + st + "] same doc set", plainScores.keySet(), retrieverScores.keySet());
            for (String id : plainScores.keySet()) {
                assertEquals("[" + st + "] score parity for id " + id, plainScores.get(id), retrieverScores.get(id), 0.0001f);
            }
            assertEquals("[" + st + "] order parity", ids(viaPlain), ids(viaRetriever));
        }
    }

    public void testParityMultiNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        createProducts(3);
        // Parity must hold when candidate docs live on different nodes (coordinator reassembly correct):
        // exact per-id scores under a fixed search type.
        SearchType st = SearchType.DFS_QUERY_THEN_FETCH;
        SearchResponse viaRetriever = retrieverSearch("{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}", 10, st);
        SearchResponse viaPlain = client().prepareSearch("products")
            .setSearchType(st)
            .setSource(new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "headphones")).size(10))
            .get();
        Map<String, Float> plainScores = scoreById(viaPlain);
        Map<String, Float> retrieverScores = scoreById(viaRetriever);
        assertEquals(plainScores.keySet(), retrieverScores.keySet());
        for (String id : plainScores.keySet()) {
            assertEquals("multi-node score parity for id " + id, plainScores.get(id), retrieverScores.get(id), 0.0001f);
        }
    }

    public void testPerLegFilterNarrowsCandidates() throws Exception {
        createProducts(3);
        // filter to brand=acme: of the headphones docs (a,b,d,f), only a,b are acme.
        SearchResponse r = retrieverSearch(
            "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}},\"filter\":{\"term\":{\"brand\":\"acme\"}}}}",
            10,
            SearchType.QUERY_THEN_FETCH
        );
        assertEquals(0, r.getFailedShards());
        List<String> got = ids(r);
        assertEquals(2, got.size());
        assertTrue(got.contains("a"));
        assertTrue(got.contains("b"));
    }

    public void testPerLegSizeBoundsCandidates() throws Exception {
        createProducts(3);
        // leg size 2 → at most 2 candidates survive; top-level size must fit the tree window (2).
        SearchResponse r = retrieverSearch(
            "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}},\"size\":2}}",
            2,
            SearchType.QUERY_THEN_FETCH
        );
        assertEquals(0, r.getFailedShards());
        assertEquals(2, r.getHits().getHits().length);
    }

    public void testTopLevelSizeBeyondCandidateWindowReturnsAvailableHits() throws Exception {
        createProducts(3);
        // Over-read: the leg produces only 2 candidates but the caller asks for size 10. Like a plain
        // `_search` over-read past the number of matching hits, this is NOT an error — it returns the
        // available slice (the 2 candidates the tree produced), never a 400.
        //
        // INPUT (over 6 docs a-f on 3 shards; 4 match "headphones" but leg size caps candidates at 2):
        //   { "retriever": { "standard": { "query": { "match": { "title": "headphones" } }, "size": 2 } },
        //     "size": 10 }
        // OUTPUT: 0 failed shards; hits.length == 2 (the 2 highest-scoring candidates in the window).
        SearchResponse r = retrieverSearch(
            "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}},\"size\":2}}",
            10,
            SearchType.QUERY_THEN_FETCH
        );
        assertEquals(0, r.getFailedShards());
        assertEquals(2, r.getHits().getHits().length);
    }

    public void testTopLevelFromBeyondCandidateWindowReturnsEmpty() throws Exception {
        createProducts(3);
        // Paging past the end of the candidate window: from 100 over a 2-candidate window. Like a plain
        // `_search` whose `from` exceeds the number of hits, this returns an EMPTY page (no error).
        //
        // INPUT: { "retriever": { "standard": { "query": { "match": { "title": "headphones" } }, "size": 2 } },
        //         "from": 100, "size": 10 }
        // OUTPUT: 0 failed shards; hits.length == 0 (empty page, no 400).
        String body = "{\"retriever\":{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}},\"size\":2}},"
            + "\"from\":100,\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch("products").setSearchType(SearchType.QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        assertEquals(0, r.getHits().getHits().length);
    }

    public void testTrackTotalHitsDisabledByDefaultForRetriever() throws Exception {
        createProducts(3);
        // A retriever request WITHOUT track_total_hits: the total is not tracked (disabled by default),
        // because a retriever result is a curated ranking, not a match set. Hits still return correctly.
        //
        // INPUT: {"retriever":{"standard":{"query":{"match":{"title":"headphones"}}}},"size":10}
        // OUTPUT: hits returned (a,b,d,f); total is NOT tracked (getTotalHits() == null).
        SearchResponse r = retrieverSearch("{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}", 10, SearchType.QUERY_THEN_FETCH);
        assertEquals(0, r.getFailedShards());
        assertEquals(4, r.getHits().getHits().length);
        // track_total_hits disabled → the total is not tracked at all; getTotalHits() is null.
        assertNull(r.getHits().getTotalHits());
    }

    public void testTrackTotalHitsExplicitTrueHonoredForRetriever() throws Exception {
        createProducts(3);
        // Same request WITH track_total_hits:true → the global leg fires and reports the accurate total.
        //
        // INPUT: {"retriever":{"standard":{"query":{"match":{"title":"headphones"}}}},"track_total_hits":true,"size":10}
        // OUTPUT: total relation EQUAL_TO, value 4 (the four headphones docs).
        String body = "{\"retriever\":{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "\"track_total_hits\":true,\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch("products").setSearchType(SearchType.QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        assertEquals(TotalHits.Relation.EQUAL_TO, r.getHits().getTotalHits().relation());
        assertEquals(4L, r.getHits().getTotalHits().value());
    }

    public void testBlockedComboStillRejectedThroughParse() throws Exception {
        createProducts(3);
        // Sanity that A3a validation still fires on the real parse path (parse-time; unit covers the matrix).
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilder.fromXContent(
                createParser(jsonXContent, "{\"retriever\":{\"standard\":{\"query\":{\"match_all\":{}}}},\"query\":{\"match_all\":{}}}")
            )
        );
        assertThat(e.getMessage(), containsString("cannot use [retriever] and [query] together"));
    }
}
