/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for {@link RankDocsQueryBuilder} / {@link RankDocsSortBuilder} on a real,
 * multi-shard (and, for the transport-serialization case, multi-node) cluster. These prove the
 * coordinator↔data-node round trip: each shard {@code _id}-seeks its own docs, the coordinator
 * reassembles the ranking, and the internal {@code rank_docs} / {@code rank_docs_sort} builders survive
 * the wire.
 * <p>
 * The internal builders cannot be authored in a {@code _search} body ({@code fromXContent} rejects), so
 * these tests set them <b>programmatically</b> on the {@link SearchSourceBuilder} — exactly as the
 * retriever framework does — then issue the search via the transport client, exercising the real
 * broadcast + per-shard scoping + wire path.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RankDocsQueryIT extends OpenSearchIntegTestCase {

    /** Compute the shard a given {@code _id} routes to in {@code index}, matching how the doc was indexed. */
    private int shardOf(String index, String id) {
        ClusterState state = client().admin().cluster().prepareState().all().get().getState();
        return OperationRouting.generateShardId(state.metadata().index(index), id, null);
    }

    /** Build a {@link RankDoc} scoped to the shard the id actually routes to. */
    private RankDoc rankDoc(String index, String id, float score, int position) {
        return new RankDoc(index, shardOf(index, id), id, score, position);
    }

    private void createIndexWithShards(String index, int shards, int replicas) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shards).put(SETTING_NUMBER_OF_REPLICAS, replicas)
            )
        );
    }

    private void indexDocs(String index, String... ids) {
        for (String id : ids) {
            client().prepareIndex(index).setId(id).setSource("title", "doc " + id).get();
        }
        client().admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    /** Issue a search with the given source, log the request/response, and return the response. */
    private SearchResponse searchAndLog(String testName, String[] indices, SearchSourceBuilder source) {
        logger.info("[{}] REQUEST indices={} source={}", testName, Arrays.toString(indices), source.toString());
        SearchResponse response = client().prepareSearch(indices).setSource(source).get();
        StringBuilder hits = new StringBuilder();
        for (SearchHit h : response.getHits().getHits()) {
            hits.append("\n  index=").append(h.getIndex()).append(" id=").append(h.getId()).append(" score=").append(h.getScore());
        }
        logger.info("[{}] RESPONSE totalHits={} hits:{}", testName, response.getHits().getTotalHits().value(), hits.toString());
        return response;
    }

    private static List<String> idsInOrder(SearchResponse response) {
        List<String> ids = new ArrayList<>();
        for (SearchHit h : response.getHits().getHits()) {
            ids.add(h.getId());
        }
        return ids;
    }

    // IT-1: score replay across shards ------------------------------------------------------------

    public void testScoreReplayAcrossShards() {
        final String index = "products";
        createIndexWithShards(index, 3, 0);
        indexDocs(index, "a", "b", "c", "d", "e", "f");

        List<RankDoc> window = List.of(rankDoc(index, "a", 0.95f, 0), rankDoc(index, "c", 0.70f, 1), rankDoc(index, "e", 0.30f, 2));
        SearchSourceBuilder source = new SearchSourceBuilder().query(new RankDocsQueryBuilder(window)).size(10);
        SearchResponse response = searchAndLog("IT-1 scoreReplayAcrossShards", new String[] { index }, source);

        assertEquals(3, response.getHits().getTotalHits().value());
        for (SearchHit h : response.getHits().getHits()) {
            float expected = switch (h.getId()) {
                case "a" -> 0.95f;
                case "c" -> 0.70f;
                case "e" -> 0.30f;
                default -> throw new AssertionError("unexpected hit " + h.getId());
            };
            assertEquals("score for " + h.getId(), expected, h.getScore(), 1e-6f);
        }
    }

    // IT-2: multi-node fan-out WITH rank_docs_sort (the #22842 serialization repro) ---------------

    public void testMultiNodeFanoutWithSortSerializes() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String index = "products";
        // 3 shards, 0 replicas -> primaries spread across the 2 nodes, so at least one target shard is on
        // a non-coordinating node: this is the exact condition that made a SortField subclass throw
        // "Cannot serialize SortField impl" on the reduce path.
        createIndexWithShards(index, 3, 0);
        indexDocs(index, "a", "b", "c", "d", "e", "f");
        ensureGreen(index);

        List<RankDoc> window = List.of(rankDoc(index, "a", 0.9f, 0), rankDoc(index, "c", 0.8f, 1), rankDoc(index, "e", 0.7f, 2));
        // Set BOTH the internal query and the internal sort, so the rank_docs_sort SortField crosses the wire.
        SearchSourceBuilder source = new SearchSourceBuilder().query(new RankDocsQueryBuilder(window))
            .sort(new RankDocsSortBuilder(window))
            .size(10);
        SearchResponse response = searchAndLog("IT-2 multiNodeFanoutWithSort", new String[] { index }, source);

        assertEquals(3, response.getHits().getTotalHits().value());
        // ordered by position 0,1,2 -> a,c,e
        assertEquals(List.of("a", "c", "e"), idsInOrder(response));
    }

    // IT-3: cross-index _id collision -------------------------------------------------------------

    public void testCrossIndexIdCollision() {
        createIndexWithShards("products", 3, 0);
        createIndexWithShards("reviews", 3, 0);
        indexDocs("products", "a", "x", "y");
        indexDocs("reviews", "a", "p", "q"); // shared id "a" across indices

        // Window pins products/a (score 0.9) and reviews/a (score 0.1): same _id, different index.
        List<RankDoc> window = List.of(rankDoc("products", "a", 0.9f, 0), rankDoc("reviews", "a", 0.1f, 1));
        SearchSourceBuilder source = new SearchSourceBuilder().query(new RankDocsQueryBuilder(window)).size(10);
        SearchResponse response = searchAndLog("IT-3 crossIndexIdCollision", new String[] { "products", "reviews" }, source);

        assertEquals(2, response.getHits().getTotalHits().value());
        for (SearchHit h : response.getHits().getHits()) {
            if ("products".equals(h.getIndex())) {
                assertEquals("products/a must keep its own score", 0.9f, h.getScore(), 1e-6f);
            } else {
                assertEquals("reviews", h.getIndex());
                assertEquals("reviews/a must keep its own score", 0.1f, h.getScore(), 1e-6f);
            }
        }
    }

    // IT-4: position sort decoupled from score ----------------------------------------------------

    public void testPositionSortDecoupledFromScore() {
        final String index = "products";
        createIndexWithShards(index, 3, 0);
        indexDocs(index, "a", "b", "c", "d", "e", "f");

        // position order (c,b,a) is the inverse of score order (a>b>c).
        List<RankDoc> window = List.of(rankDoc(index, "a", 0.90f, 2), rankDoc(index, "b", 0.80f, 1), rankDoc(index, "c", 0.10f, 0));
        SearchSourceBuilder source = new SearchSourceBuilder().query(new RankDocsQueryBuilder(window))
            .sort(new RankDocsSortBuilder(window))
            .size(10);
        SearchResponse response = searchAndLog("IT-4 positionSortDecoupledFromScore", new String[] { index }, source);

        assertEquals(3, response.getHits().getTotalHits().value());
        assertEquals("ordered by position, not score", List.of("c", "b", "a"), idsInOrder(response));
    }

    // IT-5: unpinned docs sort last ---------------------------------------------------------------

    public void testUnpinnedDocsSortLast() {
        final String index = "products";
        createIndexWithShards(index, 3, 0);
        indexDocs(index, "a", "b", "c", "d");

        // Only b and d are in the window; a and c are unpinned. Query is match_all so all 4 docs return;
        // the rank_docs_sort orders b,d first (by position) and unpinned a,c last.
        List<RankDoc> window = List.of(rankDoc(index, "d", 0.5f, 0), rankDoc(index, "b", 0.4f, 1));
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .sort(new RankDocsSortBuilder(window))
            .size(10);
        SearchResponse response = searchAndLog("IT-5 unpinnedDocsSortLast", new String[] { index }, source);

        assertEquals(4, response.getHits().getTotalHits().value());
        List<String> ids = idsInOrder(response);
        assertEquals("d", ids.get(0));
        assertEquals("b", ids.get(1));
        assertTrue("unpinned docs must sort last", ids.subList(2, 4).containsAll(List.of("a", "c")));
    }

    // IT-6: missing/deleted doc drops out ---------------------------------------------------------

    public void testMissingDocDropsOut() {
        final String index = "products";
        createIndexWithShards(index, 3, 0);
        indexDocs(index, "a", "b", "c");
        // Delete "b" before search; its window entry must resolve to nothing.
        client().prepareDelete(index, "b").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        List<RankDoc> window = List.of(rankDoc(index, "a", 0.9f, 0), rankDoc(index, "b", 0.8f, 1), rankDoc(index, "c", 0.7f, 2));
        SearchSourceBuilder source = new SearchSourceBuilder().query(new RankDocsQueryBuilder(window)).size(10);
        SearchResponse response = searchAndLog("IT-6 missingDocDropsOut", new String[] { index }, source);

        assertEquals("deleted b drops out; a and c remain", 2, response.getHits().getTotalHits().value());
        assertFalse(idsInOrder(response).contains("b"));
    }

    // IT-7: boost applied end-to-end --------------------------------------------------------------

    public void testBoostApplied() {
        final String index = "products";
        createIndexWithShards(index, 3, 0);
        indexDocs(index, "a", "b", "c");

        List<RankDoc> window = List.of(rankDoc(index, "a", 0.5f, 0));
        RankDocsQueryBuilder qb = new RankDocsQueryBuilder(window);
        qb.boost(2.0f);
        SearchSourceBuilder source = new SearchSourceBuilder().query(qb).size(10);
        SearchResponse response = searchAndLog("IT-7 boostApplied", new String[] { index }, source);

        assertEquals(1, response.getHits().getTotalHits().value());
        assertEquals("score * boost", 1.0f, response.getHits().getHits()[0].getScore(), 1e-6f);
    }

    // IT-8: negative score rejected at construction (before any search) ---------------------------

    public void testNegativeScoreRejectedBeforeSearch() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RankDoc("products", 0, "a", -0.5f, 0));
        assertTrue(e.getMessage(), e.getMessage().contains("non-negative"));
    }
}
