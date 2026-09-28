/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link RetrieverProfile}: the data-class structure and XContent shape, and each retriever
 * type's {@link RetrieverBuilder#buildProfile()} over hand-resolved candidates. Exact per-shard query
 * profiles are produced by the search layer and are covered end-to-end in the integration test; here the
 * focus is the retriever profile tree structure (types, nesting, breakdown) and the serialized shape.
 */
public class RetrieverProfileTests extends OpenSearchTestCase {

    private static final String INDEX = "products";
    private static final ShardId SHARD = new ShardId(new Index(INDEX, "uuid"), 0);

    private static RetrieverCandidate cand(String id, float score, int position) {
        return new RetrieverCandidate(INDEX, SHARD, id, score, position);
    }

    private static StandardRetrieverBuilder resolvedLeaf(List<RetrieverCandidate> candidates) {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSearchResult(candidates);
        leaf.doResolve();
        return leaf;
    }

    private static String render(ToXContent x) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        // RetrieverProfile emits its own "profile":{...} field, so it must be inside an object.
        builder.startObject();
        x.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    // ---- data class structure / XContent ----

    public void testLeafNodeShape() {
        RetrieverProfile.Node leaf = RetrieverProfile.leaf("standard", 1234L, Map.of());
        assertEquals("standard", leaf.getType());
        assertEquals(1234L, leaf.getTotalTimeInNanos());
        assertTrue("leaf has no breakdown", leaf.getBreakdown().isEmpty());
        assertTrue("leaf has no children", leaf.getChildren().isEmpty());
    }

    public void testInnerNodeShape() {
        RetrieverProfile.Node child = RetrieverProfile.leaf("standard", 10L, Map.of());
        RetrieverProfile.Node inner = RetrieverProfile.inner("rank_fusion", 100L, RetrieverProfile.breakdown("fuse", 7L), List.of(child));
        assertEquals("rank_fusion", inner.getType());
        assertEquals(100L, inner.getTotalTimeInNanos());
        assertEquals(Long.valueOf(7L), inner.getBreakdown().get("fuse"));
        assertEquals(1, inner.getChildren().size());
        assertEquals("standard", inner.getChildren().get(0).getType());
    }

    public void testTopLevelProfileXContentShape() throws Exception {
        RetrieverProfile.Node tree = RetrieverProfile.inner(
            "rank_fusion",
            100L,
            RetrieverProfile.breakdown("fuse", 7L),
            List.of(RetrieverProfile.leaf("standard", 10L, Map.of()), RetrieverProfile.leaf("standard", 20L, Map.of()))
        );
        RetrieverProfile profile = RetrieverProfile.builder().retriever(tree).selfResolveTimeInNanos(150L).build();
        String json = render(profile);
        // Emits under a "profile" object: a "self_resolve" phase wrapping the "retriever" tree, and a
        // top-level total_time_in_nanos = self_resolve + rank_docs_query (0 here).
        assertTrue(json, json.contains("\"profile\":{"));
        assertTrue(json, json.contains("\"self_resolve\":{"));
        assertTrue(json, json.contains("\"retriever\":{"));
        assertTrue(json, json.contains("\"type\":\"rank_fusion\""));
        assertTrue(json, json.contains("\"breakdown\":{\"fuse\":7}"));
        assertTrue(json, json.contains("\"children\":["));
        // self_resolve wall (150) exceeds max(tree=100, global_leg=0)=100 -> coordinator_overhead 50.
        assertTrue(json, json.contains("\"coordinator_overhead\":50"));
        // Total = self_resolve(150) + rank_docs_query(0) = 150.
        assertTrue(json, json.contains("\"total_time_in_nanos\":150"));
    }

    public void testProfileOmitsAbsentSections() throws Exception {
        // No global_leg / rank_docs_query set -> those keys are absent; self_resolve + total still present.
        RetrieverProfile profile = RetrieverProfile.builder()
            .retriever(RetrieverProfile.leaf("standard", 5L, Map.of()))
            .selfResolveTimeInNanos(5L)
            .build();
        String json = render(profile);
        assertTrue(json, json.contains("\"self_resolve\":{"));
        assertFalse(json, json.contains("global_leg"));
        assertFalse(json, json.contains("rank_docs_query"));
        // self_resolve wall (5) == max(tree=5) -> no positive overhead -> breakdown omitted.
        assertFalse("no coordinator_overhead when wall == productive", json.contains("coordinator_overhead"));
        // Total = self_resolve(5) + rank_docs_query(0) = 5.
        assertTrue(json, json.contains("\"total_time_in_nanos\":5"));
    }

    // ---- per-node buildProfile ----

    public void testStandardLeafBuildProfile() {
        StandardRetrieverBuilder leaf = resolvedLeaf(List.of(cand("a", 2.0f, 0)));
        RetrieverProfile.Node node = leaf.buildProfile();
        assertEquals("standard", node.getType());
        assertTrue("no leg profiles captured without profiling -> empty shards, no children", node.getChildren().isEmpty());
        assertTrue(node.getBreakdown().isEmpty());
    }

    public void testRankFusionBuildProfileTree() {
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 3.0f, 0), cand("b", 2.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("b", 9.0f, 0)));
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(legA, legB));
        rf.doResolve();

        RetrieverProfile.Node node = rf.buildProfile();
        assertEquals("rank_fusion", node.getType());
        assertEquals("two child leaf profiles", 2, node.getChildren().size());
        assertTrue("compound reports a fuse breakdown", node.getBreakdown().containsKey("fuse"));
        assertEquals("standard", node.getChildren().get(0).getType());
        assertEquals("standard", node.getChildren().get(1).getType());
    }

    public void testScoreFusionBuildProfileTree() {
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 10.0f, 0), cand("b", 6.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("a", 4.0f, 0)));
        ScoreFusionRetrieverBuilder sf = new ScoreFusionRetrieverBuilder(List.of(legA, legB));
        sf.doResolve();

        RetrieverProfile.Node node = sf.buildProfile();
        assertEquals("score_fusion", node.getType());
        assertEquals(2, node.getChildren().size());
        assertTrue(node.getBreakdown().containsKey("fuse"));
    }

    public void testCompoundBreakdownReconcilesToTotal() {
        // A compound node's breakdown must reconcile: fuse + orchestration_overhead + max(child total) == total.
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 3.0f, 0), cand("b", 2.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("b", 9.0f, 0)));
        legA.nodeElapsedNanos = 5_000_000L;
        legB.nodeElapsedNanos = 4_000_000L;
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(legA, legB));
        rf.doResolve(); // sets fuseElapsedNanos (small, real)
        rf.nodeElapsedNanos = 9_000_000L; // wall > fuse + max(child)=5_000_000 -> positive orchestration overhead

        RetrieverProfile.Node node = rf.buildProfile();
        assertTrue("fuse present", node.getBreakdown().containsKey("fuse"));
        assertTrue(
            "orchestration_overhead present when wall exceeds fuse + max(child)",
            node.getBreakdown().containsKey("orchestration_overhead")
        );
        long fuse = node.getBreakdown().get("fuse");
        long orchestration = node.getBreakdown().get("orchestration_overhead");
        long maxChild = Math.max(node.getChildren().get(0).getTotalTimeInNanos(), node.getChildren().get(1).getTotalTimeInNanos());
        assertEquals(
            "fuse + orchestration_overhead + max(child) == node total",
            node.getTotalTimeInNanos(),
            fuse + orchestration + maxChild
        );
    }

    public void testCompoundOmitsOrchestrationOverheadWhenNotPositive() {
        // doResolve()-only path (no async resolve): nodeElapsedNanos stays 0, so orchestration overhead
        // would be negative and must be omitted; fuse is still present.
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 3.0f, 0)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("b", 9.0f, 0)));
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(legA, legB));
        rf.doResolve();
        RetrieverProfile.Node node = rf.buildProfile();
        assertTrue("fuse always present", node.getBreakdown().containsKey("fuse"));
        assertFalse("no orchestration_overhead when not positive", node.getBreakdown().containsKey("orchestration_overhead"));
    }

    public void testNestedBuildProfileRecurses() {
        StandardRetrieverBuilder a = resolvedLeaf(List.of(cand("a", 3.0f, 0)));
        StandardRetrieverBuilder b = resolvedLeaf(List.of(cand("b", 9.0f, 0)));
        RankFusionRetrieverBuilder inner = new RankFusionRetrieverBuilder(List.of(a, b));
        inner.doResolve();
        StandardRetrieverBuilder c = resolvedLeaf(List.of(cand("c", 5.0f, 0)));
        RankFusionRetrieverBuilder outer = new RankFusionRetrieverBuilder(List.of(inner, c));
        outer.doResolve();

        RetrieverProfile.Node node = outer.buildProfile();
        assertEquals("rank_fusion", node.getType());
        assertEquals(2, node.getChildren().size());
        // First child is the inner rank_fusion (depth >= 2 in the profile tree).
        RetrieverProfile.Node innerNode = node.getChildren().get(0);
        assertEquals("rank_fusion", innerNode.getType());
        assertEquals(2, innerNode.getChildren().size());
    }
}
