/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for explain and profile support in the retriever framework.
 */
public class RetrieverExplainProfileTests extends OpenSearchTestCase {

    private static final ShardId SHARD_0 = new ShardId(new Index("products", "_na_"), 0);

    // === RankedDoc with Explanation ===

    public void testRankedDocWithExplanation() {
        Explanation explain = Explanation.match(8.5f, "BM25(title:headphones)");
        RankedDoc doc = new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, explain);

        assertEquals("doc1", doc.id());
        assertEquals(8.5f, doc.score(), 0.001f);
        assertNotNull(doc.explanation());
        assertEquals(8.5f, doc.explanation().getValue().floatValue(), 0.001f);
        assertTrue(doc.explanation().getDescription().contains("BM25"));
    }

    public void testRankedDocWithoutExplanation() {
        RankedDoc doc = new RankedDoc("doc1", "products", SHARD_0, 5.0f, 0);
        assertNull(doc.explanation());
    }

    public void testRankedDocWithScoreAndPositionPreservesExplanation() {
        Explanation explain = Explanation.match(8.5f, "BM25(title:headphones)");
        RankedDoc doc = new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, explain);
        RankedDoc modified = doc.withScoreAndPosition(0.032f, 2);

        assertEquals(0.032f, modified.score(), 0.001f);
        assertEquals(2, modified.position());
        assertNotNull(modified.explanation());
        assertEquals(8.5f, modified.explanation().getValue().floatValue(), 0.001f);
    }

    public void testRankedDocWithScoreAndPositionNewExplanation() {
        Explanation original = Explanation.match(8.5f, "BM25");
        RankedDoc doc = new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, original);

        Explanation fusedExplanation = Explanation.match(0.032f, "rank_fusion");
        RankedDoc modified = doc.withScoreAndPosition(0.032f, 1, fusedExplanation);

        assertEquals(0.032f, modified.score(), 0.001f);
        assertEquals(1, modified.position());
        assertEquals(0.032f, modified.explanation().getValue().floatValue(), 0.001f);
        assertTrue(modified.explanation().getDescription().contains("rank_fusion"));
    }

    // === RetrieverProfile ===

    public void testRetrieverProfileBuilder() {
        RetrieverProfile profile = new RetrieverProfile.Builder("rank_fusion")
            .totalTimeNanos(52000000L)
            .addBreakdown("dispatch_time_in_nanos", 51000000L)
            .addBreakdown("fusion_time_in_nanos", 50000L)
            .build();

        assertEquals("rank_fusion", profile.getType());
        assertEquals(52000000L, profile.getTotalTimeNanos());
        assertEquals(2, profile.getBreakdown().size());
        assertEquals(51000000L, (long) profile.getBreakdown().get("dispatch_time_in_nanos"));
        assertEquals(50000L, (long) profile.getBreakdown().get("fusion_time_in_nanos"));
        assertNull(profile.getLegs());
        assertNull(profile.getChild());
    }

    public void testRetrieverProfileWithLegs() {
        RetrieverProfile leg0 = new RetrieverProfile.Builder("standard")
            .totalTimeNanos(45000000L)
            .addBreakdown("dispatch_time_in_nanos", 45000000L)
            .build();
        RetrieverProfile leg1 = new RetrieverProfile.Builder("standard")
            .totalTimeNanos(48000000L)
            .addBreakdown("dispatch_time_in_nanos", 48000000L)
            .build();

        RetrieverProfile compound = new RetrieverProfile.Builder("rank_fusion")
            .totalTimeNanos(50000000L)
            .addBreakdown("dispatch_time_in_nanos", 49000000L)
            .addBreakdown("fusion_time_in_nanos", 40000L)
            .legs(List.of(leg0, leg1))
            .build();

        assertNotNull(compound.getLegs());
        assertEquals(2, compound.getLegs().size());
        assertEquals("standard", compound.getLegs().get(0).getType());
        assertEquals(45000000L, compound.getLegs().get(0).getTotalTimeNanos());
    }

    public void testRetrieverProfileWithChild() {
        RetrieverProfile child = new RetrieverProfile.Builder("rank_fusion")
            .totalTimeNanos(50000000L)
            .build();

        RetrieverProfile transformer = new RetrieverProfile.Builder("pinned")
            .totalTimeNanos(50001000L)
            .addBreakdown("reshape_time_in_nanos", 1000L)
            .child(child)
            .build();

        assertNotNull(transformer.getChild());
        assertEquals("rank_fusion", transformer.getChild().getType());
    }

    // === StandardRetrieverBuilder explain ===

    public void testStandardRetrieverBuildExplanation() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        Explanation explain = Explanation.match(8.5f, "BM25(title:headphones)");
        List<RankedDoc> results = List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, explain),
            new RankedDoc("doc2", "products", SHARD_0, 6.2f, 1, Explanation.match(6.2f, "BM25(title:wireless)"))
        );
        leaf.setSearchResult(results);
        leaf.resolveBottomUp();

        Explanation result = leaf.buildExplanation("doc1", "products");
        assertNotNull(result);
        assertEquals(8.5f, result.getValue().floatValue(), 0.001f);
        assertTrue(result.getDescription().contains("BM25"));
    }

    public void testStandardRetrieverBuildExplanationMissingDoc() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        leaf.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, Explanation.match(8.5f, "BM25"))
        ));
        leaf.resolveBottomUp();

        Explanation result = leaf.buildExplanation("doc99", "products");
        assertNull(result);
    }

    // === RankFusionRetrieverBuilder explain ===

    public void testRankFusionBuildExplanation() {
        // Set up two legs with results
        StandardRetrieverBuilder leg0 = new StandardRetrieverBuilder();
        leg0.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, Explanation.match(8.5f, "BM25")),
            new RankedDoc("doc2", "products", SHARD_0, 6.2f, 1, Explanation.match(6.2f, "BM25 lower"))
        ));
        leg0.resolveBottomUp();

        StandardRetrieverBuilder leg1 = new StandardRetrieverBuilder();
        leg1.setSearchResult(List.of(
            new RankedDoc("doc2", "products", SHARD_0, 0.92f, 0, Explanation.match(0.92f, "kNN")),
            new RankedDoc("doc1", "products", SHARD_0, 0.85f, 1, Explanation.match(0.85f, "kNN lower"))
        ));
        leg1.resolveBottomUp();

        RankFusionRetrieverBuilder rrf = new RankFusionRetrieverBuilder(List.of(leg0, leg1), 60);
        rrf.resolveBottomUp();

        // doc1 is rank 1 in leg0, rank 2 in leg1
        Explanation explain = rrf.buildExplanation("doc1", "products");
        assertNotNull(explain);
        float expected = 1.0f / 61 + 1.0f / 62; // 0.01639 + 0.01613 = 0.03252
        assertEquals(expected, explain.getValue().floatValue(), 0.0001f);
        assertTrue(explain.getDescription().contains("rank_fusion"));
        assertEquals(2, explain.getDetails().length); // two legs
        assertTrue(explain.getDetails()[0].getDescription().contains("leg 0, rank 1"));
        assertTrue(explain.getDetails()[1].getDescription().contains("leg 1, rank 2"));
    }

    public void testRankFusionBuildExplanationDocInOneLegOnly() {
        StandardRetrieverBuilder leg0 = new StandardRetrieverBuilder();
        leg0.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, Explanation.match(8.5f, "BM25"))
        ));
        leg0.resolveBottomUp();

        StandardRetrieverBuilder leg1 = new StandardRetrieverBuilder();
        leg1.setSearchResult(List.of(
            new RankedDoc("doc2", "products", SHARD_0, 0.92f, 0, Explanation.match(0.92f, "kNN"))
        ));
        leg1.resolveBottomUp();

        RankFusionRetrieverBuilder rrf = new RankFusionRetrieverBuilder(List.of(leg0, leg1), 60);
        rrf.resolveBottomUp();

        // doc1 only in leg0
        Explanation explain = rrf.buildExplanation("doc1", "products");
        assertNotNull(explain);
        float expected = 1.0f / 61; // only in one leg
        assertEquals(expected, explain.getValue().floatValue(), 0.0001f);
        assertTrue(explain.getDetails()[0].isMatch()); // leg 0 matches
        assertFalse(explain.getDetails()[1].isMatch()); // leg 1 "not present"
        assertTrue(explain.getDetails()[1].getDescription().contains("not present"));
    }

    // === PinnedRetrieverBuilder explain ===

    public void testPinnedBuildExplanationPinnedDoc() {
        StandardRetrieverBuilder child = new StandardRetrieverBuilder();
        child.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, Explanation.match(8.5f, "BM25")),
            new RankedDoc("doc2", "products", SHARD_0, 6.2f, 1, Explanation.match(6.2f, "BM25")),
            new RankedDoc("doc3", "products", SHARD_0, 5.0f, 2, Explanation.match(5.0f, "BM25"))
        ));
        child.resolveBottomUp();

        PinnedRetrieverBuilder pinned = new PinnedRetrieverBuilder(List.of("doc2", "doc3"), child);
        pinned.resolveBottomUp();

        Explanation explain = pinned.buildExplanation("doc2", "products");
        assertNotNull(explain);
        assertTrue(explain.getDescription().contains("pinned at position 1"));
        assertTrue(explain.getDescription().contains("organic rank=2"));
    }

    public void testPinnedBuildExplanationOrganicDoc() {
        StandardRetrieverBuilder child = new StandardRetrieverBuilder();
        child.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, Explanation.match(8.5f, "BM25")),
            new RankedDoc("doc2", "products", SHARD_0, 6.2f, 1, Explanation.match(6.2f, "BM25"))
        ));
        child.resolveBottomUp();

        PinnedRetrieverBuilder pinned = new PinnedRetrieverBuilder(List.of("doc2"), child);
        pinned.resolveBottomUp();

        Explanation explain = pinned.buildExplanation("doc1", "products");
        assertNotNull(explain);
        assertTrue(explain.getDescription().contains("organic"));
    }

    // === StandardRetrieverBuilder profile ===

    public void testStandardRetrieverBuildProfile() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        leaf.startTiming();
        // simulate some work
        leaf.stopTiming();

        RetrieverProfile profile = leaf.buildProfile();
        assertEquals("standard", profile.getType());
        assertTrue(profile.getTotalTimeNanos() >= 0);
        // Leaf nodes have no breakdown — their total_time IS the dispatch time
        assertTrue(profile.getBreakdown().isEmpty());
    }

    // === CompoundRetrieverBuilder profile ===

    public void testRankFusionBuildProfile() {
        StandardRetrieverBuilder leg0 = new StandardRetrieverBuilder();
        leg0.startTiming();
        leg0.setSearchResult(List.of(new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0)));
        leg0.stopTiming();
        leg0.resolveBottomUp();

        StandardRetrieverBuilder leg1 = new StandardRetrieverBuilder();
        leg1.startTiming();
        leg1.setSearchResult(List.of(new RankedDoc("doc2", "products", SHARD_0, 0.92f, 0)));
        leg1.stopTiming();
        leg1.resolveBottomUp();

        RankFusionRetrieverBuilder rrf = new RankFusionRetrieverBuilder(List.of(leg0, leg1), 60);
        rrf.startTiming();
        rrf.setFusionTimeNanos(50000L);
        rrf.resolveBottomUp();
        rrf.stopTiming();

        RetrieverProfile profile = rrf.buildProfile();
        assertEquals("rank_fusion", profile.getType());
        assertTrue(profile.getTotalTimeNanos() >= 0);
        // Only fusion_time in breakdown — no dispatch_time (total_time - breakdown = waiting time)
        assertEquals(50000L, (long) profile.getBreakdown().get("fusion_time_in_nanos"));
        assertNotNull(profile.getLegs());
        assertEquals(2, profile.getLegs().size());
        assertEquals("standard", profile.getLegs().get(0).getType());
    }

    // === RetrieverSearchContext merge ===

    public void testRetrieverSearchContextMergeExplanations() {
        RetrieverSearchContext ctx = new RetrieverSearchContext(null);
        Map<String, Explanation> explanations = Map.of(
            RetrieverSearchContext.docKey("doc1", "products"),
            Explanation.match(0.032f, "rank_fusion [rank_constant=60]")
        );
        ctx.setDocExplanations(explanations);

        // Verify the key format
        assertEquals("doc1|products", RetrieverSearchContext.docKey("doc1", "products"));
        assertNotNull(ctx.getDocExplanations());
        assertEquals(1, ctx.getDocExplanations().size());
    }

    public void testRetrieverSearchContextStoresProfile() {
        RetrieverSearchContext ctx = new RetrieverSearchContext(null);
        RetrieverProfile profile = new RetrieverProfile.Builder("rank_fusion")
            .totalTimeNanos(50000000L)
            .build();
        ctx.setRetrieverProfile(profile);

        assertNotNull(ctx.getRetrieverProfile());
        assertEquals("rank_fusion", ctx.getRetrieverProfile().getType());
    }

    // === ScoreFusion explain ===

    public void testScoreFusionBuildExplanation() {
        StandardRetrieverBuilder leg0 = new StandardRetrieverBuilder();
        leg0.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 8.5f, 0, Explanation.match(8.5f, "BM25")),
            new RankedDoc("doc2", "products", SHARD_0, 2.1f, 1, Explanation.match(2.1f, "BM25 low"))
        ));
        leg0.resolveBottomUp();

        StandardRetrieverBuilder leg1 = new StandardRetrieverBuilder();
        leg1.setSearchResult(List.of(
            new RankedDoc("doc1", "products", SHARD_0, 0.92f, 0, Explanation.match(0.92f, "kNN")),
            new RankedDoc("doc3", "products", SHARD_0, 0.3f, 1, Explanation.match(0.3f, "kNN low"))
        ));
        leg1.resolveBottomUp();

        ScoreFusionRetrieverBuilder sf = new ScoreFusionRetrieverBuilder(List.of(leg0, leg1));
        sf.setNormalization("min_max");
        sf.setCombination("arithmetic_mean");
        sf.setWeights(new float[]{0.4f, 0.6f});
        sf.resolveBottomUp();

        Explanation explain = sf.buildExplanation("doc1", "products");
        assertNotNull(explain);
        assertTrue(explain.getDescription().contains("score_fusion"));
        assertTrue(explain.getDescription().contains("min_max"));
        assertTrue(explain.getDescription().contains("arithmetic_mean"));
        assertEquals(2, explain.getDetails().length);
        // doc1 is max in both legs, so normalized to 1.0 in both
        // fused = (0.4 * 1.0 + 0.6 * 1.0) / (0.4 + 0.6) = 1.0
        assertTrue(explain.getDetails()[0].getDescription().contains("leg 0"));
        assertTrue(explain.getDetails()[1].getDescription().contains("leg 1"));
    }
}
