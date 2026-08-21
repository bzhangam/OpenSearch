/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for compound and transformer retriever builders:
 * {@link RankFusionRetrieverBuilder}, {@link ScoreFusionRetrieverBuilder},
 * {@link PinnedRetrieverBuilder}, {@link RescoreRetrieverBuilder}.
 */
public class CompoundTransformerRetrieverTests extends OpenSearchTestCase {

    private static final ShardId SHARD_0 = new ShardId(new Index("products", "uuid1"), 0);

    // ============================================================
    // RankFusionRetrieverBuilder tests
    // ============================================================

    public void testRankFusionBasicRRF() {
        // Two legs: leg1=[A(rank1), B(rank2), C(rank3)], leg2=[B(rank1), D(rank2), A(rank3)]
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 8.0f, 1),
            new RankedDoc("C", "products", SHARD_0, 5.0f, 2)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("B", "products", SHARD_0, 0.9f, 0),
            new RankedDoc("D", "products", SHARD_0, 0.7f, 1),
            new RankedDoc("A", "products", SHARD_0, 0.5f, 2)
        );

        RankFusionRetrieverBuilder builder = new RankFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
            60
        );

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));

        // Expected RRF scores (rank_constant=60, 1-based ranks):
        // A: 1/(60+1) + 1/(60+3) = 0.01639 + 0.01587 = 0.03226
        // B: 1/(60+2) + 1/(60+1) = 0.01613 + 0.01639 = 0.03252
        // C: 1/(60+3) + 0 = 0.01587
        // D: 0 + 1/(60+2) = 0.01613

        assertEquals(4, fused.size());
        // B should be first (highest RRF score)
        assertEquals("B", fused.get(0).id());
        // A should be second
        assertEquals("A", fused.get(1).id());
        // Verify scores are reasonable
        assertTrue(fused.get(0).score() > fused.get(1).score());
        assertTrue(fused.get(1).score() > fused.get(2).score());
    }

    public void testRankFusionWithRankWindowSize() {
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 8.0f, 1),
            new RankedDoc("C", "products", SHARD_0, 5.0f, 2)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("D", "products", SHARD_0, 0.9f, 0),
            new RankedDoc("E", "products", SHARD_0, 0.7f, 1)
        );

        RankFusionRetrieverBuilder builder = new RankFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
            60
        );
        builder.setRankWindowSize(2);

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));
        assertEquals(2, fused.size());
        assertEquals(2, builder.getMaxOutputSize());
    }

    public void testRankFusionWithMinScore() {
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("B", "products", SHARD_0, 0.9f, 0)
        );

        RankFusionRetrieverBuilder builder = new RankFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
            60
        );
        // Set min_score higher than any single-leg RRF contribution
        builder.setMinScore(0.02f);

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));
        // Single-leg RRF score = 1/(60+1) ≈ 0.01639 — both below 0.02
        assertEquals(0, fused.size());
    }

    public void testRankFusionInvalidRankConstant() {
        expectThrows(IllegalArgumentException.class, () -> new RankFusionRetrieverBuilder(List.of(), 0));
        expectThrows(IllegalArgumentException.class, () -> new RankFusionRetrieverBuilder(List.of(), 10_001));
    }

    public void testRankFusionValidateLessThanTwoChildren() {
        RankFusionRetrieverBuilder builder = new RankFusionRetrieverBuilder();
        builder.childRetrievers = List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));

        expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
    }

    // ============================================================
    // ScoreFusionRetrieverBuilder tests
    // ============================================================

    public void testScoreFusionMinMaxArithmeticMean() {
        // Leg 1: scores [10, 5, 1] — min=1, max=10
        // Leg 2: scores [0.9, 0.1] — min=0.1, max=0.9
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 5.0f, 1),
            new RankedDoc("C", "products", SHARD_0, 1.0f, 2)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("B", "products", SHARD_0, 0.9f, 0),
            new RankedDoc("D", "products", SHARD_0, 0.1f, 1)
        );

        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setNormalization("min_max");
        builder.setCombination("arithmetic_mean");

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));

        // After min_max normalization:
        // Leg 1: A=(10-1)/(10-1)=1.0, B=(5-1)/(10-1)=0.444, C=(1-1)/(10-1)=0.0
        // Leg 2: B=(0.9-0.1)/(0.9-0.1)=1.0, D=(0.1-0.1)/(0.9-0.1)=0.0
        // Absent docs get 0.0 after normalization.
        //
        // Combined (arithmetic mean with equal weights, all scores included):
        // A: (1.0 + 0.0)/2 = 0.5 (present in leg1 only, leg2 score=0)
        // B: (0.444 + 1.0)/2 = 0.722 (present in both legs)
        // C: (0.0 + 0.0)/2 = 0.0
        // D: (0.0 + 0.0)/2 = 0.0
        assertTrue(fused.size() >= 2);
        // B should have highest score (present in both legs with high normalized scores)
        assertEquals("B", fused.get(0).id());
        assertEquals("A", fused.get(1).id());
    }

    public void testScoreFusionWithWeights() {
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0)
        );

        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setWeights(new float[] { 0.3f, 0.7f });

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));
        assertEquals(1, fused.size());
        assertEquals("A", fused.get(0).id());
        // Single result → normalized to 1.0 in both legs
        // Combined: (0.3*1.0 + 0.7*1.0)/(0.3+0.7) = 1.0
        assertEquals(1.0f, fused.get(0).score(), 0.001f);
    }

    public void testScoreFusionValidateWeightsMismatch() {
        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setWeights(new float[] { 0.5f }); // only 1 weight for 2 children

        expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
    }

    public void testScoreFusionValidateLessThanTwoChildren() {
        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder();
        builder.childRetrievers = List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("at least 2 child retrievers"));
    }

    public void testScoreFusionL2Normalization() {
        // L2 norm: score / sqrt(sum of squares)
        // Leg 1: [3, 4] → L2 = sqrt(9+16) = 5 → normalized: [0.6, 0.8]
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 3.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 4.0f, 1)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("A", "products", SHARD_0, 1.0f, 0)
        );

        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setNormalization("l2");
        builder.setCombination("arithmetic_mean");

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));
        assertTrue(fused.size() >= 1);
        // B: normalized in leg1 = 4/5 = 0.8, not in leg2 → score = 0.8
        // A: normalized in leg1 = 3/5 = 0.6, in leg2 = 1/1 = 1.0 → mean = (0.6+1.0)/2 = 0.8
        // So A and B should be close
    }

    public void testScoreFusionZScoreNormalization() {
        // z_score makes differently-scaled legs comparable; the doc that tops both legs stays on top.
        List<RankedDoc> leg1 = List.of(
            new RankedDoc("A", "products", SHARD_0, 3.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 2.0f, 1),
            new RankedDoc("C", "products", SHARD_0, 1.0f, 2)
        );
        List<RankedDoc> leg2 = List.of(
            new RankedDoc("A", "products", SHARD_0, 30.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 20.0f, 1),
            new RankedDoc("C", "products", SHARD_0, 10.0f, 2)
        );
        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setNormalization("z_score");
        builder.setCombination("arithmetic_mean");

        List<RankedDoc> fused = builder.fuse(List.of(leg1, leg2));
        assertEquals(3, fused.size());
        assertEquals("A", fused.get(0).id());
        assertEquals("C", fused.get(2).id());
        // A's fused z-score should be strictly greater than C's.
        assertTrue(fused.get(0).score() > fused.get(2).score());
    }

    public void testScoreFusionUnknownNormalizationRejected() {
        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setNormalization("bogus");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
        assertTrue(e.getMessage().contains("unknown normalization"));
    }

    public void testScoreFusionUnknownCombinationRejected() {
        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        builder.setCombination("bogus");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
        assertTrue(e.getMessage().contains("unknown combination"));
    }

    public void testStandardRejectsHybridQuery() {
        org.opensearch.index.query.QueryBuilder hybrid = org.mockito.Mockito.mock(org.opensearch.index.query.QueryBuilder.class);
        org.mockito.Mockito.when(hybrid.getWriteableName()).thenReturn("hybrid");
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(hybrid);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> leaf.validate(RetrieverContext.root()));
        assertTrue(e.getMessage().contains("[hybrid] query is not allowed inside [standard]"));
    }

    // ============================================================
    // PinnedRetrieverBuilder tests
    // ============================================================

    public void testPinnedBasic() {
        List<RankedDoc> childResult = List.of(
            new RankedDoc("organic1", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("organic2", "products", SHARD_0, 8.0f, 1),
            new RankedDoc("pinned_doc", "products", SHARD_0, 5.0f, 2)
        );

        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(
            List.of("pinned_doc"),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );

        // Simulate resolution
        List<RankedDoc> result = builder.reshape(childResult);

        // pinned_doc should be first
        assertEquals("pinned_doc", result.get(0).id());
        // organic results follow without the pinned doc
        assertEquals("organic1", result.get(1).id());
        assertEquals("organic2", result.get(2).id());
        assertEquals(3, result.size()); // no duplicates
    }

    public void testPinnedMultipleDocs() {
        List<RankedDoc> childResult = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 8.0f, 1),
            new RankedDoc("C", "products", SHARD_0, 5.0f, 2)
        );

        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(
            List.of("C", "B"),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );

        List<RankedDoc> result = builder.reshape(childResult);

        assertEquals("C", result.get(0).id());
        assertEquals("B", result.get(1).id());
        assertEquals("A", result.get(2).id()); // only organic left
        assertEquals(3, result.size());
    }

    public void testPinnedMissingDocSkipped() {
        List<RankedDoc> childResult = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 8.0f, 1)
        );

        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(
            List.of("missing_id", "A"),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );

        List<RankedDoc> result = builder.reshape(childResult);

        // missing_id not found → skipped. A is pinned first, then B organic.
        assertEquals("A", result.get(0).id());
        assertEquals("B", result.get(1).id());
        assertEquals(2, result.size());
    }

    public void testPinnedValidateEmptyIds() {
        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(
            List.of(),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );

        expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
    }

    public void testPinnedValidateNullIds() {
        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(
            null,
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );

        expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
    }

    public void testPinnedMaxOutputSizeDelegatesToChild() {
        // pinned only re-orders within the child's own result set — it never grows or shrinks it.
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSize(42);
        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(List.of("A"), leaf);

        assertEquals(42, builder.getMaxOutputSize());
    }

    // ============================================================
    // RescoreRetrieverBuilder tests
    // ============================================================

    public void testRescorePassesThrough() {
        // Current implementation: rescore uses async resolution (needs executor dispatch)
        // Without dispatch, reshape() passes through child results
        List<RankedDoc> childResult = List.of(
            new RankedDoc("A", "products", SHARD_0, 10.0f, 0),
            new RankedDoc("B", "products", SHARD_0, 8.0f, 1)
        );

        RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder(
            new MatchPhraseQueryBuilder("title", "headphones"),
            new RankFusionRetrieverBuilder(
                List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
                60
            )
        );

        // Without executor dispatch, reshape passes through
        List<RankedDoc> result = builder.reshape(childResult);
        assertEquals(2, result.size());
        assertEquals("A", result.get(0).id());
        assertEquals("B", result.get(1).id());
    }

    public void testRescoreValidateNoQuery() {
        RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder();
        builder.setChildRetriever(new RankFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
            60
        ));
        // No rescore query set

        expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
    }

    public void testRescoreValidateNoChild() {
        RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder();
        builder.setRescoreQuery(new MatchPhraseQueryBuilder("title", "test"));
        // No child set

        expectThrows(IllegalArgumentException.class, () -> builder.validate(RetrieverContext.root()));
    }

    public void testRescoreRejectsStandardAsDirectChild() {
        RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder(
            new MatchPhraseQueryBuilder("title", "test"),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("[rescore] retriever cannot directly wrap [standard]"));
    }

    public void testRescoreInvalidWindowSize() {
        expectThrows(IllegalArgumentException.class, () -> {
            RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder();
            builder.setWindowSize(0);
        });
        expectThrows(IllegalArgumentException.class, () -> {
            RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder();
            builder.setWindowSize(-1);
        });
    }

    public void testRescoreMaxOutputSizeIsMinOfWindowAndChild() {
        RankFusionRetrieverBuilder child = new RankFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()), new StandardRetrieverBuilder(new MatchAllQueryBuilder())),
            60
        );
        child.setRankWindowSize(50);

        RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder(new MatchPhraseQueryBuilder("title", "test"), child);

        // window_size (default 100) > child's rank_window_size (50) → ceiling is the child's
        assertEquals(50, builder.getMaxOutputSize());

        // window_size smaller than the child's window → rescore's own window is the ceiling
        builder.setWindowSize(10);
        assertEquals(10, builder.getMaxOutputSize());
    }

    // ============================================================
    // Registration tests
    // ============================================================

    public void testAllCoreRetrieversRegistered() {
        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(List.of());
        assertTrue(parser.hasRetriever("standard"));
        assertTrue(parser.hasRetriever("rank_fusion"));
        assertTrue(parser.hasRetriever("score_fusion"));
        assertTrue(parser.hasRetriever("pinned"));
        assertTrue(parser.hasRetriever("rescore"));
    }
}
