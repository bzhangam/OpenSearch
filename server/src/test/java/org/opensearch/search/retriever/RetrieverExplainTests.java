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
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for the coordinator-assembled {@code _explanation} tree. Each retriever type's
 * {@link RetrieverBuilder#buildExplanation(String, String)} is exercised over hand-built resolved
 * candidates (with leg Lucene explanations attached), asserting: the root value equals the node's fused
 * score, the per-leg detail structure and descriptions, present-vs-absent-leg handling, and bottom-up
 * nesting. The exact fusion arithmetic is pinned in the per-type builder tests; here the focus is the
 * explanation structure and that it mirrors the score that produced the ranking.
 */
public class RetrieverExplainTests extends OpenSearchTestCase {

    private static final String INDEX = "products";
    private static final ShardId SHARD = new ShardId(new Index(INDEX, "uuid"), 0);

    /** A candidate carrying a leaf Lucene explanation, as extractCandidates would build under explain. */
    private static RetrieverCandidate cand(String id, float score, int position) {
        Explanation legExplanation = Explanation.match(score, "weight(title:headphones) [" + id + "]");
        return new RetrieverCandidate(INDEX, SHARD, id, score, position, legExplanation);
    }

    /** A resolved standard leaf over the given candidates (searchResult -> resolvedResult via doResolve). */
    private static StandardRetrieverBuilder resolvedLeaf(List<RetrieverCandidate> candidates) {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSearchResult(candidates);
        leaf.doResolve();
        return leaf;
    }

    private static Explanation childByPrefix(Explanation root, String prefix) {
        for (Explanation d : root.getDetails()) {
            if (d.getDescription().startsWith(prefix)) {
                return d;
            }
        }
        throw new AssertionError("no child detail starting with [" + prefix + "] in: " + root);
    }

    // ---- standard leaf ----

    public void testStandardLeafReturnsLegExplanation() {
        StandardRetrieverBuilder leaf = resolvedLeaf(List.of(cand("a", 2.0f, 0), cand("b", 1.0f, 1)));
        Explanation e = leaf.buildExplanation(INDEX, "a");
        assertNotNull(e);
        assertEquals(2.0f, e.getValue().floatValue(), 1e-6f);
        assertTrue(e.getDescription(), e.getDescription().contains("weight(title:headphones)"));
    }

    public void testStandardLeafReturnsNullForAbsentDoc() {
        StandardRetrieverBuilder leaf = resolvedLeaf(List.of(cand("a", 2.0f, 0)));
        assertNull(leaf.buildExplanation(INDEX, "zzz"));
    }

    // ---- rank_fusion ----

    public void testRankFusionExplanationStructureAndValue() {
        // Leg A ranks a(1), b(2); Leg B ranks b(1), c(2). rank_constant default 60.
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 3.0f, 0), cand("b", 2.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("b", 9.0f, 0), cand("c", 4.0f, 1)));
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(legA, legB));
        rf.doResolve();

        // Doc b is in both legs: rank 2 in A, rank 1 in B -> 1/62 + 1/61.
        Explanation b = rf.buildExplanation(INDEX, "b");
        assertNotNull(b);
        assertTrue(b.getDescription(), b.getDescription().contains("rank_fusion"));
        assertTrue(b.getDescription(), b.getDescription().contains("rank_constant=60"));
        assertEquals(2, b.getDetails().length);
        // Root value equals the fused score, and the two present-leg contributions sum to it.
        double leg0 = 1.0 / (60 + 2);
        double leg1 = 1.0 / (60 + 1);
        assertEquals(leg0 + leg1, b.getValue().doubleValue(), 1e-6);
        Explanation leg0Detail = childByPrefix(b, "leg 0:");
        Explanation leg1Detail = childByPrefix(b, "leg 1:");
        assertEquals(leg0, leg0Detail.getValue().doubleValue(), 1e-6);
        assertEquals(leg1, leg1Detail.getValue().doubleValue(), 1e-6);
        // Each present leg nests the leaf Lucene explanation.
        assertEquals(1, leg0Detail.getDetails().length);
        assertTrue(leg0Detail.getDetails()[0].getDescription().contains("weight(title:headphones)"));
    }

    public void testRankFusionExplanationMarksAbsentLeg() {
        // Doc a is only in leg A; leg B must be rendered as "not present".
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 3.0f, 0), cand("b", 2.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("b", 9.0f, 0)));
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(legA, legB));
        rf.doResolve();

        Explanation a = rf.buildExplanation(INDEX, "a");
        assertNotNull(a);
        // a: only leg A rank 1 -> 1/61
        assertEquals(1.0 / 61, a.getValue().doubleValue(), 1e-6);
        Explanation leg1Detail = childByPrefix(a, "leg 1:");
        assertFalse("absent leg is a no-match node", leg1Detail.isMatch());
        assertTrue(leg1Detail.getDescription(), leg1Detail.getDescription().contains("not present"));
    }

    // ---- score_fusion ----

    public void testScoreFusionExplanationShowsNormalizationAndWeight() {
        // Leg A: a=10, b=6 -> min6,max10 -> norm a=1.0, b=0.001(floor). Leg B: a=4 (single doc -> 1.0).
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 10.0f, 0), cand("b", 6.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("a", 4.0f, 0)));
        ScoreFusionRetrieverBuilder sf = new ScoreFusionRetrieverBuilder(List.of(legA, legB));
        sf.doResolve();

        // a: (1.0 + 1.0)/2 = 1.0
        Explanation a = sf.buildExplanation(INDEX, "a");
        assertNotNull(a);
        assertTrue(a.getDescription(), a.getDescription().contains("score_fusion"));
        assertTrue(a.getDescription(), a.getDescription().contains("min_max"));
        assertTrue(a.getDescription(), a.getDescription().contains("arithmetic_mean"));
        assertEquals(1.0f, a.getValue().floatValue(), 1e-4f);
        assertEquals(2, a.getDetails().length);
        Explanation leg0 = childByPrefix(a, "leg 0:");
        assertTrue(leg0.getDescription(), leg0.getDescription().contains("norm=1.0"));
        assertTrue(leg0.getDescription(), leg0.getDescription().contains("weight=1.0"));
        assertEquals(1, leg0.getDetails().length); // nested leaf explanation
    }

    public void testScoreFusionExplanationMarksAbsentLegAndWeight() {
        // b only in leg A. weights [1,3]. Leg A: a=10,b=6 -> b norm 0.001. Leg B absent for b.
        StandardRetrieverBuilder legA = resolvedLeaf(List.of(cand("a", 10.0f, 0), cand("b", 6.0f, 1)));
        StandardRetrieverBuilder legB = resolvedLeaf(List.of(cand("a", 4.0f, 0)));
        ScoreFusionRetrieverBuilder sf = new ScoreFusionRetrieverBuilder(List.of(legA, legB));
        sf.setWeights(new float[] { 1.0f, 3.0f });
        sf.doResolve();

        Explanation b = sf.buildExplanation(INDEX, "b");
        assertNotNull(b);
        Explanation leg1 = childByPrefix(b, "leg 1:");
        assertFalse(leg1.isMatch());
        assertTrue(leg1.getDescription(), leg1.getDescription().contains("not present"));
        assertTrue(leg1.getDescription(), leg1.getDescription().contains("weight=3.0"));
    }

    // ---- nested fusion (bottom-up) ----

    public void testNestedFusionExplanationRecurses() {
        // rank_fusion( rank_fusion(A, B), C ). Doc a present in inner A.
        StandardRetrieverBuilder a = resolvedLeaf(List.of(cand("a", 3.0f, 0), cand("b", 2.0f, 1)));
        StandardRetrieverBuilder b = resolvedLeaf(List.of(cand("b", 9.0f, 0)));
        RankFusionRetrieverBuilder inner = new RankFusionRetrieverBuilder(List.of(a, b));
        inner.doResolve();
        StandardRetrieverBuilder c = resolvedLeaf(List.of(cand("c", 5.0f, 0)));
        RankFusionRetrieverBuilder outer = new RankFusionRetrieverBuilder(List.of(inner, c));
        outer.doResolve();

        Explanation aExp = outer.buildExplanation(INDEX, "a");
        assertNotNull(aExp);
        assertTrue(aExp.getDescription().contains("rank_fusion"));
        // leg 0 of the outer is the INNER rank_fusion for doc a -> its nested explanation is itself a
        // rank_fusion node (depth >= 3): outer -> inner leg detail -> inner rank_fusion -> its leg detail.
        Explanation outerLeg0 = childByPrefix(aExp, "leg 0:");
        assertEquals(1, outerLeg0.getDetails().length);
        Explanation innerNode = outerLeg0.getDetails()[0];
        assertTrue(innerNode.getDescription(), innerNode.getDescription().contains("rank_fusion"));
        assertTrue("inner fusion has its own leg details", innerNode.getDetails().length >= 1);
    }
}
