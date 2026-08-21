/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.retriever.constraints.DisallowTrackScoresFalse;
import org.opensearch.search.retriever.modifiers.ForceTrackScores;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link RetrieverExecutor} lifecycle phases.
 * <p>
 * These tests validate the tree traversal logic without requiring a real Client or MultiSearchResponse.
 * The executor's async dispatch is tested separately via integration tests.
 */
public class RetrieverExecutorTests extends OpenSearchTestCase {

    // === Phase 1: Validate ===

    public void testValidateSingleLeaf() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        // Should not throw
        RetrieverContext ctx = RetrieverContext.root();
        leaf.validate(ctx);
    }

    public void testValidateLeafWithoutQueryFails() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> leaf.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("[standard] requires [query]"));
    }

    // === Phase 2: Prepare Leaves ===

    public void testPrepareLeavesPropagatesModifiers() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        RetrieverContext ctx = RetrieverContext.root()
            .withModifier(ForceTrackScores.INSTANCE);

        leaf.prepareLeaves(ctx);
        assertTrue(leaf.getTrackScores());
    }

    // === Phase 3: Collect Leaves ===

    public void testCollectLeavesFromSingleLeaf() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        List<StandardRetrieverBuilder> leaves = leaf.collectLeaves();

        assertEquals(1, leaves.size());
        assertSame(leaf, leaves.get(0));
    }

    public void testCollectLeavesFromCompound() {
        StandardRetrieverBuilder leaf1 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "a"));
        StandardRetrieverBuilder leaf2 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "b"));

        TestCompoundRetriever compound = new TestCompoundRetriever(List.of(leaf1, leaf2));

        List<StandardRetrieverBuilder> leaves = compound.collectLeaves();
        assertEquals(2, leaves.size());
        assertSame(leaf1, leaves.get(0));
        assertSame(leaf2, leaves.get(1));
    }

    public void testCollectLeavesFromNestedTree() {
        StandardRetrieverBuilder leaf1 = new StandardRetrieverBuilder(new MatchQueryBuilder("a", "x"));
        StandardRetrieverBuilder leaf2 = new StandardRetrieverBuilder(new MatchQueryBuilder("b", "y"));
        StandardRetrieverBuilder leaf3 = new StandardRetrieverBuilder(new MatchQueryBuilder("c", "z"));

        TestCompoundRetriever inner = new TestCompoundRetriever(List.of(leaf1, leaf2));
        TestTransformerRetriever outer = new TestTransformerRetriever(inner);

        // The transformer wraps the compound, which has 2 leaves
        List<StandardRetrieverBuilder> leaves = outer.collectLeaves();
        assertEquals(2, leaves.size());
    }

    // === Phase 4/5: Resolve bottom-up ===

    public void testResolveBottomUpSingleLeaf() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        List<RankedDoc> mockResults = List.of();
        leaf.setSearchResult(mockResults);

        leaf.resolveBottomUp();
        assertSame(mockResults, leaf.getResolvedResult());
    }

    public void testResolveBottomUpCompound() {
        StandardRetrieverBuilder leaf1 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "a"));
        StandardRetrieverBuilder leaf2 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "b"));

        TestCompoundRetriever compound = new TestCompoundRetriever(List.of(leaf1, leaf2));

        // Set mock results
        leaf1.setSearchResult(List.of());
        leaf2.setSearchResult(List.of());

        compound.resolveBottomUp();
        assertNotNull(compound.getResolvedResult());
    }

    // === extractAggregationQuery ===

    public void testExtractAggregationQueryFromCompound() {
        MatchQueryBuilder q1 = new MatchQueryBuilder("title", "shoes");
        MatchQueryBuilder q2 = new MatchQueryBuilder("title", "boots");
        StandardRetrieverBuilder leaf1 = new StandardRetrieverBuilder(q1);
        StandardRetrieverBuilder leaf2 = new StandardRetrieverBuilder(q2);

        TestCompoundRetriever compound = new TestCompoundRetriever(List.of(leaf1, leaf2));

        QueryBuilder aggQuery = compound.extractAggregationQuery();
        // Should be a bool.should with both leaf queries
        assertTrue(aggQuery instanceof org.opensearch.index.query.BoolQueryBuilder);
        org.opensearch.index.query.BoolQueryBuilder bool = (org.opensearch.index.query.BoolQueryBuilder) aggQuery;
        assertEquals(2, bool.should().size());
    }

    // === Compound validation ===

    public void testCompoundValidateRequiresMinTwoChildren() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        TestCompoundRetriever compound = new TestCompoundRetriever(List.of(leaf));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> compound.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("requires at least 2 child retrievers"));
    }

    // === Transformer validation ===

    public void testTransformerValidateRequiresChild() {
        TestTransformerRetriever transformer = new TestTransformerRetriever(null);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> transformer.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("requires a child retriever"));
    }

    public void testTransformerResolveBottomUp() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSearchResult(List.of());

        TestTransformerRetriever transformer = new TestTransformerRetriever(leaf);
        transformer.resolveBottomUp();

        assertNotNull(transformer.getResolvedResult());
    }

    // === L8: resolution is idempotent — a resolved node with unchanged children is not re-run ===

    public void testResolveIsIdempotent() {
        StandardRetrieverBuilder leaf1 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "a"));
        StandardRetrieverBuilder leaf2 = new StandardRetrieverBuilder(new MatchQueryBuilder("title", "b"));
        leaf1.setSearchResult(List.of());
        leaf2.setSearchResult(List.of());
        TestCompoundRetriever compound = new TestCompoundRetriever(List.of(leaf1, leaf2));

        // First pass computes (returns true); subsequent passes skip (return false) since nothing is dirty.
        assertTrue(compound.resolve());
        assertFalse(compound.resolve());
        assertFalse(compound.resolve());
        assertNotNull(compound.getResolvedResult());
    }

    // === Test implementation of CompoundRetrieverBuilder for unit testing ===

    private static class TestCompoundRetriever extends CompoundRetrieverBuilder {
        TestCompoundRetriever(List<RetrieverBuilder> children) {
            this.childRetrievers = children;
        }

        @Override
        protected List<RankedDoc> fuse(List<List<RankedDoc>> childResults) {
            // Simple passthrough — concatenate all results
            List<RankedDoc> all = new ArrayList<>();
            for (List<RankedDoc> result : childResults) {
                if (result != null) {
                    all.addAll(result);
                }
            }
            return all;
        }

        @Override
        protected org.apache.lucene.search.Explanation buildFusionExplanation(String docId, String docIndex) {
            return org.apache.lucene.search.Explanation.match(0.0f, "test_compound fusion", java.util.List.of());
        }

        @Override
        public String getName() {
            return "test_compound";
        }

        @Override
        public org.opensearch.core.xcontent.XContentBuilder toXContent(
            org.opensearch.core.xcontent.XContentBuilder builder,
            Params params
        ) {
            return builder;
        }
    }

    // === Test implementation of TransformerRetrieverBuilder for unit testing ===

    private static class TestTransformerRetriever extends TransformerRetrieverBuilder {
        TestTransformerRetriever(RetrieverBuilder child) {
            this.childRetriever = child;
        }

        @Override
        protected List<RankedDoc> reshape(List<RankedDoc> childResult) {
            // Simple passthrough
            return childResult != null ? childResult : List.of();
        }

        @Override
        protected org.apache.lucene.search.Explanation buildReshapeExplanation(
            String docId, String docIndex, org.apache.lucene.search.Explanation childExplanation
        ) {
            return childExplanation != null ? childExplanation
                : org.apache.lucene.search.Explanation.match(0.0f, "test_transformer", java.util.List.of());
        }

        @Override
        public String getName() {
            return "test_transformer";
        }

        @Override
        public org.opensearch.core.xcontent.XContentBuilder toXContent(
            org.opensearch.core.xcontent.XContentBuilder builder,
            Params params
        ) {
            return builder;
        }
    }
}
