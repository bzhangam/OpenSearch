/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for the {@link CompoundRetrieverBuilder} base contract, exercised through its concrete
 * implementation {@link RankFusionRetrieverBuilder}: leaf collection (incl. nested), the ≥2-children
 * validation + recursion, child traversal, and the {@code bool.should} aggregation-query union.
 */
public class CompoundRetrieverBuilderTests extends OpenSearchTestCase {

    private static StandardRetrieverBuilder std(QueryBuilder q) {
        return new StandardRetrieverBuilder(q);
    }

    public void testCollectLeavesUnionIncludingNested() {
        StandardRetrieverBuilder a = std(new MatchAllQueryBuilder());
        StandardRetrieverBuilder b = std(new MatchAllQueryBuilder());
        StandardRetrieverBuilder c = std(new MatchAllQueryBuilder());
        // rank_fusion( a, rank_fusion(b, c) ) → leaves = [a, b, c]
        RankFusionRetrieverBuilder inner = new RankFusionRetrieverBuilder(List.of(b, c));
        RankFusionRetrieverBuilder outer = new RankFusionRetrieverBuilder(List.of(a, inner));
        List<StandardRetrieverBuilder> leaves = outer.collectLeaves();
        assertEquals(3, leaves.size());
        assertTrue(leaves.contains(a));
        assertTrue(leaves.contains(b));
        assertTrue(leaves.contains(c));
    }

    public void testGetChildRetrievers() {
        StandardRetrieverBuilder a = std(new MatchAllQueryBuilder());
        StandardRetrieverBuilder b = std(new MatchAllQueryBuilder());
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(a, b));
        assertEquals(List.of(a, b), rf.getChildRetrievers());
    }

    public void testValidateRequiresAtLeastTwoChildren() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RankFusionRetrieverBuilder(List.of(std(new MatchAllQueryBuilder()))).validate()
        );
        assertTrue(e.getMessage(), e.getMessage().contains("at least 2"));

        IllegalArgumentException empty = expectThrows(
            IllegalArgumentException.class,
            () -> new RankFusionRetrieverBuilder(List.of()).validate()
        );
        assertTrue(empty.getMessage(), empty.getMessage().contains("at least 2"));
    }

    public void testValidateRecursesIntoChildren() {
        // A child standard with no query fails its own validate() → the compound's validate must surface it.
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(
            List.of(std(new MatchAllQueryBuilder()), new StandardRetrieverBuilder())
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, rf::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("[standard] requires [query]"));
    }

    public void testExtractAggregationQueryIsBoolShouldUnion() {
        StandardRetrieverBuilder a = std(new TermQueryBuilder("f", "x"));
        StandardRetrieverBuilder b = std(new TermQueryBuilder("f", "y"));
        RankFusionRetrieverBuilder rf = new RankFusionRetrieverBuilder(List.of(a, b));
        QueryBuilder agg = rf.extractAggregationQuery();
        assertTrue(agg instanceof BoolQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) agg;
        assertEquals(2, bool.should().size());
        assertTrue(bool.should().contains(a.extractAggregationQuery()));
        assertTrue(bool.should().contains(b.extractAggregationQuery()));
    }
}
