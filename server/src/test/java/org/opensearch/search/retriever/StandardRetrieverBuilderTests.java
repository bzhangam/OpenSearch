/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.retriever.constraints.DisallowTrackScoresFalse;
import org.opensearch.search.retriever.modifiers.ForceTrackScores;
import org.opensearch.search.retriever.modifiers.RequireDocvalueField;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link StandardRetrieverBuilder}.
 */
public class StandardRetrieverBuilderTests extends OpenSearchTestCase {

    /**
     * fromXContent round-trip tests parse a real query (match_all) inside "query", which requires
     * the QueryBuilder named-XContent category — not present in the base OpenSearchTestCase registry.
     * Built once (not per-call) purely to avoid rebuilding a SearchModule on every parse call;
     * setGlobalRetrieverParser() is now idempotent so constructing SearchModule here no longer
     * risks colliding with other test classes' instances sharing the same JVM fork.
     */
    private static final org.opensearch.core.xcontent.NamedXContentRegistry QUERY_XCONTENT_REGISTRY =
        new org.opensearch.core.xcontent.NamedXContentRegistry(
            new org.opensearch.search.SearchModule(org.opensearch.common.settings.Settings.EMPTY, Collections.emptyList())
                .getNamedXContents()
        );

    @Override
    protected org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry() {
        return QUERY_XCONTENT_REGISTRY;
    }

    // === Validation tests ===

    public void testValidateRequiresQuery() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder();
        // No query set

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("[standard] requires [query]"));
    }

    public void testValidatePassesWithQuery() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        // Should not throw
        builder.validate(RetrieverContext.root());
    }

    public void testValidateAppliesAncestorConstraints() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setTrackScores(false);

        RetrieverContext ctx = RetrieverContext.root()
            .withConstraint(new DisallowTrackScoresFalse("score_fusion"));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.validate(ctx)
        );
        assertTrue(e.getMessage().contains("cannot disable [track_scores]"));
    }

    // === prepareLeaves tests ===

    public void testPrepareLeavesAppliesModifiers() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        assertNull(builder.getTrackScores());

        RetrieverContext ctx = RetrieverContext.root()
            .withModifier(ForceTrackScores.INSTANCE)
            .withModifier(new RequireDocvalueField("embedding"));

        builder.prepareLeaves(ctx);

        assertTrue(builder.getTrackScores());
        assertNotNull(builder.getDocvalueFields());
        assertEquals(1, builder.getDocvalueFields().size());
        assertEquals("embedding", builder.getDocvalueFields().get(0).field);
    }

    // === toQueryBuilder tests ===

    public void testToQueryBuilderWithoutFilter() {
        MatchQueryBuilder query = new MatchQueryBuilder("title", "hello");
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(query);

        QueryBuilder result = builder.toQueryBuilder();
        assertSame(query, result);
    }

    public void testToQueryBuilderWithFilter() {
        MatchQueryBuilder query = new MatchQueryBuilder("title", "hello");
        TermQueryBuilder filter = new TermQueryBuilder("status", "active");

        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(query);
        builder.setFilterBuilder(filter);

        QueryBuilder result = builder.toQueryBuilder();
        assertTrue(result instanceof BoolQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) result;
        assertEquals(1, bool.must().size());
        assertEquals(1, bool.filter().size());
        assertSame(query, bool.must().get(0));
        assertSame(filter, bool.filter().get(0));
    }

    // === extractAggregationQuery tests ===

    public void testExtractAggregationQueryMatchesToQueryBuilder() {
        MatchQueryBuilder query = new MatchQueryBuilder("title", "test");
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(query);

        QueryBuilder aggQuery = builder.extractAggregationQuery();
        assertSame(query, aggQuery);
    }

    // === collectLeaves tests ===

    public void testCollectLeavesReturnsSelf() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        List<StandardRetrieverBuilder> leaves = builder.collectLeaves();

        assertEquals(1, leaves.size());
        assertSame(builder, leaves.get(0));
    }

    // === toSearchRequest tests ===

    public void testToSearchRequestBasic() {
        MatchQueryBuilder query = new MatchQueryBuilder("title", "test");
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(query);
        builder.setSize(50);

        SearchRequest original = new SearchRequest("products");
        SearchRequest legRequest = builder.toSearchRequest(new String[]{"products"}, original);

        assertEquals(1, legRequest.indices().length);
        assertEquals("products", legRequest.indices()[0]);
        assertNotNull(legRequest.source());
        assertEquals(50, legRequest.source().size());
    }

    public void testToSearchRequestWithMinScore() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setMinScore(5.0f);

        SearchRequest legRequest = builder.toSearchRequest(new String[]{"idx"}, new SearchRequest("idx"));

        assertEquals(5.0f, legRequest.source().minScore(), 0.0001f);
    }

    public void testToSearchRequestPropagatesPreference() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        SearchRequest original = new SearchRequest("idx");
        original.preference("_local");

        SearchRequest legRequest = builder.toSearchRequest(new String[]{"idx"}, original);
        assertEquals("_local", legRequest.preference());
    }

    public void testToSearchRequestPropagatesRouting() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        SearchRequest original = new SearchRequest("idx");
        original.routing("shard_1");

        SearchRequest legRequest = builder.toSearchRequest(new String[]{"idx"}, original);
        assertEquals("shard_1", legRequest.routing());
    }

    // === getChildRetrievers tests ===

    public void testGetChildRetrieversIsEmpty() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        assertTrue(builder.getChildRetrievers().isEmpty());
    }

    // === getName tests ===

    public void testGetName() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder();
        assertEquals("standard", builder.getName());
    }

    // === resolveBottomUp tests ===

    public void testResolveBottomUpUsesSearchResult() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        // Simulate executor setting results
        List<RankedDoc> fakeResults = Collections.emptyList();
        builder.setSearchResult(fakeResults);
        builder.resolveBottomUp();

        assertSame(fakeResults, builder.getResolvedResult());
    }

    // === search_after (per-leg pagination) tests ===

    public void testValidateSearchAfterRequiresSort() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setSearchAfter(new Object[] { 29.99, "doc_5" });
        // No sort set

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.validate(RetrieverContext.root())
        );
        assertTrue(e.getMessage().contains("[sort]"));
        assertTrue(e.getMessage().contains("[search_after]"));
    }

    public void testValidateSearchAfterWithSortPasses() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setSorts(List.of(
            new org.opensearch.search.sort.FieldSortBuilder("price").order(org.opensearch.search.sort.SortOrder.ASC),
            new org.opensearch.search.sort.FieldSortBuilder("_id").order(org.opensearch.search.sort.SortOrder.ASC)
        ));
        builder.setSearchAfter(new Object[] { 29.99, "doc_5" });

        // Should not throw
        builder.validate(RetrieverContext.root());
    }

    public void testToSearchRequestPropagatesSearchAfter() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setSorts(List.of(new org.opensearch.search.sort.FieldSortBuilder("price").order(org.opensearch.search.sort.SortOrder.ASC)));
        Object[] cursor = new Object[] { 29.99, "doc_5" };
        builder.setSearchAfter(cursor);

        SearchRequest request = builder.toSearchRequest(new String[] { "products" }, null);

        assertNotNull(request.source().searchAfter());
        assertArrayEquals(cursor, request.source().searchAfter());
    }

    public void testToSearchRequestWithoutSearchAfterOmitsCursor() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        SearchRequest request = builder.toSearchRequest(new String[] { "products" }, null);

        assertNull(request.source().searchAfter());
    }

    public void testGetSearchAfterReturnsSetValue() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        Object[] cursor = new Object[] { "value1", 42 };
        builder.setSearchAfter(cursor);

        assertArrayEquals(cursor, builder.getSearchAfter());
    }

    public void testGetSearchAfterDefaultsToNull() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        assertNull(builder.getSearchAfter());
    }

    // === from (leaf-level windowing) tests ===

    public void testGetFromDefaultsToZero() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        assertEquals(0, builder.getFrom());
    }

    public void testGetFromReturnsSetValue() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setFrom(20);
        assertEquals(20, builder.getFrom());
    }

    public void testToSearchRequestPropagatesFrom() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setFrom(20);
        builder.setSize(50);

        SearchRequest request = builder.toSearchRequest(new String[] { "products" }, null);

        assertEquals(20, request.source().from());
        assertEquals(50, request.source().size());
    }

    public void testToSearchRequestDefaultFromIsZero() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());

        SearchRequest request = builder.toSearchRequest(new String[] { "products" }, null);

        assertEquals(0, request.source().from());
    }

    // === getMaxOutputSize tests ===

    public void testGetMaxOutputSizeReturnsSize() {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setSize(250);
        assertEquals(250, builder.getMaxOutputSize());
    }

    public void testGetMaxOutputSizeUnaffectedByFrom() {
        // from only shifts which window of this leg's own ranking is returned — the leg's own
        // dispatched request always returns up to `size` docs, regardless of `from`.
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        builder.setSize(50);
        builder.setFrom(500);
        assertEquals(50, builder.getMaxOutputSize());
    }

    // === fromXContent round-trip ===

    public void testFromXContentParsesFromAndSize() throws IOException {
        String json = "{\"query\":{\"match_all\":{}},\"from\":20,\"size\":50}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // START_OBJECT

        StandardRetrieverBuilder builder = StandardRetrieverBuilder.fromXContent(parser);

        assertEquals(20, builder.getFrom());
        assertEquals(50, builder.getSize());
    }

    public void testFromXContentDefaultsFromToZero() throws IOException {
        String json = "{\"query\":{\"match_all\":{}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // START_OBJECT

        StandardRetrieverBuilder builder = StandardRetrieverBuilder.fromXContent(parser);

        assertEquals(0, builder.getFrom());
    }
}
