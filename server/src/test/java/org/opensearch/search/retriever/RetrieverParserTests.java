/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link RetrieverParser}, {@link RetrieverPlugin}, and {@link RetrieverModuleRegistration}.
 */
public class RetrieverParserTests extends OpenSearchTestCase {

    /**
     * Nested child-retriever parsing tests build real queries (e.g. match_all/match_phrase) inside
     * "standard"/"rescore", which requires the QueryBuilder named-XContent category — not present
     * in the base OpenSearchTestCase registry. Built once (not per-call) purely to avoid rebuilding
     * a SearchModule on every parse call; setGlobalRetrieverParser() is now idempotent so
     * constructing SearchModule here no longer risks colliding with other test classes' instances
     * sharing the same JVM fork.
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

    // === RetrieverParser.Builder tests ===

    public void testBuilderRegisterAndBuild() {
        RetrieverParser parser = RetrieverParser.builder()
            .register("test_type", p -> new StandardRetrieverBuilder())
            .build();

        assertTrue(parser.hasRetriever("test_type"));
        assertFalse(parser.hasRetriever("nonexistent"));
    }

    public void testBuilderRejectsDuplicateRegistration() {
        RetrieverParser.Builder builder = RetrieverParser.builder()
            .register("my_type", p -> new StandardRetrieverBuilder());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.register("my_type", p -> new StandardRetrieverBuilder())
        );
        assertTrue(e.getMessage().contains("already registered"));
    }

    public void testBuilderRejectsNullName() {
        expectThrows(
            NullPointerException.class,
            () -> RetrieverParser.builder().register(null, p -> new StandardRetrieverBuilder())
        );
    }

    public void testBuilderRejectsNullParser() {
        expectThrows(
            NullPointerException.class,
            () -> RetrieverParser.builder().register("my_type", null)
        );
    }

    // === RetrieverParser.parse() tests ===

    public void testParseUnknownTypeThrows() throws IOException {
        RetrieverParser parser = RetrieverParser.builder()
            .register("standard", p -> {
                try {
                    return StandardRetrieverBuilder.fromXContent(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .build();

        // Build JSON: {"unknown_type": {"query": {}}}
        XContentBuilder xContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("unknown_type")
            .endObject()
            .endObject();

        XContentParser xParser = createParser(xContent);
        xParser.nextToken(); // START_OBJECT

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(xParser)
        );
        assertTrue(e.getMessage().contains("unknown retriever type [unknown_type]"));
    }

    public void testGetRegisteredTypes() {
        RetrieverParser parser = RetrieverParser.builder()
            .register("type_a", p -> new StandardRetrieverBuilder())
            .register("type_b", p -> new StandardRetrieverBuilder())
            .build();

        int count = 0;
        for (String type : parser.getRegisteredTypes()) {
            assertTrue(type.equals("type_a") || type.equals("type_b"));
            count++;
        }
        assertEquals(2, count);
    }

    // === RetrieverModuleRegistration tests ===

    public void testBuildRetrieverParserRegistersStandard() {
        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());

        assertTrue(parser.hasRetriever("standard"));
    }

    public void testBuildRetrieverParserWithPlugin() {
        org.opensearch.plugins.SearchPlugin mockPlugin = new org.opensearch.plugins.SearchPlugin() {};
        // SearchPlugin without RetrieverPlugin — should be ignored

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(List.of(mockPlugin));
        assertTrue(parser.hasRetriever("standard"));
    }

    public void testBuildRetrieverParserWithRetrieverPlugin() {
        // Create a mock plugin implementing RetrieverPlugin
        MockRetrieverPlugin mockRetrieverPlugin = new MockRetrieverPlugin();

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(List.of(mockRetrieverPlugin));
        assertTrue(parser.hasRetriever("standard"));
        assertTrue(parser.hasRetriever("mock_fusion"));
    }

    // === RetrieverPlugin.RetrieverSpec tests ===

    public void testRetrieverSpecGetters() {
        RetrieverPlugin.RetrieverSpec<StandardRetrieverBuilder> spec = new RetrieverPlugin.RetrieverSpec<>(
            "my_retriever",
            p -> new StandardRetrieverBuilder()
        );

        assertEquals("my_retriever", spec.getName());
        assertNotNull(spec.getParser());
    }

    public void testRetrieverSpecNullNameThrows() {
        expectThrows(
            NullPointerException.class,
            () -> new RetrieverPlugin.RetrieverSpec<>(null, p -> new StandardRetrieverBuilder())
        );
    }

    public void testRetrieverSpecNullParserThrows() {
        expectThrows(
            NullPointerException.class,
            () -> new RetrieverPlugin.RetrieverSpec<StandardRetrieverBuilder>("name", null)
        );
    }

    // === SearchSourceBuilderRetrieverIntegration validation tests ===

    /** Helper: creates a valid compound retriever (not a bare standard) for testing validation. */
    private static RetrieverBuilder validRootRetriever() {
        return new MockCompoundRetrieverBuilder(
            new StandardRetrieverBuilder(new org.opensearch.index.query.MatchAllQueryBuilder()),
            new StandardRetrieverBuilder(new org.opensearch.index.query.MatchAllQueryBuilder())
        );
    }

    public void testValidateCompatibilityNoConflict() {
        // compound retriever alone — no error
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityBareStandardThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(new StandardRetrieverBuilder(new org.opensearch.index.query.MatchAllQueryBuilder()));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("[standard] retriever cannot be used at the top level"));
    }

    public void testValidateCompatibilityWithQueryThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.query(new org.opensearch.index.query.MatchAllQueryBuilder());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("cannot use [retriever] and [query] together"));
    }

    public void testValidateCompatibilityNullRetrieverSkips() {
        // No retriever — should not throw even with query present
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.query(new org.opensearch.index.query.MatchAllQueryBuilder());
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityWithSortThrows() {
        // Top-level sort is now supported — it reorders the final fused output for presentation.
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.sort("price");

        // No exception expected — sort is supported
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityWithSearchAfterThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.searchAfter(new Object[] { "value1", 42 });

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("[retriever] and [search_after]"));
    }

    public void testValidateCompatibilityWithTerminateAfterThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.terminateAfter(100);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("[retriever] and [terminate_after]"));
    }

    public void testValidateCompatibilityWithSuggestDoesNotThrow() {
        // suggest is now supported with retrievers (computed on the global leg) — should NOT throw
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.suggest(new org.opensearch.search.suggest.SuggestBuilder().addSuggestion(
            "my-suggest",
            new org.opensearch.search.suggest.term.TermSuggestionBuilder("title")
        ));

        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityWithExplainThrows() {
        // explain is now supported with retrievers — should NOT throw
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.explain(true);

        // No exception expected — explain is supported
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityWithExplainFalseDoesNotThrow() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.explain(false);

        // explain=false should not trigger the block
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityWithProfileThrows() {
        // profile is now supported with retrievers — should NOT throw
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.profile(true);

        // No exception expected — profile is supported
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityWithIncludeNamedQueriesScoreThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.includeNamedQueriesScores(true);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("[retriever] and [include_named_queries_score]"));
    }

    public void testValidateCompatibilityWithSliceThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.slice(new org.opensearch.search.slice.SliceBuilder("_id", 0, 5));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("[retriever] and [slice]"));
    }

    public void testValidateCompatibilityWithDerivedFieldsThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.derivedField("full_name", "keyword", new org.opensearch.script.Script("params._source.first + params._source.last"));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("[retriever] and [derived_fields]"));
    }

    // === from + size vs. getMaxOutputSize() window validation ===

    public void testValidateCompatibilityFromPlusSizeWithinWindowPasses() {
        // MockCompoundRetrieverBuilder's rank_window_size defaults to 100 (CompoundRetrieverBuilder default)
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.from(50);
        source.size(50); // exactly at the default window boundary

        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityFromPlusSizeExceedsWindowThrows() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());
        source.from(95);
        source.size(10); // 95 + 10 = 105 > default rank_window_size (100)

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("exceeds the retriever's available window"));
    }

    public void testValidateCompatibilityUsesDefaultFromAndSizeWhenUnset() {
        // from/size default to -1 (unset) on SearchSourceBuilder; the check must fall back to
        // SearchService.DEFAULT_FROM (0) / DEFAULT_SIZE (10), well within the default 100 window.
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(validRootRetriever());

        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);
    }

    public void testValidateCompatibilityRespectsRankWindowSize() {
        MockCompoundRetrieverBuilder root = new MockCompoundRetrieverBuilder(
            new StandardRetrieverBuilder(new org.opensearch.index.query.MatchAllQueryBuilder()),
            new StandardRetrieverBuilder(new org.opensearch.index.query.MatchAllQueryBuilder())
        );
        root.setRankWindowSize(20);

        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        source.retriever(root);
        source.from(0);
        source.size(20); // exactly at the narrowed window

        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source);

        source.size(21); // now exceeds the narrowed window
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue(e.getMessage().contains("exceeds the retriever's available window"));
    }

    // === Pipeline validation tests ===

    public void testValidatePipelineCompatibilityRequestProcessorsOnly() {
        // Should not throw — request processors are compatible
        SearchSourceBuilderRetrieverIntegration.validatePipelineCompatibility(
            "my-pipeline", false, false, List.of(), List.of()
        );
    }

    public void testValidatePipelineCompatibilityResponseProcessorThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validatePipelineCompatibility(
                "my-pipeline", true, false, List.of("rerank"), List.of()
            )
        );
        assertTrue(e.getMessage().contains("response processor"));
        assertTrue(e.getMessage().contains("rerank"));
    }

    public void testValidatePipelineCompatibilityPhaseProcessorThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validatePipelineCompatibility(
                "my-pipeline", false, true, List.of(), List.of("normalization-processor")
            )
        );
        assertTrue(e.getMessage().contains("phase results processor"));
        assertTrue(e.getMessage().contains("normalization-processor"));
    }

    // === Nested retriever parsing tests (compound-in-compound / transformer-in-transformer) ===
    //
    // Previously each compound/transformer's fromXContent hardcoded which child type names it
    // would accept (typically just "standard", plus "rank_fusion"/"score_fusion" for the two
    // transformers) — nesting a compound inside a compound, or a transformer inside a transformer,
    // threw "unknown retriever type [...] inside [...]". Child parsing now dispatches through
    // RetrieverBuilder#parseInnerRetrieverBuilder (the same registry as the top-level "retriever"
    // field), so any registered type can nest inside any other.

    public void testRankFusionAcceptsNestedCompoundChild() throws IOException {
        String json = "{"
            + "\"rank_fusion\": {"
            + "  \"retrievers\": ["
            + "    {\"standard\": {\"query\": {\"match_all\": {}}}},"
            + "    {\"score_fusion\": {\"retrievers\": ["
            + "        {\"standard\": {\"query\": {\"match_all\": {}}}},"
            + "        {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "    ]}}"
            + "  ]"
            + "}}";

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        XContentParser xParser = createParser(JsonXContent.jsonXContent, json);
        xParser.nextToken(); // START_OBJECT

        RetrieverBuilder result = parser.parse(xParser);

        assertTrue(result instanceof RankFusionRetrieverBuilder);
        List<RetrieverBuilder> children = ((RankFusionRetrieverBuilder) result).getChildRetrievers();
        assertEquals(2, children.size());
        assertTrue(children.get(0) instanceof StandardRetrieverBuilder);
        assertTrue(children.get(1) instanceof ScoreFusionRetrieverBuilder);
        assertEquals(2, ((ScoreFusionRetrieverBuilder) children.get(1)).getChildRetrievers().size());
    }

    public void testPinnedAcceptsNestedRescoreChild() throws IOException {
        // Previously blocked: pinned's hardcoded dispatch only accepted standard/rank_fusion/score_fusion.
        String json = "{"
            + "\"pinned\": {"
            + "  \"ids\": [\"doc1\"],"
            + "  \"retriever\": {"
            + "    \"rescore\": {"
            + "      \"query\": {\"match_phrase\": {\"title\": \"test\"}},"
            + "      \"retriever\": {"
            + "        \"rank_fusion\": {"
            + "          \"retrievers\": ["
            + "            {\"standard\": {\"query\": {\"match_all\": {}}}},"
            + "            {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "          ]"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}}";

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        XContentParser xParser = createParser(JsonXContent.jsonXContent, json);
        xParser.nextToken();

        RetrieverBuilder result = parser.parse(xParser);

        assertTrue(result instanceof PinnedRetrieverBuilder);
        RetrieverBuilder child = ((PinnedRetrieverBuilder) result).getChildRetriever();
        assertTrue(child instanceof RescoreRetrieverBuilder);
        assertTrue(((RescoreRetrieverBuilder) child).getChildRetriever() instanceof RankFusionRetrieverBuilder);
    }

    public void testRescoreAcceptsNestedPinnedChild() throws IOException {
        // Previously blocked: rescore's hardcoded dispatch only accepted standard/rank_fusion/score_fusion.
        String json = "{"
            + "\"rescore\": {"
            + "  \"query\": {\"match_phrase\": {\"title\": \"test\"}},"
            + "  \"retriever\": {"
            + "    \"pinned\": {"
            + "      \"ids\": [\"doc1\"],"
            + "      \"retriever\": {"
            + "        \"rank_fusion\": {"
            + "          \"retrievers\": ["
            + "            {\"standard\": {\"query\": {\"match_all\": {}}}},"
            + "            {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "          ]"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}}";

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        XContentParser xParser = createParser(JsonXContent.jsonXContent, json);
        xParser.nextToken();

        RetrieverBuilder result = parser.parse(xParser);

        assertTrue(result instanceof RescoreRetrieverBuilder);
        assertTrue(((RescoreRetrieverBuilder) result).getChildRetriever() instanceof PinnedRetrieverBuilder);
    }

    // === Unknown-field rejection tests (symmetry with StandardRetrieverBuilder's default-throw switch) ===

    public void testRankFusionRejectsUnknownField() throws IOException {
        // Also covers the pre-rename field name "size" (now rank_window_size) — previously silently
        // ignored instead of rejected.
        String json = "{\"rank_fusion\": {\"size\": 2, \"retrievers\": ["
            + "{\"standard\": {\"query\": {\"match_all\": {}}}}, {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "]}}";
        assertUnknownFieldRejected(json, "[rank_fusion] unknown field [size]");
    }

    public void testScoreFusionRejectsUnknownField() throws IOException {
        String json = "{\"score_fusion\": {\"size\": 2, \"retrievers\": ["
            + "{\"standard\": {\"query\": {\"match_all\": {}}}}, {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "]}}";
        assertUnknownFieldRejected(json, "[score_fusion] unknown field [size]");
    }

    public void testPinnedRejectsUnknownField() throws IOException {
        String json = "{\"pinned\": {\"ids\": [\"1\"], \"bogus_field\": 1, "
            + "\"retriever\": {\"standard\": {\"query\": {\"match_all\": {}}}}}}";
        assertUnknownFieldRejected(json, "[pinned] unknown field [bogus_field]");
    }

    public void testRescoreRejectsUnknownField() throws IOException {
        String json = "{\"rescore\": {\"query\": {\"match_all\": {}}, \"bogus_field\": 1, "
            + "\"retriever\": {\"rank_fusion\": {\"retrievers\": ["
            + "{\"standard\": {\"query\": {\"match_all\": {}}}}, {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "]}}}}";
        assertUnknownFieldRejected(json, "[rescore] unknown field [bogus_field]");
    }

    private void assertUnknownFieldRejected(String json, String expectedMessageFragment) throws IOException {
        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        XContentParser xParser = createParser(JsonXContent.jsonXContent, json);
        xParser.nextToken();

        Exception e = expectThrows(Exception.class, () -> parser.parse(xParser));
        // fromXContent lambdas in RetrieverModuleRegistration wrap parse failures in RuntimeException
        Throwable cause = e instanceof RuntimeException && e.getCause() != null ? e.getCause() : e;
        assertTrue(cause.getMessage(), cause.getMessage().contains(expectedMessageFragment));
    }

    // === rescore "query" field: full query DSL support (was previously a single-field-match hack) ===

    public void testRescoreQueryAcceptsMatchAll() throws IOException {
        String json = "{\"rescore\": {\"query\": {\"match_all\": {}}, \"retriever\": {\"rank_fusion\": {\"retrievers\": ["
            + "{\"standard\": {\"query\": {\"match_all\": {}}}}, {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "]}}}}";

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        XContentParser xParser = createParser(JsonXContent.jsonXContent, json);
        xParser.nextToken();

        RetrieverBuilder result = parser.parse(xParser);
        assertTrue(result instanceof RescoreRetrieverBuilder);
        assertTrue(((RescoreRetrieverBuilder) result).getRescoreQuery() instanceof org.opensearch.index.query.MatchAllQueryBuilder);
    }

    public void testRescoreQueryAcceptsBoolQuery() throws IOException {
        String json = "{\"rescore\": {"
            + "\"query\": {\"bool\": {\"must\": [{\"match\": {\"title\": \"headphones\"}}]}}, "
            + "\"retriever\": {\"rank_fusion\": {\"retrievers\": ["
            + "{\"standard\": {\"query\": {\"match_all\": {}}}}, {\"standard\": {\"query\": {\"match_all\": {}}}}"
            + "]}}}}";

        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        XContentParser xParser = createParser(JsonXContent.jsonXContent, json);
        xParser.nextToken();

        RetrieverBuilder result = parser.parse(xParser);
        assertTrue(result instanceof RescoreRetrieverBuilder);
        assertTrue(((RescoreRetrieverBuilder) result).getRescoreQuery() instanceof org.opensearch.index.query.BoolQueryBuilder);
    }

    // === Mock plugin for testing ===

    private static class MockRetrieverPlugin extends Plugin implements org.opensearch.plugins.SearchPlugin, RetrieverPlugin {
        @Override
        public List<RetrieverSpec<?>> getRetrievers() {
            return List.of(
                new RetrieverSpec<>("mock_fusion", p -> new StandardRetrieverBuilder())
            );
        }
    }

    /** Minimal compound retriever for validation tests — just holds two children. */
    private static class MockCompoundRetrieverBuilder extends CompoundRetrieverBuilder {
        MockCompoundRetrieverBuilder(RetrieverBuilder child1, RetrieverBuilder child2) {
            this.childRetrievers = List.of(child1, child2);
        }

        @Override
        protected List<RankedDoc> fuse(List<List<RankedDoc>> childResults) {
            return List.of(); // Not needed for validation tests
        }

        @Override
        protected org.apache.lucene.search.Explanation buildFusionExplanation(String docId, String docIndex) {
            return null; // Not needed for validation tests
        }

        @Override
        public String getName() {
            return "mock_compound";
        }

        @Override
        public org.opensearch.core.xcontent.XContentBuilder toXContent(
            org.opensearch.core.xcontent.XContentBuilder builder,
            org.opensearch.core.xcontent.ToXContent.Params params
        ) throws IOException {
            return builder; // Not needed for validation tests
        }
    }
}
