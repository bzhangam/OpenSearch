/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Unit tests for {@link RetrieverParser} — the registry-based dispatch of the top-level {@code retriever}
 * field to a concrete {@link RetrieverBuilder} by type name.
 */
public class RetrieverParserTests extends OpenSearchTestCase {

    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Constructing a SearchModule registers query parsers (match_all, etc.) AND sets the global
        // retriever parser (registers `standard`), which is what the fallback path in the base uses.
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private RetrieverBuilder parse(String json) throws Exception {
        try (XContentParser parser = createParser(jsonXContent, json)) {
            // position at the START_OBJECT of the retriever value
            parser.nextToken();
            return RetrieverBuilder.parseInnerRetrieverBuilder(parser);
        }
    }

    public void testParseStandard() throws Exception {
        RetrieverBuilder rb = parse("{\"standard\":{\"query\":{\"match_all\":{}}}}");
        assertTrue(rb instanceof StandardRetrieverBuilder);
        assertEquals("standard", rb.getName());
        assertNotNull(((StandardRetrieverBuilder) rb).getQueryBuilder());
    }

    public void testUnknownTypeRejected() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse("{\"nope\":{}}"));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown retriever type [nope]"));
    }

    public void testEmptyRetrieverObjectRejected() throws Exception {
        // { } — no type field
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse("{}"));
        assertTrue(e.getMessage(), e.getMessage().contains("exactly one retriever type"));
    }

    public void testTrailingSecondTypeRejected() throws Exception {
        // two DISTINCT type keys in one retriever object → after parsing the first, a trailing FIELD_NAME
        // remains, which the parser rejects. (A duplicate key would be rejected earlier by JSON itself.)
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parse("{\"standard\":{\"query\":{\"match_all\":{}}},\"other\":{}}")
        );
        assertTrue(e.getMessage(), e.getMessage().contains("exactly one retriever type"));
    }

    public void testFallbackRegistryParsesStandardWithoutGlobalParser() throws Exception {
        // Force the fallback path: clear the global parser so parseInnerRetrieverBuilder uses FALLBACK_PARSER.
        RetrieverParser previous = SearchSourceBuilderRetrieverIntegration.getGlobalRetrieverParser();
        try {
            SearchSourceBuilderRetrieverIntegration.resetGlobalRetrieverParser();
            RetrieverBuilder rb = parse("{\"standard\":{\"query\":{\"match_all\":{}}}}");
            assertTrue(rb instanceof StandardRetrieverBuilder);
        } finally {
            SearchSourceBuilderRetrieverIntegration.setGlobalRetrieverParser(previous);
        }
    }

    public void testRegistryBuilderRejectsDuplicate() {
        RetrieverParser.Builder builder = RetrieverParser.builder();
        builder.register("dup", p -> new StandardRetrieverBuilder());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder.register("dup", p -> new StandardRetrieverBuilder())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }

    public void testHasRetrieverAndRegisteredTypes() {
        RetrieverParser parser = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());
        assertTrue(parser.hasRetriever("standard"));
        assertFalse(parser.hasRetriever("nope"));
        List<String> types = new ArrayList<>();
        parser.getRegisteredTypes().forEach(types::add);
        assertTrue(types.contains("standard"));
    }
}
