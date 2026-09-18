/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Unit tests for {@link StandardRetrieverBuilder} parsing, the tree contract, and validation. A3a scope:
 * parse/contract only — the executor dispatch of {@code toSearchRequest} is exercised in A3b.
 */
public class StandardRetrieverBuilderTests extends OpenSearchTestCase {

    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    /** Parse a {@code standard} body: parser positioned inside the standard object (past its START_OBJECT). */
    private StandardRetrieverBuilder parseStandardBody(String bodyJson) throws Exception {
        try (XContentParser parser = createParser(jsonXContent, bodyJson)) {
            parser.nextToken(); // START_OBJECT of the standard body
            return StandardRetrieverBuilder.fromXContent(parser);
        }
    }

    public void testParseQueryAndDefaults() throws Exception {
        StandardRetrieverBuilder b = parseStandardBody("{\"query\":{\"match_all\":{}}}");
        assertTrue(b.getQueryBuilder() instanceof MatchAllQueryBuilder);
        assertNull(b.getFilterBuilder());
        assertEquals(0, b.getFrom());
        assertEquals(100, b.getSize());
        assertEquals("standard", b.getName());
    }

    public void testParseAllFields() throws Exception {
        StandardRetrieverBuilder b = parseStandardBody(
            "{\"query\":{\"match_all\":{}},\"filter\":{\"match_all\":{}},\"min_score\":0.5,"
                + "\"from\":2,\"size\":25,\"track_scores\":true}"
        );
        assertNotNull(b.getFilterBuilder());
        assertEquals(Float.valueOf(0.5f), b.getMinScore());
        assertEquals(2, b.getFrom());
        assertEquals(25, b.getSize());
        assertEquals(Boolean.TRUE, b.getTrackScores());
    }

    public void testUnknownFieldRejected() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseStandardBody("{\"query\":{\"match_all\":{}},\"bogus\":1}")
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[standard] unknown field [bogus]"));
    }

    public void testCollectLeavesAndChildren() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        assertEquals(Collections.singletonList(b), b.collectLeaves());
        assertTrue(b.getChildRetrievers().isEmpty());
    }

    public void testValidateNullQueryRejected() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, b::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("[standard] requires [query]"));
    }

    public void testValidateHybridInLeafRejected() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new HybridNamedQueryBuilder());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, b::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("[hybrid] query is not allowed inside [standard]"));
    }

    public void testValidateSearchAfterWithoutSortRejected() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        b.setSearchAfter(new Object[] { "x" });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, b::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("[standard] requires [sort]"));
    }

    public void testValidateAcceptsSimpleQuery() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        b.validate(); // no throw
    }

    public void testToSearchRequestPropagatesFields() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        b.setSize(25);
        b.setFrom(2);
        b.setMinScore(0.3f);
        SearchRequest legReq = b.toSearchRequest(new String[] { "products" }, null);
        assertArrayEquals(new String[] { "products" }, legReq.indices());
        assertEquals(25, legReq.source().size());
        assertEquals(2, legReq.source().from());
        assertEquals(Float.valueOf(0.3f), legReq.source().minScore());
        // query is the plain leg query when no filter is set
        assertTrue(legReq.source().query() instanceof MatchAllQueryBuilder);
    }

    public void testToSearchRequestWrapsFilterInBool() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        b.setFilterBuilder(new MatchAllQueryBuilder());
        SearchRequest legReq = b.toSearchRequest(new String[] { "products" }, null);
        assertTrue(legReq.source().query() instanceof BoolQueryBuilder);
    }

    public void testToQueryBuilderProjectsResolvedCandidatesToRankDocsQuery() {
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        ShardId sid = new ShardId(new Index("products", "_na_"), 1);
        b.setSearchResult(List.of(new RetrieverCandidate("products", sid, "a", 0.9f, 0)));
        b.doResolve(); // leaf: resolvedResult = searchResult (the executor calls this via resolve(...))
        QueryBuilder q = b.toQueryBuilder();
        assertTrue(q instanceof RankDocsQueryBuilder);
    }

    /**
     * A minimal QueryBuilder whose writeable name is "hybrid" — lets us assert the leaf's hybrid guard by
     * name without depending on the neural-search plugin's class.
     */
    private static final class HybridNamedQueryBuilder extends MatchAllQueryBuilder {
        @Override
        public String getWriteableName() {
            return "hybrid";
        }
    }
}
