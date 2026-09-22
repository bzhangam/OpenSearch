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
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Unit tests for {@link StandardRetrieverBuilder} parsing, the tree contract, and validation. Scope:
 * parse/contract only — the executor dispatch of {@code toSearchRequest} is exercised in the executor/IT tests.
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
        // Unset size resolves to the normal _search default (10); no explicit size was supplied.
        assertNull(b.getExplicitSize());
        assertEquals(StandardRetrieverBuilder.DEFAULT_SIZE, b.getSize());
        assertEquals(10, b.getSize());
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

    public void testToSearchRequestTrimsFetchForRankOnlyLeg() {
        // A leg disables _source (payload loaded by the final fetch) but keeps stored fields — it still
        // needs _id to key candidates.
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        SearchRequest legReq = b.toSearchRequest(new String[] { "products" }, null);
        assertNotNull(legReq.source().fetchSource());
        assertFalse("_source disabled on the leg", legReq.source().fetchSource().fetchSource());
        assertNull("stored fields left default (so _id is still fetched)", legReq.source().storedFields());
    }

    public void testToSearchRequestKeepsSourceDisabledWithCollapse() {
        // _source stays disabled regardless of collapse; collapse fetches its field via stored/docvalue.
        StandardRetrieverBuilder b = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        b.setCollapse(new CollapseBuilder("brand"));
        SearchRequest legReq = b.toSearchRequest(new String[] { "products" }, null);
        assertFalse("_source still disabled", legReq.source().fetchSource().fetchSource());
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

    /** A RetrieverWindowAware test query that records the window handed to it (or -1 if never called). */
    private static final class RetrieverWindowAwareQueryBuilder extends MatchAllQueryBuilder implements RetrieverWindowAware {
        int appliedWindow = Integer.MIN_VALUE;

        @Override
        public void applyRetrieverWindow(int window) {
            this.appliedWindow = window;
        }
    }

    /** A RetrieverWindowAware test query that rejects the window — models knn with an insufficient explicit k. */
    private static final class RejectingWindowAwareQueryBuilder extends MatchAllQueryBuilder implements RetrieverWindowAware {
        @Override
        public void applyRetrieverWindow(int window) {
            throw new IllegalArgumentException("[knn] explicit [k] is smaller than [rank_window_size] " + window);
        }
    }

    public void testPrepareLeavesAppliesWindowToWindowAwareQueryUnderFusion() {
        RetrieverWindowAwareQueryBuilder q = new RetrieverWindowAwareQueryBuilder();
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(q);
        leaf.prepareLeaves(LeafPreparationContext.root().underFusion(50));
        assertEquals("window pushed to the RetrieverWindowAware leg query", 50, q.appliedWindow);
        assertEquals("leg fetch depth inherits the window", 50, leaf.getSize());
    }

    public void testPrepareLeavesPropagatesWindowAwareRejection() {
        // A RetrieverWindowAware query that rejects an insufficient cap (e.g. knn with explicit k < window)
        // must surface its exception through prepareLeaves, before any dispatch.
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new RejectingWindowAwareQueryBuilder());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> leaf.prepareLeaves(LeafPreparationContext.root().underFusion(50))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("smaller than [rank_window_size]"));
    }

    public void testPrepareLeavesDoesNotApplyWindowWhenNotFusionGoverned() {
        RetrieverWindowAwareQueryBuilder q = new RetrieverWindowAwareQueryBuilder();
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(q);
        leaf.prepareLeaves(LeafPreparationContext.root());
        assertEquals("applyRetrieverWindow not called off-fusion", Integer.MIN_VALUE, q.appliedWindow);
        assertEquals("unset, non-fusion leg falls back to default size", StandardRetrieverBuilder.DEFAULT_SIZE, leaf.getSize());
    }

    public void testPrepareLeavesRejectsExplicitSizeUnderFusion() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSize(25);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> leaf.prepareLeaves(LeafPreparationContext.root().underFusion(50))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not support [size] inside a [rank_fusion]"));
    }

    public void testPrepareLeavesAllowsExplicitSizeOffFusion() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSize(25);
        leaf.prepareLeaves(LeafPreparationContext.root()); // no throw
        assertEquals(25, leaf.getSize());
    }
}
