/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link SearchSourceBuilderRetrieverIntegration#validateCompatibility} — each blocked
 * top-level field is rejected with a message that states the REASON it is blocked, plus the bare-standard
 * rule, the from+size window rule, the no-op-when-absent case, and the node-scope cap plumbing.
 */
public class SearchSourceBuilderRetrieverIntegrationTests extends OpenSearchTestCase {

    /** A minimal non-leaf retriever so we can test blocked combos with a non-bare (valid-shape) root. */
    private static RetrieverBuilder compoundRoot() {
        return new RetrieverBuilder() {
            @Override
            public List<StandardRetrieverBuilder> collectLeaves() {
                return Collections.emptyList();
            }

            @Override
            public List<RetrieverBuilder> getChildRetrievers() {
                return Collections.emptyList();
            }

            @Override
            public int getMaxOutputSize() {
                return 100;
            }

            @Override
            public void validate() {}

            @Override
            public void prepareLeaves() {}

            @Override
            public String getName() {
                return "test_compound";
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().endObject();
            }
        };
    }

    private static SearchSourceBuilder sourceWithCompoundRetriever() {
        return new SearchSourceBuilder().retriever(compoundRoot());
    }

    public void testNoRetrieverIsNoOp() {
        // No retriever set → validateCompatibility returns cleanly even with an otherwise-"blocked" field.
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source); // no throw
    }

    public void testTopLevelStandardAccepted() {
        // A top-level `standard` retriever is a valid entry point — validateCompatibility must NOT reject it.
        SearchSourceBuilder source = new SearchSourceBuilder().retriever(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source); // no throw
    }

    public void testQueryBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever().query(new MatchAllQueryBuilder());
        assertReason(source, "cannot use [retriever] and [query] together");
    }

    public void testRescoreBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever().addRescorer(new QueryRescorerBuilder(new MatchAllQueryBuilder()));
        assertReason(source, "cannot use [retriever] and [rescore] together");
    }

    public void testSearchAfterBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever().searchAfter(new Object[] { "a" });
        assertReason(source, "search_after");
    }

    public void testTerminateAfterBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever().terminateAfter(10);
        assertReason(source, "permanent design choice");
    }

    public void testSliceBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever().slice(new SliceBuilder(0, 2));
        assertReason(source, "scroll slicing is incompatible");
    }

    public void testFromSizeWindowExceededRejected() {
        // compoundRoot window = 100; from + size = 90 + 20 = 110 > 100 → reject.
        SearchSourceBuilder source = sourceWithCompoundRetriever().from(90).size(20);
        assertReason(source, "exceeds the retriever's");
    }

    public void testFromSizeWindowBoundaryAccepted() {
        // from + size == 100 (the window) → accepted.
        SearchSourceBuilder source = sourceWithCompoundRetriever().from(80).size(20);
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source); // no throw
    }

    public void testNamedSearchPipelineBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever().pipeline("my_pipeline");
        assertReason(source, "cannot use [retriever] and [search_pipeline] together");
    }

    public void testInlineSearchPipelineBlocked() {
        SearchSourceBuilder source = sourceWithCompoundRetriever();
        source.searchPipelineSource(Collections.singletonMap("request_processors", Collections.emptyList()));
        assertReason(source, "retriever-compatible");
    }

    public void testConfigureLimitsReadsSettings() {
        Settings settings = Settings.builder()
            .put(SearchSourceBuilderRetrieverIntegration.MAX_LEAF_COUNT_SETTING.getKey(), 9)
            .put(SearchSourceBuilderRetrieverIntegration.MAX_DEPTH_SETTING.getKey(), 7)
            .build();
        SearchSourceBuilderRetrieverIntegration.configureLimits(settings);
        assertEquals(9, SearchSourceBuilderRetrieverIntegration.getMaxLeafCount());
        assertEquals(7, SearchSourceBuilderRetrieverIntegration.getMaxDepth());
        // restore defaults for other tests in the JVM
        SearchSourceBuilderRetrieverIntegration.configureLimits(Settings.EMPTY);
        assertEquals(5, SearchSourceBuilderRetrieverIntegration.getMaxLeafCount());
        assertEquals(5, SearchSourceBuilderRetrieverIntegration.getMaxDepth());
    }

    private void assertReason(SearchSourceBuilder source, String reasonFragment) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue("expected reason [" + reasonFragment + "] in: " + e.getMessage(), e.getMessage().contains(reasonFragment));
    }
}
