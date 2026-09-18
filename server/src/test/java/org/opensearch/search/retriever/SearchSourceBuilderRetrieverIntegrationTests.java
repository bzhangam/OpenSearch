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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link SearchSourceBuilderRetrieverIntegration#validateCompatibility} — each blocked
 * top-level field is rejected with a message that states the REASON it is blocked, plus the bare-standard
 * rule, the no-op-when-absent case, and the node-scope cap plumbing.
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
            public void validate() {}

            @Override
            public void prepareLeaves() {}

            @Override
            void doResolve() {
                this.resolvedResult = Collections.emptyList();
            }

            @Override
            void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
                doResolve();
                whenDone.onResponse(null);
            }

            @Override
            public QueryBuilder toQueryBuilder() {
                return new RankDocsQueryBuilder(Collections.emptyList());
            }

            @Override
            public QueryBuilder extractAggregationQuery() {
                return new MatchAllQueryBuilder();
            }

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

    public void testExplicitPitWithRetrieverPitFalseConflicts() {
        // pit + retriever_pit:false is a contradiction → 400.
        SearchSourceBuilder source = sourceWithCompoundRetriever().pointInTimeBuilder(new PointInTimeBuilder("user-pit"))
            .retrieverPit(false);
        assertReason(source, "cannot use an explicit [pit] with [retriever_pit: false]");
    }

    public void testExplicitPitWithRetrieverPitTrueIsLegal() {
        // pit + retriever_pit:true is redundant-but-legal (the user pit wins; framework manages nothing).
        SearchSourceBuilder source = sourceWithCompoundRetriever().pointInTimeBuilder(new PointInTimeBuilder("user-pit"))
            .retrieverPit(true);
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(source); // no throw
    }

    public void testRetrieverPitDefaultAbsentIsLegal() {
        // No retriever_pit → default (framework-managed). With or without an explicit pit, no conflict.
        SearchSourceBuilder withPit = sourceWithCompoundRetriever().pointInTimeBuilder(new PointInTimeBuilder("user-pit"));
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(withPit); // no throw
        SearchSourceBuilder noPit = sourceWithCompoundRetriever();
        SearchSourceBuilderRetrieverIntegration.validateCompatibility(noPit); // no throw
    }

    public void testPitKeepAliveSizing() {
        // Default (no request timeout) → the configured default (30s).
        SearchSourceBuilderRetrieverIntegration.configureLimits(Settings.EMPTY);
        assertEquals(TimeValue.timeValueSeconds(30), SearchSourceBuilderRetrieverIntegration.pitKeepAliveFor(null));
        // A short request timeout → still the default (never shorter than default).
        assertEquals(
            TimeValue.timeValueSeconds(30),
            SearchSourceBuilderRetrieverIntegration.pitKeepAliveFor(TimeValue.timeValueSeconds(5))
        );
        // A long request timeout → timeout + slack (5s).
        assertEquals(
            TimeValue.timeValueSeconds(65),
            SearchSourceBuilderRetrieverIntegration.pitKeepAliveFor(TimeValue.timeValueSeconds(60))
        );
    }

    public void testPitKeepAliveSettingOverride() {
        Settings settings = Settings.builder()
            .put(SearchSourceBuilderRetrieverIntegration.PIT_KEEP_ALIVE_SETTING.getKey(), "45s")
            .build();
        SearchSourceBuilderRetrieverIntegration.configureLimits(settings);
        assertEquals(TimeValue.timeValueSeconds(45), SearchSourceBuilderRetrieverIntegration.getPitKeepAliveSetting());
        assertEquals(TimeValue.timeValueSeconds(45), SearchSourceBuilderRetrieverIntegration.pitKeepAliveFor(null));
        // restore defaults for other tests in the JVM
        SearchSourceBuilderRetrieverIntegration.configureLimits(Settings.EMPTY);
        assertEquals(TimeValue.timeValueSeconds(30), SearchSourceBuilderRetrieverIntegration.getPitKeepAliveSetting());
    }

    private void assertReason(SearchSourceBuilder source, String reasonFragment) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderRetrieverIntegration.validateCompatibility(source)
        );
        assertTrue("expected reason [" + reasonFragment + "] in: " + e.getMessage(), e.getMessage().contains(reasonFragment));
    }
}
