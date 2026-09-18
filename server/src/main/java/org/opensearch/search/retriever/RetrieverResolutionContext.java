/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;

/**
 * The response-side accumulator for one retriever request. Created before dispatch and threaded through
 * the bottom-up cascade, it serves double duty (LLD → Resolution context): the write channel nodes
 * populate <i>during</i> resolution, and the holder consumed <i>once</i> at the end to patch the final
 * response with what the {@code RankDocsQuery} search could not compute itself.
 * <p>
 * <b>A3b/A3c scope.</b> Today the context carries the read flags and the stashed
 * {@link #globalLegResponse} (aggregations / {@code track_total_hits} computed over the union of leaf
 * queries), plus the framework-managed PIT id when one was opened. The per-doc {@code explanations}, the
 * {@code shardProfiles}, the per-node {@code profile} timings, and {@code legExtraInfo} named in the LLD
 * are <b>reserved for A3d</b> (explain / profile) — a new type or A3d adds them here without reworking
 * this accumulator or the executor. Keeping one object now is what lets A3d extend rather than replace.
 * <p>
 * <b>Thread-safety.</b> The only writer today is the executor's global-leg callback (a single write of
 * {@link #globalLegResponse} before the 2-way join fires the outer listener) and the PIT-id set (once,
 * before the cascade). The atomic join in {@link RetrieverExecutor} publishes both before {@link #merge}
 * reads them. When A3d adds per-node writes, those channels must be concurrent, node-keyed structures
 * (see the LLD Thread-safety note).
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public class RetrieverResolutionContext {

    private final boolean trackTotalHitsRequested;

    // Stashed when the separate global-leg search returns (aggregations / track_total_hits over the union).
    private volatile SearchResponse globalLegResponse;

    // The framework-managed PIT id, if the executor opened one (null when user-supplied or opted out).
    private volatile String frameworkManagedPitId;

    /**
     * @param trackTotalHitsRequested whether the user explicitly requested {@code track_total_hits}; only
     *                                then does {@link #merge} overwrite the response total from the global
     *                                leg (aggregations are always merged when present).
     */
    public RetrieverResolutionContext(boolean trackTotalHitsRequested) {
        this.trackTotalHitsRequested = trackTotalHitsRequested;
    }

    /** Stash the global-leg response (aggregations / total over the union). Set once by the executor. */
    public void setGlobalLegResponse(SearchResponse globalLegResponse) {
        this.globalLegResponse = globalLegResponse;
    }

    public SearchResponse getGlobalLegResponse() {
        return globalLegResponse;
    }

    public void setFrameworkManagedPitId(String pitId) {
        this.frameworkManagedPitId = pitId;
    }

    public String getFrameworkManagedPitId() {
        return frameworkManagedPitId;
    }

    public boolean isTrackTotalHitsRequested() {
        return trackTotalHitsRequested;
    }

    /**
     * Patch the final {@code RankDocsQuery} search response with what that search could not compute:
     * the union aggregations (always, when a global leg produced them) and, only when the user requested
     * {@code track_total_hits}, the union total. Hit order, {@code from}/{@code size}, scores, and
     * {@code suggest} are already correct on the final response and are left untouched.
     * <p>
     * Returns the input response unchanged when there is no global-leg response to merge (the common
     * case: no aggs and {@code track_total_hits} disabled — the default for a retriever).
     */
    public SearchResponse merge(SearchResponse finalResponse) {
        if (globalLegResponse == null) {
            return finalResponse;
        }

        SearchHits finalHits = finalResponse.getHits();
        SearchHits mergedHits = finalHits;
        if (trackTotalHitsRequested && globalLegResponse.getHits() != null && globalLegResponse.getHits().getTotalHits() != null) {
            // Overwrite the total with the union match count from the global leg; keep the final search's
            // hit array, maxScore, and per-hit scores (the fused window is the page the user sees).
            TotalHits unionTotal = globalLegResponse.getHits().getTotalHits();
            mergedHits = new SearchHits(finalHits.getHits(), unionTotal, finalHits.getMaxScore());
        }

        Aggregations aggregations = globalLegResponse.getAggregations();
        InternalAggregations internalAggregations = aggregations instanceof InternalAggregations
            ? (InternalAggregations) aggregations
            : null;

        SearchResponseSections sections = new InternalSearchResponse(
            mergedHits,
            internalAggregations,
            finalResponse.getSuggest(),
            finalResponse.getProfileResults() == null ? null : new SearchProfileShardResults(finalResponse.getProfileResults()),
            finalResponse.isTimedOut(),
            finalResponse.isTerminatedEarly(),
            finalResponse.getNumReducePhases()
        );

        return new SearchResponse(
            sections,
            finalResponse.getScrollId(),
            finalResponse.getTotalShards(),
            finalResponse.getSuccessfulShards(),
            finalResponse.getSkippedShards(),
            finalResponse.getTook().millis(),
            finalResponse.getShardFailures(),
            finalResponse.getClusters(),
            finalResponse.pointInTimeId()
        );
    }
}
