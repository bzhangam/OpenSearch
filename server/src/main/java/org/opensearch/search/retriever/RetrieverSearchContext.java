/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds retriever execution results that must be merged into the final {@link SearchResponse}
 * after the standard search execution completes.
 * <p>
 * The retriever framework resolves during the rewrite phase (before query execution), producing
 * a {@code RankDocsQuery} for the final search. However, the final search only matches the
 * fused top-N docs — it cannot provide correct total_hits or aggregation counts (those require
 * the full match set). The global leg sub-search computes these during retriever resolution,
 * and this context carries them forward for post-response merging.
 * <p>
 * Also supports:
 * <ul>
 *   <li>{@code explain} — per-doc tree-structured explanations from the retriever tree</li>
 *   <li>{@code profile} — retriever-level timing breakdown (dispatch, fusion, per-leg)</li>
 * </ul>
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RetrieverSearchContext {

    private final SearchResponse globalLegResponse;

    /** Per-doc explanations built from the retriever tree (docId|index → Explanation). */
    private Map<String, Explanation> docExplanations;

    /** Retriever profile tree (nullable — only present when profile=true). */
    private RetrieverProfile retrieverProfile;

    /** Global leg shard profiles (nullable — for profile response). */
    private Map<String, org.opensearch.search.profile.ProfileShardResult> globalLegShardProfiles;

    /** Overall retriever execution start time (for total_time). */
    private long executionStartNanos;

    /** Overall retriever execution end time (for total_time). */
    private long executionEndNanos;

    public RetrieverSearchContext(SearchResponse globalLegResponse) {
        this.globalLegResponse = globalLegResponse;
    }

    /**
     * The global leg response containing aggregations and total hit count
     * computed over the union of all leaf queries' match sets.
     * May be null if no global leg was dispatched (no aggs, no track_total_hits).
     */
    public SearchResponse getGlobalLegResponse() {
        return globalLegResponse;
    }

    /**
     * Store per-doc explanations built from the resolved retriever tree.
     * Key format: "docId|indexName"
     */
    public void setDocExplanations(Map<String, Explanation> explanations) {
        this.docExplanations = explanations;
    }

    /**
     * Get per-doc explanations (nullable).
     */
    public Map<String, Explanation> getDocExplanations() {
        return docExplanations;
    }

    /**
     * Store the retriever profile tree.
     */
    public void setRetrieverProfile(RetrieverProfile profile) {
        this.retrieverProfile = profile;
    }

    /**
     * Get the retriever profile (nullable).
     */
    public RetrieverProfile getRetrieverProfile() {
        return retrieverProfile;
    }

    /**
     * Store the global leg's shard profiles.
     */
    public void setGlobalLegShardProfiles(Map<String, org.opensearch.search.profile.ProfileShardResult> profiles) {
        this.globalLegShardProfiles = profiles;
    }

    /**
     * Set execution timing for total_time calculation.
     */
    public void setExecutionTiming(long startNanos, long endNanos) {
        this.executionStartNanos = startNanos;
        this.executionEndNanos = endNanos;
    }

    /**
     * Build a doc key for the explanations map.
     */
    public static String docKey(String docId, String index) {
        return docId + "|" + index;
    }

    /**
     * Merge retriever execution results into the final SearchResponse.
     * <p>
     * Replaces:
     * <ul>
     *   <li>{@code total_hits} — with the global leg's total (full match set count)</li>
     *   <li>{@code aggregations} — with the global leg's aggregation results</li>
     *   <li>{@code _explanation} — per-hit, with tree-structured retriever explanation</li>
     * </ul>
     *
     * @param finalResponse the response from the final search (RankDocsQuery execution)
     * @return a new SearchResponse with corrected data
     */
    public SearchResponse merge(SearchResponse finalResponse) {
        // Overlay per-hit explanations from the retriever tree
        if (docExplanations != null && !docExplanations.isEmpty()) {
            for (SearchHit hit : finalResponse.getHits().getHits()) {
                String key = docKey(hit.getId(), hit.getIndex());
                Explanation retrieverExplanation = docExplanations.get(key);
                if (retrieverExplanation != null) {
                    hit.explanation(retrieverExplanation);
                }
            }
        }

        if (globalLegResponse == null && retrieverProfile == null) {
            return finalResponse;
        }

        // Build merged hits: use final response's hits (correct docs/scores) but the global leg's
        // totalHits — only when the global leg actually tracked totals (it won't for a suggest-only
        // request, where getTotalHits() is null and we must keep the final response's value).
        SearchHits originalHits = finalResponse.getHits();
        SearchHits mergedHits;
        if (globalLegResponse != null && globalLegResponse.getHits().getTotalHits() != null) {
            mergedHits = new SearchHits(
                originalHits.getHits(),
                globalLegResponse.getHits().getTotalHits(),
                originalHits.getMaxScore()
            );
        } else {
            mergedHits = originalHits;
        }

        // Use aggregations from global leg (computed over full match set)
        InternalAggregations mergedAggs;
        if (globalLegResponse != null && globalLegResponse.getAggregations() != null) {
            mergedAggs = (InternalAggregations) globalLegResponse.getAggregations();
        } else {
            mergedAggs = (InternalAggregations) finalResponse.getAggregations();
        }

        // Suggest is computed on the global leg (the final RankDocsQuery search doesn't run it).
        org.opensearch.search.suggest.Suggest mergedSuggest =
            (globalLegResponse != null && globalLegResponse.getSuggest() != null)
                ? globalLegResponse.getSuggest()
                : finalResponse.getSuggest();

        // Profile: Build the full retriever profile when profile data is available.
        // Replaces the meaningless RankDocsQuery-only shard profile with the complete
        // retriever tree profile (per-leg shards, global leg, rank_docs_query, total_time).
        SearchProfileShardResults profileResults = null;
        if (retrieverProfile != null) {
            // RankDocsQuery shard profiles from the final search go into rank_docs_query section
            java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> rankDocsQueryShards =
                finalResponse.getProfileResults() != null && !finalResponse.getProfileResults().isEmpty()
                    ? finalResponse.getProfileResults()
                    : null;

            // Total time: from retriever execution start to now
            long totalTime = executionStartNanos > 0
                ? System.nanoTime() - executionStartNanos
                : retrieverProfile.getTotalTimeNanos();

            final RetrieverProfile.FullRetrieverProfileResult fullProfile = new RetrieverProfile.FullRetrieverProfileResult(
                retrieverProfile,
                globalLegShardProfiles,
                rankDocsQueryShards,
                totalTime
            );
            // Wrap in SearchProfileShardResults with overridden toXContent to serialize our custom format
            profileResults = new SearchProfileShardResults(java.util.Collections.emptyMap()) {
                @Override
                public org.opensearch.core.xcontent.XContentBuilder toXContent(
                    org.opensearch.core.xcontent.XContentBuilder builder,
                    org.opensearch.core.xcontent.ToXContent.Params params
                ) throws java.io.IOException {
                    return fullProfile.toXContent(builder, params);
                }
            };
        }

        InternalSearchResponse mergedInternalResponse = new InternalSearchResponse(
            mergedHits,
            mergedAggs,
            mergedSuggest,
            profileResults,
            finalResponse.isTimedOut(),
            finalResponse.isTerminatedEarly(),
            finalResponse.getNumReducePhases()
        );

        return new SearchResponse(
            mergedInternalResponse,
            finalResponse.getScrollId(),
            finalResponse.getTotalShards(),
            finalResponse.getSuccessfulShards(),
            finalResponse.getSkippedShards(),
            finalResponse.getTook().millis(),
            finalResponse.getPhaseTook(),
            finalResponse.getShardFailures(),
            finalResponse.getClusters(),
            finalResponse.pointInTimeId()
        );
    }
}
