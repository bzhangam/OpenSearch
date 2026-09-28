/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;

import java.util.Map;

/**
 * The response-side accumulator for one retriever request. Created before dispatch and threaded through
 * the bottom-up cascade, it serves double duty: the write channel nodes
 * populate <i>during</i> resolution, and the holder consumed <i>once</i> at the end to patch the final
 * response with what the {@code RankDocsQuery} search could not compute itself.
 * <p>
 * <b>Scope.</b> Today the context carries the read flags and the stashed
 * {@link #globalLegResponse} (aggregations / {@code track_total_hits} computed over the union of leaf
 * queries), plus the framework-managed PIT id when one was opened. Additional accumulators (per-doc
 * explanations, shard profiles, per-node profile timings, per-leg extra info) can be added here without
 * reworking this accumulator or the executor — keeping one object now lets those extend rather than replace.
 * <p>
 * <b>Thread-safety.</b> The only writer today is the executor's global-leg callback (a single write of
 * {@link #globalLegResponse} before the 2-way join fires the outer listener) and the PIT-id set (once,
 * before the cascade). The atomic join in {@link RetrieverExecutor} publishes both before {@link #merge}
 * reads them. Any future per-node writes would need concurrent, node-keyed structures.
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

    // Coordinator-assembled per-document explanations, keyed by (index,_id). Populated once after tree
    // resolution when explain was requested; null/empty otherwise. Merged onto the final hits.
    private volatile Map<String, Explanation> explanations;

    // Coordinator-assembled retriever profile tree (per-node timing + per-leg shard profiles), set once
    // after tree resolution when profile was requested; null otherwise. Combined with the global-leg and
    // final-search shard profiles in merge() into the response profile section.
    private volatile RetrieverProfile.Node retrieverProfileTree;

    // Wall time (ns) of the whole self_resolve phase (tree resolution + global leg, run in parallel).
    private volatile long selfResolveTimeInNanos;

    // Wall time (ns) of the global-leg search alone (0 when no global leg ran).
    private volatile long globalLegTimeInNanos;

    // System.nanoTime() stamped when self_resolve completes; used in merge() to measure the sequential
    // rank_docs_query phase wall (now - selfResolveEndNanos). 0 when profiling was not requested.
    private volatile long selfResolveEndNanos;

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

    /**
     * Set the coordinator-assembled explanations, keyed by {@code index + '\u0000' + _id}. Called once by
     * the executor after tree resolution when {@code explain: true} was requested.
     */
    public void setExplanations(Map<String, Explanation> explanations) {
        this.explanations = explanations;
    }

    /**
     * Set the coordinator-assembled retriever profile tree and the self_resolve phase timings. Called once
     * by the executor when self_resolve completes (tree + global leg both done) and {@code profile: true}
     * was requested.
     *
     * @param retrieverProfileTree    the resolved retriever node tree
     * @param selfResolveTimeInNanos  wall time (ns) of the whole self_resolve phase (tree || global leg)
     * @param globalLegTimeInNanos    wall time (ns) of the global-leg search alone (0 when none ran)
     * @param selfResolveEndNanos     {@code System.nanoTime()} at self_resolve completion; the rank_docs_query
     *                                phase wall is measured from here in {@link #merge}
     */
    public void setRetrieverProfile(
        RetrieverProfile.Node retrieverProfileTree,
        long selfResolveTimeInNanos,
        long globalLegTimeInNanos,
        long selfResolveEndNanos
    ) {
        this.retrieverProfileTree = retrieverProfileTree;
        this.selfResolveTimeInNanos = selfResolveTimeInNanos;
        this.globalLegTimeInNanos = globalLegTimeInNanos;
        this.selfResolveEndNanos = selfResolveEndNanos;
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
        // Patch per-hit _explanation first (independent of the global leg). When explain was requested the
        // coordinator-assembled tree replaces whatever the final RankDocsQuery search produced.
        SearchResponse withExplanations = applyExplanations(finalResponse);

        if (globalLegResponse == null) {
            return applyRetrieverProfile(withExplanations, finalResponse);
        }

        SearchHits finalHits = withExplanations.getHits();
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
            withExplanations.getSuggest(),
            withExplanations.getProfileResults() == null ? null : new SearchProfileShardResults(withExplanations.getProfileResults()),
            withExplanations.isTimedOut(),
            withExplanations.isTerminatedEarly(),
            withExplanations.getNumReducePhases()
        );
        attachRetrieverProfile(sections, finalResponse);

        return new SearchResponse(
            sections,
            withExplanations.getScrollId(),
            withExplanations.getTotalShards(),
            withExplanations.getSuccessfulShards(),
            withExplanations.getSkippedShards(),
            withExplanations.getTook().millis(),
            withExplanations.getShardFailures(),
            withExplanations.getClusters(),
            withExplanations.pointInTimeId()
        );
    }

    /**
     * Build the {@link RetrieverProfile} (retriever tree + global-leg + rank_docs_query shard profiles +
     * total time) and attach it to the sections so it renders under the {@code profile} key. No-op when
     * profiling was not requested. The {@code rank_docs_query} profiles come from the final search's own
     * profile results.
     */
    private void attachRetrieverProfile(SearchResponseSections sections, SearchResponse finalResponse) {
        if (retrieverProfileTree == null) {
            return;
        }
        // The rank_docs_query phase runs sequentially after self_resolve; its wall is the time from
        // self_resolve completion to now (this merge runs on the final response listener). Guard against a
        // missing stamp (0) so we never report a bogus multi-year duration from epoch-relative nanoTime.
        long rankDocsQueryTimeInNanos = selfResolveEndNanos > 0 ? Math.max(0L, System.nanoTime() - selfResolveEndNanos) : 0L;
        RetrieverProfile profile = RetrieverProfile.builder()
            .retriever(retrieverProfileTree)
            .selfResolveTimeInNanos(selfResolveTimeInNanos)
            .globalLegProfile(globalLegResponse == null ? null : globalLegResponse.getProfileResults())
            .globalLegTimeInNanos(globalLegTimeInNanos)
            .rankDocsQueryProfile(finalResponse.getProfileResults())
            .rankDocsQueryTimeInNanos(rankDocsQueryTimeInNanos)
            .build();
        sections.setRetrieverProfile(profile);
    }

    /**
     * Rebuild {@code response} with the retriever profile attached to its sections (used on the no-global-leg
     * path). Returns {@code response} unchanged when profiling was not requested.
     */
    private SearchResponse applyRetrieverProfile(SearchResponse response, SearchResponse finalResponse) {
        if (retrieverProfileTree == null) {
            return response;
        }
        SearchHits hits = response.getHits();
        SearchResponseSections sections = new InternalSearchResponse(
            hits,
            response.getAggregations() instanceof InternalAggregations ? (InternalAggregations) response.getAggregations() : null,
            response.getSuggest(),
            response.getProfileResults() == null ? null : new SearchProfileShardResults(response.getProfileResults()),
            response.isTimedOut(),
            response.isTerminatedEarly(),
            response.getNumReducePhases()
        );
        attachRetrieverProfile(sections, finalResponse);
        return new SearchResponse(
            sections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }

    /**
     * Replace each hit's {@code _explanation} with the coordinator-assembled tree keyed by
     * {@code (index,_id)}. Returns the response unchanged when no explanations were assembled (explain not
     * requested) or there are no hits. A hit with no assembled explanation is left untouched.
     */
    private SearchResponse applyExplanations(SearchResponse response) {
        Map<String, Explanation> assembled = explanations;
        if (assembled == null || assembled.isEmpty()) {
            return response;
        }
        SearchHits hits = response.getHits();
        if (hits == null || hits.getHits() == null || hits.getHits().length == 0) {
            return response;
        }

        SearchHit[] source = hits.getHits();
        SearchHit[] patched = new SearchHit[source.length];
        for (int i = 0; i < source.length; i++) {
            SearchHit hit = source[i];
            Explanation explanation = assembled.get(hit.getIndex() + "\u0000" + hit.getId());
            if (explanation != null) {
                hit.explanation(explanation);
            }
            patched[i] = hit;
        }
        SearchHits mergedHits = new SearchHits(patched, hits.getTotalHits(), hits.getMaxScore());

        SearchResponseSections sections = new InternalSearchResponse(
            mergedHits,
            response.getAggregations() instanceof InternalAggregations ? (InternalAggregations) response.getAggregations() : null,
            response.getSuggest(),
            response.getProfileResults() == null ? null : new SearchProfileShardResults(response.getProfileResults()),
            response.isTimedOut(),
            response.isTerminatedEarly(),
            response.getNumReducePhases()
        );
        return new SearchResponse(
            sections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }
}
