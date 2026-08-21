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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base for compound retrievers that combine multiple child result sets into one
 * (e.g., RRF, score fusion). Handles tree traversal, leaf collection, and bottom-up resolution.
 * Subclasses implement only the fusion logic via {@link #fuse(List)}.
 *
 * @opensearch.internal
 */
public abstract class CompoundRetrieverBuilder extends RetrieverBuilder {

    protected List<RetrieverBuilder> childRetrievers;
    /** Fusion window: how many candidates survive {@link #fuse} before being handed to the parent. */
    protected int rankWindowSize = 100;
    protected Float minScore;

    /**
     * Perform fusion on child results to produce this node's ranked output.
     *
     * @param childResults the resolved results from each child retriever
     * @return the fused ranked documents
     */
    protected abstract List<RankedDoc> fuse(List<List<RankedDoc>> childResults);

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        List<StandardRetrieverBuilder> leaves = new ArrayList<>();
        for (RetrieverBuilder child : childRetrievers) {
            leaves.addAll(child.collectLeaves());
        }
        return leaves;
    }

    @Override
    protected void doResolve() {
        // Children are already resolved by RetrieverBuilder#resolve() before this is called.
        List<List<RankedDoc>> childResults = new ArrayList<>();
        for (RetrieverBuilder child : childRetrievers) {
            childResults.add(child.getResolvedResult());
        }
        this.resolvedResult = fuse(childResults);
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        return new RankDocsQueryBuilder(resolvedResult);
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        BoolQueryBuilder bool = new BoolQueryBuilder();
        for (RetrieverBuilder child : childRetrievers) {
            bool.should(child.extractAggregationQuery());
        }
        return bool;
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return childRetrievers;
    }

    @Override
    public int getMaxOutputSize() {
        return rankWindowSize;
    }

    @Override
    public void validate(RetrieverContext context) {
        if (childRetrievers == null || childRetrievers.size() < 2) {
            throw new IllegalArgumentException("[" + getName() + "] requires at least 2 child retrievers");
        }
        for (RetrieverBuilder child : childRetrievers) {
            child.validate(context);
        }
    }

    @Override
    public void prepareLeaves(RetrieverContext context) {
        for (RetrieverBuilder child : childRetrievers) {
            child.prepareLeaves(context);
        }
    }

    public int getRankWindowSize() {
        return rankWindowSize;
    }

    public void setRankWindowSize(int rankWindowSize) {
        this.rankWindowSize = rankWindowSize;
    }

    public Float getMinScore() {
        return minScore;
    }

    public void setMinScore(Float minScore) {
        this.minScore = minScore;
    }

    // --- Explain and Profile ---

    /** Timing: dispatch wall time (set by executor). */
    protected long dispatchTimeNanos;

    /** Timing: fusion computation time. */
    protected long fusionTimeNanos;

    public void setDispatchTimeNanos(long nanos) {
        this.dispatchTimeNanos = nanos;
    }

    public void setFusionTimeNanos(long nanos) {
        this.fusionTimeNanos = nanos;
    }

    @Override
    public Explanation buildExplanation(String docId, String docIndex) {
        // Delegate to subclass for the fusion-specific explanation
        return buildFusionExplanation(docId, docIndex);
    }

    /**
     * Subclasses override to provide fusion-specific explanation.
     * The base provides the child explanation lookup pattern.
     */
    protected abstract Explanation buildFusionExplanation(String docId, String docIndex);

    /**
     * Find a document's explanation from a specific child retriever.
     * Returns null if the doc is not present in that child's results.
     */
    protected Explanation getChildExplanation(RetrieverBuilder child, String docId, String docIndex) {
        return child.buildExplanation(docId, docIndex);
    }

    @Override
    public RetrieverProfile buildProfile() {
        List<RetrieverProfile> legProfiles = new ArrayList<>();
        for (RetrieverBuilder child : childRetrievers) {
            legProfiles.add(child.buildProfile());
        }
        RetrieverProfile.Builder builder = new RetrieverProfile.Builder(getName())
            .totalTimeNanos(getElapsedNanos())
            .legs(legProfiles);
        if (fusionTimeNanos > 0) {
            builder.addBreakdown("fusion_time_in_nanos", fusionTimeNanos);
        }
        return builder.build();
    }
}
