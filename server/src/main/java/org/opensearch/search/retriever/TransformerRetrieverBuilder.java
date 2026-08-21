/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.index.query.QueryBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Abstract base for transformer retrievers that reshape a single child's result set
 * (e.g., rescore, MMR, pinned). Handles tree traversal and bottom-up resolution.
 * Subclasses implement only the reshaping logic via {@link #reshape(List)}.
 *
 * @opensearch.internal
 */
public abstract class TransformerRetrieverBuilder extends RetrieverBuilder {

    protected RetrieverBuilder childRetriever;

    /**
     * Reshape the child's results to produce this node's output.
     *
     * @param childResult the resolved results from the child retriever
     * @return the reshaped ranked documents
     */
    protected abstract List<RankedDoc> reshape(List<RankedDoc> childResult);

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        return childRetriever.collectLeaves();
    }

    @Override
    protected void doResolve() {
        // The child is already resolved by RetrieverBuilder#resolve() before this is called.
        this.resolvedResult = reshape(childRetriever.getResolvedResult());
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        return new RankDocsQueryBuilder(resolvedResult);
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        return childRetriever.extractAggregationQuery();
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return Collections.singletonList(childRetriever);
    }

    /**
     * Default: reshaping never adds documents beyond what the child produced, so the ceiling is
     * the child's. Subclasses that can shrink the window (e.g. {@link RescoreRetrieverBuilder},
     * whose {@code window_size} may be smaller than the child's output) must override this.
     */
    @Override
    public int getMaxOutputSize() {
        return childRetriever.getMaxOutputSize();
    }

    @Override
    public void validate(RetrieverContext context) {
        if (childRetriever == null) {
            throw new IllegalArgumentException("[" + getName() + "] requires a child retriever");
        }
        childRetriever.validate(context);
    }

    @Override
    public void prepareLeaves(RetrieverContext context) {
        childRetriever.prepareLeaves(context);
    }

    public RetrieverBuilder getChildRetriever() {
        return childRetriever;
    }

    public void setChildRetriever(RetrieverBuilder childRetriever) {
        this.childRetriever = childRetriever;
    }

    // --- Explain and Profile ---

    /**
     * Subclasses override to explain how they reshaped the doc's score/position.
     *
     * @param docId the document _id
     * @param docIndex the document's index name
     * @param childExplanation the child retriever's explanation for this doc (may be null)
     * @return the reshape explanation wrapping the child's
     */
    protected abstract Explanation buildReshapeExplanation(String docId, String docIndex, Explanation childExplanation);

    @Override
    public Explanation buildExplanation(String docId, String docIndex) {
        Explanation childExplain = childRetriever.buildExplanation(docId, docIndex);
        return buildReshapeExplanation(docId, docIndex, childExplain);
    }

    @Override
    public RetrieverProfile buildProfile() {
        RetrieverProfile childProfile = childRetriever.buildProfile();
        long reshapeTime = getElapsedNanos() - childRetriever.getElapsedNanos();
        RetrieverProfile.Builder builder = new RetrieverProfile.Builder(getName())
            .totalTimeNanos(getElapsedNanos())
            .child(childProfile);
        if (reshapeTime > 0) {
            builder.addBreakdown("reshape_time_in_nanos", reshapeTime);
        }
        return builder.build();
    }
}
