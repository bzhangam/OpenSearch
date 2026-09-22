/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for <b>compound</b> retrievers — nodes that fuse the rankings of two or more child
 * retrievers into a single ranking (e.g. {@link RankFusionRetrieverBuilder}). It carries the shared
 * tree-node machinery so a concrete compound only implements its fusion math ({@link #fuse}) and its
 * parse/render/name.
 * <p>
 * <b>Resolution.</b> A compound resolves by fanning out to all children (via the shared
 * {@link #resolveChildren}) and, once every child is resolved, computing its own {@link #resolvedResult}
 * inline in {@link #doResolve()} by calling {@link #fuse(List)} with the children's resolved candidate
 * lists in child order. It is pure in-memory work on a resolution-callback thread — no I/O, no blocking.
 * <p>
 * <b>Final query.</b> {@link #toQueryBuilder()} projects the fused candidate window to {@link RankDoc}s
 * and wraps them in a {@link RankDocsQueryBuilder}, exactly like a leaf — the difference is only how the
 * window was produced (fusion vs a single leg).
 * <p>
 * <b>Aggregation query.</b> {@link #extractAggregationQuery()} returns a {@code bool.should} union of the
 * children's aggregation queries, so the global leg (aggs / {@code track_total_hits}) counts over
 * everything any child leg matched.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public abstract class CompoundRetrieverBuilder extends RetrieverBuilder {

    /** Minimum children a compound must have — fusing fewer than two rankings is meaningless. */
    public static final int MIN_CHILDREN = 2;

    protected final List<RetrieverBuilder> children;

    protected CompoundRetrieverBuilder(List<RetrieverBuilder> children) {
        this.children = children == null ? new ArrayList<>() : new ArrayList<>(children);
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return children;
    }

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        List<StandardRetrieverBuilder> leaves = new ArrayList<>();
        for (RetrieverBuilder child : children) {
            leaves.addAll(child.collectLeaves());
        }
        return leaves;
    }

    @Override
    public void validate() {
        if (children.size() < MIN_CHILDREN) {
            throw new IllegalArgumentException(
                "[" + getName() + "] requires at least " + MIN_CHILDREN + " child retrievers in [retrievers], but got " + children.size()
            );
        }
        for (RetrieverBuilder child : children) {
            child.validate();
        }
    }

    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        // Default compound behavior: propagate the ancestor context unchanged. A fusion compound that
        // governs its own window (e.g. RankFusionRetrieverBuilder) overrides this to derive underFusion(...).
        for (RetrieverBuilder child : children) {
            child.prepareLeaves(context);
        }
    }

    @Override
    void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
        // Fan out to children; once all are resolved, fuse them inline on the last child's callback thread.
        resolveChildren(client, indices, original, ActionListener.wrap(v -> {
            doResolve();
            whenDone.onResponse(null);
        }, whenDone::onFailure));
    }

    @Override
    void doResolve() {
        List<List<RetrieverCandidate>> childResults = new ArrayList<>(children.size());
        for (RetrieverBuilder child : children) {
            List<RetrieverCandidate> childResult = child.getResolvedResult();
            childResults.add(childResult == null ? List.of() : childResult);
        }
        this.resolvedResult = fuse(childResults);
    }

    /**
     * Fuse the children's resolved candidate lists (in child order) into this node's ranking. Pure
     * function of its inputs — deterministic, no I/O. Implemented by each concrete compound (e.g. RRF for
     * {@link RankFusionRetrieverBuilder}).
     *
     * @param childResults each child's resolved candidates, in child order
     * @return the fused, ordered candidate list (already truncated to this compound's window)
     */
    protected abstract List<RetrieverCandidate> fuse(List<List<RetrieverCandidate>> childResults);

    @Override
    public QueryBuilder toQueryBuilder() {
        List<RankDoc> window = new ArrayList<>(resolvedResult == null ? 0 : resolvedResult.size());
        if (resolvedResult != null) {
            for (RetrieverCandidate candidate : resolvedResult) {
                window.add(candidate.toRankDoc());
            }
        }
        return new RankDocsQueryBuilder(window);
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        // Union of the children's aggregation queries: aggs/total count over everything any leg matched.
        BoolQueryBuilder union = new BoolQueryBuilder();
        for (RetrieverBuilder child : children) {
            union.should(child.extractAggregationQuery());
        }
        return union;
    }
}
