/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for all retriever nodes in the retriever tree.
 * <p>
 * A retriever produces a ranked result set. Leaf retrievers retrieve candidates from an index,
 * compound retrievers fuse multiple children, and transformer retrievers reshape a single child.
 * <p>
 * The lifecycle is driven by {@link RetrieverExecutor}:
 * <ol>
 *   <li>{@link #validate(RetrieverContext)} — top-down structural validation</li>
 *   <li>{@link #prepareLeaves(RetrieverContext)} — top-down leaf modification</li>
 *   <li>{@link #collectLeaves()} — gather all leaf nodes for dispatch</li>
 *   <li>{@link #resolveBottomUp()} — after leaf results arrive, resolve this node</li>
 *   <li>{@link #toQueryBuilder()} — produce the final RankDocsQueryBuilder</li>
 * </ol>
 *
 * @opensearch.internal
 */
@ExperimentalApi
public abstract class RetrieverBuilder implements ToXContentObject {

    protected List<RankedDoc> resolvedResult;

    /**
     * Whether this node has already produced its {@link #resolvedResult}. Used by {@link #resolve()}
     * to avoid re-running fusion/reshaping on a node whose inputs haven't changed. An async node
     * clears this flag (via {@link #setAsyncSearchResult}) once its extra-round result arrives, so
     * that it — and only the ancestors above it — recompute on the next pass.
     */
    protected boolean resolved = false;

    /** Global leg response (aggs, total_hits) — stored on the root by the executor. */
    private SearchResponse globalLegResponse;

    /** Global leg shard profiles — stored on the root by the executor when profile=true. */
    private java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> globalLegShardProfiles;

    /**
     * Collect all leaf {@link StandardRetrieverBuilder} nodes in this subtree.
     */
    public abstract List<StandardRetrieverBuilder> collectLeaves();

    /**
     * Resolve this node and its subtree after leaf results are available.
     * <p>
     * This is the idempotent entry point driven by {@link RetrieverExecutor}. It resolves children
     * first (depth-first) and only (re)computes this node — via {@link #doResolve()} — when the node
     * has not yet resolved or when at least one child's output changed on this pass. A node that is
     * already resolved and whose children are unchanged is skipped entirely, so a compound is never
     * re-fused and a transformer never re-reshaped just because some unrelated part of the tree
     * needed another async round.
     *
     * @return {@code true} if this node (re)computed its output on this pass, {@code false} if it was
     *         skipped because it was already resolved with unchanged children
     */
    public boolean resolve() {
        boolean childChanged = false;
        for (RetrieverBuilder child : getChildRetrievers()) {
            childChanged |= child.resolve();
        }
        if (!resolved || childChanged) {
            doResolve();
            resolved = true;
            return true;
        }
        return false;
    }

    /**
     * Backward-compatible entry point that forces a resolution pass over this subtree.
     * Prefer {@link #resolve()} in new code — it reports whether anything changed.
     */
    public final void resolveBottomUp() {
        resolve();
    }

    /**
     * Compute this node's {@link #resolvedResult} from its (already-resolved) children. Called by
     * {@link #resolve()} only when this node is dirty. Leaves assign their dispatched result;
     * compounds fuse; transformers reshape.
     */
    protected abstract void doResolve();

    /**
     * Produce the final query after the tree is fully resolved.
     * Typically returns a {@code RankDocsQueryBuilder} with fused doc IDs and scores.
     */
    public abstract QueryBuilder toQueryBuilder();

    /**
     * Returns the union of all leaf queries for the global leg (aggregations, suggest, etc.).
     */
    public abstract QueryBuilder extractAggregationQuery();

    /**
     * Returns child retrievers for tree traversal (explain, profile).
     */
    public abstract List<RetrieverBuilder> getChildRetrievers();

    /**
     * Upper bound on how many documents this node can contribute to its parent — or, at the root,
     * to the final page. This is the single source of truth for pagination validation: it lets
     * {@code from + size} be checked against the tree regardless of depth or shape.
     * <p>
     * Each node type defines this in terms of its own knobs and its child/children:
     * <ul>
     *   <li>{@link StandardRetrieverBuilder} — its own {@code size} (candidate depth)</li>
     *   <li>{@link CompoundRetrieverBuilder} — its own {@code rank_window_size} (fusion window)</li>
     *   <li>{@link TransformerRetrieverBuilder} — by default, delegates to its child (reshaping
     *       never adds documents beyond what the child produced); subclasses that can shrink the
     *       window (e.g. {@link RescoreRetrieverBuilder}) override this</li>
     * </ul>
     * A new retriever type must implement this the same way it implements {@code fuse()}/{@code reshape()} —
     * there is no central registry that needs updating.
     */
    public abstract int getMaxOutputSize();

    /**
     * Phase 1: Top-down structural validation. Fails fast before any sub-search dispatch.
     *
     * @param context accumulated constraints from ancestor retrievers
     * @throws IllegalArgumentException if validation fails
     */
    public abstract void validate(RetrieverContext context);

    /**
     * Phase 2: Top-down leaf preparation. Propagates modifications to leaves.
     *
     * @param context accumulated modifiers from ancestor retrievers
     */
    public abstract void prepareLeaves(RetrieverContext context);

    /**
     * The name of this retriever type (for explain/profile/error messages).
     */
    public abstract String getName();

    /**
     * Get the resolved result after {@link #resolveBottomUp()} has been called.
     */
    public List<RankedDoc> getResolvedResult() {
        return resolvedResult;
    }

    /**
     * Get the global leg response (stored on root by executor). Contains total_hits and aggregations.
     */
    public SearchResponse getGlobalLegResponse() {
        return globalLegResponse;
    }

    /**
     * Set the global leg response (called by executor on the root node).
     */
    public void setGlobalLegResponse(SearchResponse response) {
        this.globalLegResponse = response;
    }

    /**
     * Get the global leg shard profiles (stored on root by executor, for profile response).
     */
    public java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> getGlobalLegShardProfiles() {
        return globalLegShardProfiles;
    }

    /**
     * Set the global leg shard profiles (called by executor on the root node when profile=true).
     */
    public void setGlobalLegShardProfiles(java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> profiles) {
        this.globalLegShardProfiles = profiles;
    }

    /**
     * Whether this node needs an async action (sub-search dispatch, ML inference, etc.)
     * to complete its resolution. Called by the executor after each resolveBottomUp() pass.
     * <p>
     * If true, the executor will call {@link #buildAsyncSearchRequest} to get the request,
     * dispatch it, then call {@link #setAsyncSearchResult} with the response before
     * re-resolving the tree.
     * <p>
     * Default: false (most nodes resolve synchronously from child results).
     */
    public boolean needsAsyncResolution() {
        return false;
    }

    /**
     * Build the SearchRequest needed to complete async resolution.
     * Only called when {@link #needsAsyncResolution()} returns true.
     *
     * @param indices         target indices for the sub-search
     * @param originalRequest original request for PIT/preference/routing propagation
     * @return a SearchRequest ready for dispatch
     */
    public SearchRequest buildAsyncSearchRequest(String[] indices, SearchRequest originalRequest) {
        throw new UnsupportedOperationException("[" + getName() + "] does not support async resolution");
    }

    /**
     * Receive the async search result and complete this node's resolution.
     * Called by the executor after dispatching the request from {@link #buildAsyncSearchRequest}.
     * After this call, {@link #needsAsyncResolution()} should return false and
     * {@link #getResolvedResult()} should return the final result.
     *
     * @param result ranked documents from the async sub-search response
     */
    public void setAsyncSearchResult(List<RankedDoc> result) {
        throw new UnsupportedOperationException("[" + getName() + "] does not support async resolution");
    }

    // --- Explain and Profile support ---

    /** Timing: wall-clock start of this node's execution (set by executor or resolveBottomUp). */
    protected long profileStartNanos;
    /** Timing: wall-clock end of this node's execution. */
    protected long profileEndNanos;

    /**
     * Mark the start of this node's timed execution.
     */
    public void startTiming() {
        this.profileStartNanos = System.nanoTime();
    }

    /**
     * Mark the end of this node's timed execution.
     */
    public void stopTiming() {
        this.profileEndNanos = System.nanoTime();
    }

    /**
     * Get total elapsed time for this node in nanoseconds.
     */
    public long getElapsedNanos() {
        return profileEndNanos - profileStartNanos;
    }

    /**
     * Get the profile start time (nanoTime) for total_time calculation.
     */
    public long getProfileStartNanos() {
        return profileStartNanos;
    }

    /**
     * Build a tree-structured explanation for a specific document after resolution.
     * Called bottom-up: children's explanations are available via their resolved results.
     * <p>
     * Each node describes its own contribution (fusion formula, reshape logic) and wraps
     * the child/leg explanations for that document.
     *
     * @param docId the document _id to explain
     * @param docIndex the index name of the document
     * @return Explanation for this node's contribution, or null if the doc is not in this node's output
     */
    public abstract Explanation buildExplanation(String docId, String docIndex);

    /**
     * Build profile timing for this node's execution.
     * Called after resolution. Each node reports its own timing breakdown plus child/leg profiles.
     *
     * @return profile data for this node (tree-structured via legs/child)
     */
    public abstract RetrieverProfile buildProfile();

    /**
     * Fallback registry used when the global parser hasn't been initialized yet (e.g. unit tests
     * that build/parse retriever trees without a full {@code SearchModule} startup). Covers only
     * the built-in types — no plugin types, since plugins aren't known outside of node startup.
     */
    private static final RetrieverParser FALLBACK_PARSER = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());

    /**
     * Parse a nested child retriever — used by compound retrievers parsing their {@code "retrievers"}
     * array and transformer retrievers parsing their {@code "retriever"} field.
     * <p>
     * The parser must be positioned at the {@code START_OBJECT} of {@code { "type": {...} } }.
     * Dispatches through the same registry as the top-level {@code "retriever"} field, so any
     * registered type — {@code standard}, any compound/transformer type, or a plugin-registered
     * type — can appear as a child. This is what allows arbitrary nesting (e.g. a compound inside
     * a compound, or a transformer wrapping another transformer) without each retriever type
     * hardcoding a list of the types it's willing to nest.
     *
     * @param parser positioned at the START_OBJECT of the child retriever
     * @return the parsed child RetrieverBuilder
     * @throws IOException on parsing errors
     * @throws IllegalArgumentException if the type name isn't registered
     */
    public static RetrieverBuilder parseInnerRetrieverBuilder(XContentParser parser) throws IOException {
        RetrieverParser registry = SearchSourceBuilderRetrieverIntegration.getGlobalRetrieverParser();
        return (registry != null ? registry : FALLBACK_PARSER).parse(parser);
    }
}
