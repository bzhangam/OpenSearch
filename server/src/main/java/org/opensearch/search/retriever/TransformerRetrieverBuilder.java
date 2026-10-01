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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base for <b>single-child reranker</b> retrievers — the sibling of {@link CompoundRetrieverBuilder}.
 * <p>
 * Where a {@link CompoundRetrieverBuilder} fuses <i>two or more</i> children into one ranking, a transformer
 * wraps <b>exactly one</b> child and <i>reshapes</i> that child's ranking: it re-orders and/or re-scores the
 * child's resolved window without changing which underlying query produced it. Pinning ({@link
 * PinRetrieverBuilder}) is the first such type; rescore, MMR/diversify, and late-interaction reranking are
 * natural future subclasses. Because they all share the same skeleton — resolve the one child, then reshape
 * its window — the plumbing lives here once and each subclass implements only its reshape and explanation.
 * <p>
 * <b>Top-level only.</b> A reranker must not sit inside a fusion subtree: a fusion node governs its legs'
 * window so the fused result is complete and stable, and injecting a reranker into that contract would
 * change a leg's membership in a way the fusion cannot account for. {@link #prepareLeaves} enforces this
 * for every subclass.
 * <p>
 * <b>Resolution.</b> {@link #resolve} resolves the child, then invokes the {@link #afterChildResolved} hook
 * (a seam for a subclass that needs an extra bounded async step over the resolved child window — e.g. pin's
 * id lookup for always-pin), then computes the reshape inline via {@code reshape(...)} (which the base calls
 * from {@link #doResolve()}). Node wall time and the child wall time are recorded for the profile.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public abstract class TransformerRetrieverBuilder extends RetrieverBuilder {

    static final String RETRIEVER_FIELD = "retriever";

    /** Breakdown key for the reranker's own coordinator time beyond its single child's resolution. */
    static final String ORCHESTRATION_OVERHEAD = "orchestration_overhead";

    protected final RetrieverBuilder retriever;

    // Profiling: child subtree wall time and this node's own wall time (set only when profile==true).
    protected boolean explain;
    protected boolean profile;
    private long childElapsedNanos;

    // Fusion window inherited from an enclosing fusion, captured during prepareLeaves when this reranker
    // opts in via preservesFusionWindow(); NO_WINDOW when top-level (not fusion-governed). A window-preserving
    // reranker must emit exactly this many candidates so the fused window stays complete and stable.
    protected int effectiveWindow = LeafPreparationContext.NO_WINDOW;

    protected TransformerRetrieverBuilder(RetrieverBuilder retriever) {
        this.retriever = retriever;
    }

    public RetrieverBuilder getRetriever() {
        return retriever;
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return retriever == null ? Collections.emptyList() : Collections.singletonList(retriever);
    }

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        return retriever == null ? Collections.emptyList() : retriever.collectLeaves();
    }

    @Override
    public void validate() {
        if (retriever == null) {
            throw new IllegalArgumentException("[" + getName() + "] requires a [" + RETRIEVER_FIELD + "] child retriever");
        }
        validateTransformer();
        retriever.validate();
    }

    /** Subclass-specific validation (e.g. pin's non-empty pin list). Called after the child null-check. */
    protected void validateTransformer() {}

    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        this.explain = context.isExplain();
        this.profile = context.isProfile();
        if (context.isFusionGoverned()) {
            if (preservesFusionWindow() == false) {
                throw new IllegalArgumentException(
                    "[" + getName() + "] retriever is only allowed at the top level, not inside a [rank_fusion]/[score_fusion] retriever"
                );
            }
            // Window-preserving reranker under fusion: it reorders/selects within the leg's window but emits the
            // same candidate count the fusion node demands, so the fused window stays complete and reproducible.
            // Capture that window as this node's output size and propagate the fusion context UNCHANGED to the
            // child, so the child leg still fetches exactly rank_window_size candidates.
            this.effectiveWindow = context.getInheritedWindow();
            retriever.prepareLeaves(context);
            return;
        }
        // Top level: a reranker does not impose a window on its child; propagate the root flags unchanged so the
        // child keeps its own sizing and the explain/profile flags flow down. Output size is the request size.
        this.effectiveWindow = LeafPreparationContext.NO_WINDOW;
        retriever.prepareLeaves(LeafPreparationContext.root(context.isExplain(), context.isProfile()));
    }

    /**
     * Whether this reranker may sit inside a {@code rank_fusion}/{@code score_fusion} subtree. Default
     * {@code false}: a reranker is top-level only, because shrinking a leg's candidate set would break the
     * fusion-window contract (every leg must contribute exactly {@code rank_window_size} candidates so the
     * fused window is complete and reproducible — see {@link LeafPreparationContext}).
     * <p>
     * A subclass that <b>preserves the candidate count</b> — reordering/selecting within the inherited window
     * but emitting the same number of candidates (e.g. {@code diversify} diversify-and-reorder of the whole
     * window) — overrides this to return {@code true} <i>when it is fusion-governed</i>. In that mode
     * {@link #prepareLeaves} does not reject the node, records {@link #effectiveWindow} = the inherited window,
     * and propagates the fusion context unchanged to the child so the leg still fetches the full window.
     */
    protected boolean preservesFusionWindow() {
        return false;
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        // Aggregations / track_total_hits are computed over what the child matched; a reranker only reorders
        // that set, so by default it delegates to the child. A subclass that injects out-of-set documents
        // (e.g. always-pin) still delegates here so injected docs do not inflate the union count.
        return retriever.extractAggregationQuery();
    }

    @Override
    void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
        final long startNanos = profile ? System.nanoTime() : 0L;
        retriever.resolve(client, indices, original, ActionListener.wrap(v -> {
            if (profile) {
                this.childElapsedNanos = System.nanoTime() - startNanos;
            }
            // Optional subclass async step over the resolved child window (default: no-op), then reshape.
            afterChildResolved(client, indices, original, ActionListener.wrap(unused -> {
                doResolve();
                if (profile) {
                    this.nodeElapsedNanos = System.nanoTime() - startNanos;
                }
                whenDone.onResponse(null);
            }, whenDone::onFailure));
        }, whenDone::onFailure));
    }

    /**
     * Hook for a subclass that needs a bounded async step <i>after</i> the child is resolved but <i>before</i>
     * the reshape — e.g. pin's {@code ids} sub-search to locate always-pinned docs missing from the child
     * window. Default: no extra work, fire {@code whenReady} immediately. Implementations must invoke
     * {@code whenReady} exactly once.
     */
    protected void afterChildResolved(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenReady) {
        whenReady.onResponse(null);
    }

    @Override
    void doResolve() {
        this.resolvedResult = reshape(childWindow());
    }

    /**
     * Reshape the child's resolved window into this node's ranking (re-order and/or re-score). Pure
     * in-memory, no I/O — runs inline on a resolution callback thread. The returned list becomes this
     * node's {@code resolvedResult}.
     *
     * @param childWindow the child's resolved candidates (never null; empty if the child produced none)
     */
    protected abstract List<RetrieverCandidate> reshape(List<RetrieverCandidate> childWindow);

    /** The child's resolved window, or an empty list if the child produced none. */
    protected final List<RetrieverCandidate> childWindow() {
        List<RetrieverCandidate> window = retriever.getResolvedResult();
        return window == null ? List.of() : window;
    }

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
    public RetrieverProfile.Node buildProfile() {
        // A reranker reports its wall time and a breakdown that reconciles to it:
        // total = orchestration_overhead + child total
        // The single child runs sequentially before the reshape, so there is no parallel max; the reshape's
        // own compute is negligible and folded into orchestration_overhead (nodeElapsed - child), emitted
        // only when positive.
        RetrieverProfile.Node childProfile = retriever.buildProfile();
        long childNanos = childProfile.getTotalTimeInNanos();
        Map<String, Long> breakdown = new LinkedHashMap<>();
        long orchestration = nodeElapsedNanos - childNanos;
        if (orchestration > 0) {
            breakdown.put(ORCHESTRATION_OVERHEAD, orchestration);
        }
        return RetrieverProfile.inner(getName(), nodeElapsedNanos, breakdown, List.of(childProfile));
    }

    /** Wall time (ns) of the child subtree's resolution; available to subclasses for their own profiling. */
    protected long childElapsedNanos() {
        return childElapsedNanos;
    }

    // ---- Shared XContent helpers for the single {@code retriever} child ----

    /**
     * Emit the {@code "retriever": { "<type>": {...} }} child field. A subclass writes its own type-specific
     * fields, then calls this to render the child. Must be called inside the subclass's {@code startObject}.
     */
    protected void writeChildRetriever(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD);
        builder.startObject();
        retriever.toXContent(builder, params);
        builder.endObject();
    }
}
