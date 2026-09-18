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
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract base class for all retriever nodes in the retriever tree.
 * <p>
 * A retriever produces a ranked result set. Leaf retrievers ({@link StandardRetrieverBuilder}) retrieve
 * candidates from an index; compound retrievers fuse multiple children; transformer retrievers reshape a
 * single child. This A3a base carries the parse/validate/tree-structure contract only — the bottom-up
 * resolution machinery (the executor, the {@code RetrieverCandidate} currency, {@code toQueryBuilder},
 * async resolution, and {@code explain}/{@code profile}) is added by later sub-features (A3b/A3d) and is
 * intentionally absent here, so the skeleton can be parsed and validated before any execution exists.
 * <p>
 * The A3a lifecycle a node participates in:
 * <ol>
 *   <li>parse (via the registry / {@link #parseInnerRetrieverBuilder} for nested children)</li>
 *   <li>{@link #validate()} — top-down structural validation</li>
 *   <li>{@link #prepareLeaves()} — top-down leaf preparation</li>
 *   <li>{@link #collectLeaves()} — gather all leaf nodes</li>
 * </ol>
 * <p>
 * Note: the top-down ancestor→leaf constraint/modifier mechanism (a {@code RetrieverContext} carrying
 * {@code LeafConstraint}/{@code LeafModifier}) is intentionally NOT introduced here — A3a has no
 * compound/transformer types to produce constraints/modifiers, so it would be an abstraction with no
 * consumer. It is deferred to the first type that actually needs it (Workstream B), designed against
 * that concrete consumer rather than guessed now.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public abstract class RetrieverBuilder implements ToXContentObject {

    /**
     * Collect all leaf {@link StandardRetrieverBuilder} nodes in this subtree.
     */
    public abstract List<StandardRetrieverBuilder> collectLeaves();

    /**
     * Returns child retrievers for tree traversal (validation, leaf collection, depth checks).
     */
    public abstract List<RetrieverBuilder> getChildRetrievers();

    /**
     * Top-down structural validation. Fails fast before any sub-search dispatch.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public abstract void validate();

    /**
     * Top-down leaf preparation. A hook for a node to prepare its subtree before execution. No-op for a
     * lone leaf in A3a; compound/transformer types (Workstream B) use it to propagate preparation to
     * their leaves.
     */
    public abstract void prepareLeaves();

    /**
     * The name of this retriever type (for error messages / debugging).
     */
    public abstract String getName();

    // --- Bottom-up resolution: per-node, listener-based ---
    //
    // Each node resolves its own subtree and invokes a `whenDone` listener exactly once when it (and
    // everything under it) is resolved. Completion MAY be synchronous (in-memory nodes, or a subtree with
    // no pending I/O — whenDone fires on the calling thread) or asynchronous (a leaf's sub-search — whenDone
    // fires later on a search-response thread). Callers must not assume completion timing.
    //
    // Dispatch model:
    // - a LEAF ({@link StandardRetrieverBuilder}) dispatches its own {@code client.search} (the only I/O),
    // - a COMPOUND/TRANSFORMER fans out to its children via {@link #resolveChildren} and, once they are all
    // resolved, computes its own output via {@link #doResolve()} INLINE (in-memory work is never
    // dispatched to a threadpool — it runs on whichever thread completed the last child).
    // The global leg (aggs/track_total_hits) is dispatched independently by the executor and never
    // participates in this chain, so it cannot block tree resolution.

    /** This node's resolved ranked output, available once its {@code whenDone} has fired. Package-internal. */
    List<RetrieverCandidate> resolvedResult;

    /**
     * Resolve this node and its subtree, invoking {@code whenDone} exactly once on completion.
     * <p>
     * <b>Completion may be synchronous or asynchronous</b> (see the class-level dispatch note); do no
     * blocking work in the listener. A leaf dispatches a search; a compound/transformer resolves its
     * children then computes {@link #doResolve()} inline.
     *
     * @param client   used by leaves to dispatch their sub-search
     * @param indices  target indices for leaf sub-searches
     * @param original the original search request (for PIT / preference / routing / indices_boost propagation)
     * @param whenDone invoked once: {@code onResponse(null)} when this subtree is resolved, or
     *                 {@code onFailure} on the first error in the subtree
     */
    abstract void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone);

    /**
     * Fan out resolution to all children and invoke {@code onAllResolved} once every child has resolved
     * (or {@code onFailure} on the first child failure). Shared by every compound/transformer node so the
     * completion-latch + first-failure logic lives in exactly one audited place.
     * <p>
     * Threading: each child may complete on a different thread; a per-call {@link CountDown} decides which
     * child callback is the last, and only that one invokes {@code onAllResolved} — inline, on its thread.
     * The atomic count-down provides the happens-before edge, so {@code onAllResolved} sees every child's
     * resolved result. First failure wins; late sibling results are ignored.
     */
    protected final void resolveChildren(Client client, String[] indices, SearchRequest original, ActionListener<Void> onAllResolved) {
        List<RetrieverBuilder> children = getChildRetrievers();
        if (children.isEmpty()) {
            onAllResolved.onResponse(null);
            return;
        }
        final CountDown remaining = new CountDown(children.size());
        final AtomicReference<Exception> firstFailure = new AtomicReference<>();
        for (RetrieverBuilder child : children) {
            child.resolve(client, indices, original, ActionListener.wrap(v -> {
                if (remaining.countDown()) {
                    Exception failure = firstFailure.get();
                    if (failure != null) {
                        onAllResolved.onFailure(failure);
                    } else {
                        onAllResolved.onResponse(null);
                    }
                }
            }, e -> {
                firstFailure.compareAndSet(null, e);
                if (remaining.countDown()) {
                    onAllResolved.onFailure(firstFailure.get());
                }
            }));
        }
    }

    /**
     * Compute this node's {@link #resolvedResult} from its (already-resolved) children. A leaf assigns its
     * dispatched result; a compound fuses its children; a transformer reshapes its child. Pure in-memory:
     * no I/O, no blocking — it runs inline on a resolution callback thread.
     */
    abstract void doResolve();

    /** This node's resolved ranked output (after {@code whenDone} fired). Package-internal. */
    List<RetrieverCandidate> getResolvedResult() {
        return resolvedResult;
    }

    /**
     * Produce the final query after the tree is fully resolved — typically a {@code RankDocsQuery} built
     * from the resolved candidate window (projected to {@link RankDoc}s). Called on the root by the
     * executor / the {@code SearchSourceBuilder} rewrite wiring.
     */
    public abstract QueryBuilder toQueryBuilder();

    /**
     * The query used for the global leg (aggregations / {@code track_total_hits}) — the union of all leaf
     * queries in this subtree, so aggregations are computed over everything any leg matched, not just the
     * final top-N. A leaf returns its own leg query; a compound returns a {@code bool.should} union of its
     * children's.
     */
    public abstract QueryBuilder extractAggregationQuery();

    /**
     * Fallback registry used when the global parser hasn't been initialized yet (e.g. unit tests that
     * build/parse retriever trees without a full {@code SearchModule} startup). Covers only the built-in
     * types — no plugin types, since plugins aren't known outside of node startup.
     */
    private static final RetrieverParser FALLBACK_PARSER = RetrieverModuleRegistration.buildRetrieverParser(Collections.emptyList());

    /**
     * Parse a nested child retriever — used by compound retrievers parsing their {@code "retrievers"}
     * array and transformer retrievers parsing their {@code "retriever"} field.
     * <p>
     * The parser must be positioned at the {@code START_OBJECT} of {@code { "type": {...} } }. Dispatches
     * through the same registry as the top-level {@code "retriever"} field, so any registered type —
     * {@code standard}, any core compound/transformer type, or a plugin-registered type — can appear as a
     * child. This is what allows arbitrary nesting without each retriever type hardcoding a list of the
     * types it is willing to nest.
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
