/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
     * Upper bound on how many documents this node can contribute to its parent — or, at the root, to the
     * final page. This is the single source of truth for pagination validation: it lets {@code from + size}
     * be checked against the tree regardless of depth or shape.
     * <p>
     * Each node type defines this in terms of its own knobs and its child/children:
     * <ul>
     *   <li>{@link StandardRetrieverBuilder} — its own {@code size} (candidate depth)</li>
     *   <li>a compound retriever — its own {@code rank_window_size} (fusion window)</li>
     *   <li>a transformer retriever — by default delegates to its child (reshaping never adds documents),
     *       unless it can shrink the window (e.g. a rescore window)</li>
     * </ul>
     */
    public abstract int getMaxOutputSize();

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
