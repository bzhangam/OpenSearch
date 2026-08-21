/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 * Integration point between {@link SearchSourceBuilder} and the retriever framework.
 * <p>
 * Provides parsing and validation logic called by {@link SearchSourceBuilder}
 * when it encounters a {@code "retriever"} field in the search request.
 *
 * <h2>Field Compatibility with Retrievers</h2>
 * <p>
 * When {@code "retriever"} is present, the following rules apply to other top-level
 * {@link SearchSourceBuilder} fields:
 *
 * <h3>Blocked (mutually exclusive — validation rejects these)</h3>
 * <ul>
 *   <li>{@code query} — the retriever produces the query; cannot have both</li>
 *   <li>{@code rescore} — ambiguous ordering with retriever resolution; use a rescore retriever instead</li>
 *   <li>{@code search_after} (top level) — ambiguous with fusion because fused scores are not stable across
 *       pages (they depend on the full candidate window). Use {@code search_after} on individual
 *       {@code standard} retrievers instead — see "Pagination" below</li>
 *   <li>{@code terminate_after} (top level) — permanent design choice, not deferred: by the time a retriever
 *       tree resolves there is no single collection process left to bound (leg dispatch is already complete;
 *       the final fetch is a lookup of a handful of already-known doc IDs). Per-leg early termination on
 *       individual {@code standard} retrievers is a real, well-defined use case (bounding one expensive leg's
 *       tail latency) but is intentionally not exposed yet — revisit if a concrete need emerges</li>
 *   <li>{@code ext} — semantics with retrievers are undefined; if leaf queries need extensions, add
 *       them to the standard retriever params</li>
 *   <li>{@code include_named_queries_score} — named query scores are per-leg and confusing with fusion;
 *       needs design work</li>
 *   <li>{@code slice} — scroll slicing is incompatible with single-shot retriever execution</li>
 *   <li>{@code derived_fields} — may interfere with leg query execution; blocked until confirmed safe</li>
 * </ul>
 *
 * <h3>Pagination</h3>
 * <p>
 * Exactly one pagination decision exists in the tree — the top level. Retriever-level nodes
 * (compound/transformer) never have their own {@code from}/cursor; they only control how large a
 * candidate window feeds into that one decision. Two independent, non-conflicting mechanisms:
 * <ul>
 *   <li><b>{@code from} / {@code size}</b> — pagination within the retriever's resolved window.
 *       Simple, works today. The window ({@code rank_window_size} on a compound retriever,
 *       {@code window_size} on {@code rescore}, or a leaf {@code standard}'s own {@code size} when
 *       it's the only thing between the root and the page) must be ≥ {@code from + size} — enforced
 *       by {@link RetrieverBuilder#getMaxOutputSize()}, checked once at the root by
 *       {@link #validateCompatibility}, and correct for any tree depth/shape because each node type
 *       defines its own contribution recursively.</li>
 *   <li><b>Top-level {@code sort}</b> — reorders the <i>final resolved output</i> for presentation
 *       (e.g. sort fused results by {@code price}). Does not affect which documents are in the
 *       result set, only display order within the window.</li>
 *   <li><b>Per-leg {@code sort} + {@code search_after}</b> (on a {@code standard} retriever) — each leg
 *       independently pages through its own candidates using a stored-field cursor <i>before</i> fusion.
 *       This is unambiguous because the cursor applies to exactly one leg's own sort order — it never
 *       needs to be interpreted against a fused score, and it never crosses leg boundaries. A
 *       {@code standard} retriever also supports a plain {@code from} for shallow, one-off windowing
 *       of its own candidates without requiring a sort — prefer {@code search_after} for anything
 *       deep or repeated, same guidance as plain {@code _search}.</li>
 * </ul>
 * <p>
 * Top-level {@code search_after} is deliberately not supported: fused scores are recomputed from
 * scratch each request and depend on the full candidate window, so there is no stable cursor value
 * that could be pushed down to legs or reapplied consistently across pages.
 *
 * <h3>Supported — fetch-phase (no interaction with retriever resolution)</h3>
 * <ul>
 *   <li>{@code _source} — controls which source fields to return</li>
 *   <li>{@code fields} — fetch-phase field retrieval</li>
 *   <li>{@code stored_fields} — fetch-phase stored field retrieval</li>
 *   <li>{@code docvalue_fields} — fetch-phase doc value retrieval</li>
 *   <li>{@code script_fields} — fetch-phase scripted fields</li>
 *   <li>{@code highlight} — runs on final hits; leaf queries provide term context</li>
 *   <li>{@code version} — return document version</li>
 *   <li>{@code seq_no_primary_term} — return sequence number and primary term</li>
 * </ul>
 *
 * <h3>Supported — result-set control</h3>
 * <ul>
 *   <li>{@code from} / {@code size} — pagination on final results</li>
 *   <li>{@code sort} — reorders the final fused output for presentation (see "Pagination" above)</li>
 *   <li>{@code post_filter} — filters final hits without affecting aggregation counts</li>
 *   <li>{@code min_score} — applied on the resolved query's output</li>
 *   <li>{@code collapse} — deduplicates final results by field</li>
 *   <li>{@code track_scores} — return scores even when sorting by field</li>
 *   <li>{@code track_total_hits} — control total hit counting</li>
 * </ul>
 *
 * <h3>Supported — aggregations</h3>
 * <ul>
 *   <li>{@code aggs} / {@code aggregations} — computed over the union of all leg queries' match sets
 *       (via global leg), not just the final top-N results</li>
 *   <li>{@code suggest} — computed on the global leg (union of all leaf queries) and merged into the
 *       final response, in parallel with the scoring legs</li>
 * </ul>
 *
 * <h3>Supported — infrastructure (propagated to leg sub-searches)</h3>
 * <ul>
 *   <li>{@code timeout} — bounds entire request execution</li>
 *   <li>{@code pit} — point-in-time snapshot propagated to each leg for consistency</li>
 *   <li>{@code preference} / {@code routing} — shard routing propagated to sub-searches</li>
 *   <li>{@code indices_boost} — per-index score boost propagated to sub-searches</li>
 *   <li>{@code stats} — statistics groups</li>
 *   <li>{@code search_pipeline} — search pipeline (request processors only; response/phase-results
 *       processors are rejected)</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class SearchSourceBuilderRetrieverIntegration {

    private SearchSourceBuilderRetrieverIntegration() {}

    /**
     * Node-scope safety cap on the number of leaf ({@code standard}) retrievers a single request may
     * fan out to. Bounds fan-out amplification (each leaf is an independent sub-search). Default 5.
     */
    public static final org.opensearch.common.settings.Setting<Integer> MAX_LEAF_COUNT_SETTING =
        org.opensearch.common.settings.Setting.intSetting(
            "search.retriever.max_leaf_count",
            5,
            1,
            org.opensearch.common.settings.Setting.Property.NodeScope
        );

    /**
     * Node-scope safety cap on retriever tree depth (root = depth 1). Bounds the number of serial
     * async rounds and overall request complexity. Default 5.
     */
    public static final org.opensearch.common.settings.Setting<Integer> MAX_DEPTH_SETTING =
        org.opensearch.common.settings.Setting.intSetting(
            "search.retriever.max_depth",
            5,
            1,
            org.opensearch.common.settings.Setting.Property.NodeScope
        );

    private static volatile int maxLeafCount = MAX_LEAF_COUNT_SETTING.getDefault(org.opensearch.common.settings.Settings.EMPTY);
    private static volatile int maxDepth = MAX_DEPTH_SETTING.getDefault(org.opensearch.common.settings.Settings.EMPTY);

    /**
     * Read the retriever safety caps from node settings. Called by {@code SearchModule} at startup.
     */
    public static void configureLimits(org.opensearch.common.settings.Settings settings) {
        maxLeafCount = MAX_LEAF_COUNT_SETTING.get(settings);
        maxDepth = MAX_DEPTH_SETTING.get(settings);
    }

    /** Max leaf count cap (node scope). */
    public static int getMaxLeafCount() {
        return maxLeafCount;
    }

    /** Max tree depth cap (node scope). */
    public static int getMaxDepth() {
        return maxDepth;
    }

    /**
     * Global RetrieverParser instance set once by SearchModule at node startup.
     * Used by SearchSourceBuilder.parseXContent() to dispatch retriever types from the registry
     * (including plugin-registered types) rather than hardcoding type names.
     */
    private static volatile RetrieverParser globalRetrieverParser;

    /**
     * Set the global RetrieverParser. Called by {@code SearchModule} during initialization.
     * <p>
     * Idempotent (last-writer-wins), not "once ever": in production exactly one {@code SearchModule}
     * is constructed per node, so this only ever runs once in practice. But many unit tests —
     * inside and outside this package — legitimately construct more than one {@code SearchModule}
     * in the same JVM (test runners routinely share a JVM across test classes), and each
     * construction calls this. A "can only be set once ever" guard broke every one of those tests
     * with an {@code IllegalStateException} the moment more than one ran in the same JVM fork; the
     * registry itself is cheap to rebuild and has no state worth protecting across calls.
     */
    public static void setGlobalRetrieverParser(RetrieverParser parser) {
        globalRetrieverParser = parser;
    }

    /**
     * Get the global RetrieverParser for parsing retriever types.
     * Returns null if not yet initialized (e.g., in unit tests without full module setup).
     */
    public static RetrieverParser getGlobalRetrieverParser() {
        return globalRetrieverParser;
    }

    /**
     * Reset the global parser (for testing only).
     */
    static void resetGlobalRetrieverParser() {
        globalRetrieverParser = null;
    }

    /**
     * Parse the retriever field value from XContent using the registry-based parser.
     *
     * @param parser positioned at the START_OBJECT of the "retriever" field value
     * @param retrieverParser the registry-based parser for retriever types
     * @return the parsed RetrieverBuilder
     */
    public static RetrieverBuilder parseRetriever(XContentParser parser, RetrieverParser retrieverParser) throws IOException {
        return retrieverParser.parse(parser);
    }

    /**
     * Validate that the retriever is not combined with incompatible top-level fields.
     * Called at the end of {@link SearchSourceBuilder#parseXContent} after all fields are parsed.
     * <p>
     * Fails fast with a clear error message identifying the conflict.
     *
     * @param source the fully-parsed search source builder
     * @throws IllegalArgumentException if retriever is combined with a blocked field
     */
    public static void validateCompatibility(SearchSourceBuilder source) {
        if (source.retriever() == null) {
            return;
        }
        // A bare standard retriever at root is pointless — use query directly.
        // The retriever framework exists for composition (compound/transformer wrapping leaves).
        if (source.retriever() instanceof StandardRetrieverBuilder) {
            throw new IllegalArgumentException(
                "[standard] retriever cannot be used at the top level; "
                    + "use [query] directly or wrap [standard] inside a compound/transformer retriever"
            );
        }
        if (source.query() != null) {
            throw new IllegalArgumentException("cannot use [retriever] and [query] together");
        }
        if (source.rescores() != null && !source.rescores().isEmpty()) {
            throw new IllegalArgumentException("cannot use [retriever] and [rescore] together");
        }
        if (source.searchAfter() != null) {
            throw new IllegalArgumentException(
                "cannot use [retriever] and [search_after] together at the top level; "
                    + "use [search_after] on individual [standard] retrievers instead — "
                    + "top-level search_after is ambiguous with fusion because fused scores are not stable "
                    + "across pages (they depend on the full candidate window)"
            );
        }
        if (source.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
            throw new IllegalArgumentException(
                "cannot use [retriever] and [terminate_after] together; this is a permanent design choice, not a "
                    + "temporary gap — by the time a retriever tree resolves, there is no single collection process "
                    + "left to bound (leg dispatch is already complete, and the final fetch is a lookup of a handful "
                    + "of already-known doc IDs where an early-termination cap has nothing to act on)"
            );
        }
        if (source.ext() != null && !source.ext().isEmpty()) {
            throw new IllegalArgumentException("cannot use [retriever] and [ext] together");
        }
        if (source.includeNamedQueriesScore()) {
            throw new IllegalArgumentException(
                "cannot use [retriever] and [include_named_queries_score] together"
            );
        }
        if (source.slice() != null) {
            throw new IllegalArgumentException(
                "cannot use [retriever] and [slice] together; scroll slicing is incompatible with retrievers"
            );
        }
        if ((source.getDerivedFieldsObject() != null && !source.getDerivedFieldsObject().isEmpty())
            || (source.getDerivedFields() != null && !source.getDerivedFields().isEmpty())) {
            throw new IllegalArgumentException("cannot use [retriever] and [derived_fields] together");
        }

        // from + size must fit within the tree's available window. This is checked once at the
        // root via the recursive RetrieverBuilder#getMaxOutputSize() contract, so it's correct
        // regardless of tree depth/shape (compound rank_window_size, transformer pass-through or
        // shrink, leaf size — see getMaxOutputSize() javadoc).
        int effectiveFrom = source.from() < 0 ? SearchService.DEFAULT_FROM : source.from();
        int effectiveSize = source.size() < 0 ? SearchService.DEFAULT_SIZE : source.size();
        int maxOutputSize = source.retriever().getMaxOutputSize();
        if (effectiveFrom + effectiveSize > maxOutputSize) {
            throw new IllegalArgumentException(
                "[from] (" + effectiveFrom + ") + [size] (" + effectiveSize + ") exceeds the retriever's "
                    + "available window (" + maxOutputSize + "); increase [rank_window_size] on the enclosing "
                    + "compound retriever (or [window_size] on a wrapping [rescore]), or reduce [from]/[size]"
            );
        }
    }

    /**
     * Validate that a search pipeline does not contain unsupported processor types when
     * used with retrievers.
     * <p>
     * Only request processors are supported. Response processors and phase results processors
     * conflict with retriever-managed result transformation.
     * <p>
     * Called from {@link RetrieverExecutor} before dispatch (Phase 2+).
     * For Phase 1 ({@code standard} retriever only), the retriever resolves into a plain query
     * before pipeline execution, so all processors work normally.
     *
     * @param pipelineName              name of the resolved pipeline (for error messages)
     * @param hasResponseProcessors     whether the pipeline has any response processors
     * @param hasPhaseResultsProcessors whether the pipeline has any phase results processors
     * @param responseProcessorNames    names of response processors (for error messages)
     * @param phaseResultsProcessorNames names of phase results processors (for error messages)
     * @throws IllegalArgumentException if unsupported processor types are present
     */
    public static void validatePipelineCompatibility(
        String pipelineName,
        boolean hasResponseProcessors,
        boolean hasPhaseResultsProcessors,
        List<String> responseProcessorNames,
        List<String> phaseResultsProcessorNames
    ) {
        if (hasResponseProcessors) {
            String processorList = String.join(", ", responseProcessorNames);
            throw new IllegalArgumentException(
                "search pipeline [" + pipelineName + "] contains response processor ["
                    + processorList + "] which is not supported with [retriever]. "
                    + "Response processors cannot be used with retrievers because the retriever tree "
                    + "handles result transformation (normalization, fusion, reranking) internally."
            );
        }
        if (hasPhaseResultsProcessors) {
            String processorList = String.join(", ", phaseResultsProcessorNames);
            throw new IllegalArgumentException(
                "search pipeline [" + pipelineName + "] contains phase results processor ["
                    + processorList + "] which is not supported with [retriever]. "
                    + "Phase results processors cannot be used with retrievers because the retriever "
                    + "tree controls the boundary between query and fetch phases internally."
            );
        }
    }

    /**
     * The field name used in the search request for the retriever.
     */
    public static final String RETRIEVER_FIELD = "retriever";
}
