/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Integration point between {@link SearchSourceBuilder} and the retriever framework.
 * <p>
 * Provides parsing and validation logic called by {@link SearchSourceBuilder} when it encounters a
 * {@code "retriever"} field in the search request. When {@code "retriever"} is present, a set of
 * top-level fields is mutually exclusive with it (see {@link #validateCompatibility}); each rejection
 * message states the reason the field is blocked.
 *
 * @opensearch.internal
 */
public final class SearchSourceBuilderRetrieverIntegration {

    private SearchSourceBuilderRetrieverIntegration() {}

    /** The field name used in the search request for the retriever. */
    public static final String RETRIEVER_FIELD = "retriever";

    /**
     * Node-scope safety cap on the number of leaf ({@code standard}) retrievers a single request may fan
     * out to. Bounds fan-out amplification (each leaf is an independent sub-search). Default 5.
     */
    public static final Setting<Integer> MAX_LEAF_COUNT_SETTING = Setting.intSetting(
        "search.retriever.max_leaf_count",
        5,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Node-scope safety cap on retriever tree depth (root = depth 1). Bounds the number of serial async
     * rounds and overall request complexity. Default 5.
     */
    public static final Setting<Integer> MAX_DEPTH_SETTING = Setting.intSetting(
        "search.retriever.max_depth",
        5,
        1,
        Setting.Property.NodeScope
    );

    private static volatile int maxLeafCount = MAX_LEAF_COUNT_SETTING.getDefault(Settings.EMPTY);
    private static volatile int maxDepth = MAX_DEPTH_SETTING.getDefault(Settings.EMPTY);

    /**
     * Read the retriever safety caps from node settings. Called by {@code SearchModule} at startup.
     */
    public static void configureLimits(Settings settings) {
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
     * Global RetrieverParser instance set once by {@code SearchModule} at node startup. Used by
     * {@link SearchSourceBuilder#parseXContent} to dispatch retriever types from the registry (including
     * plugin-registered types) rather than hardcoding type names.
     */
    private static volatile RetrieverParser globalRetrieverParser;

    /**
     * Set the global RetrieverParser. Called by {@code SearchModule} during initialization.
     * <p>
     * Idempotent (last-writer-wins), not "once ever": in production exactly one {@code SearchModule} is
     * constructed per node, so this only ever runs once in practice. But many unit tests construct more
     * than one {@code SearchModule} in the same JVM (test runners share a JVM across test classes), and
     * each construction calls this. A "can only be set once ever" guard would break those tests; the
     * registry is cheap to rebuild and has no state worth protecting across calls.
     */
    public static void setGlobalRetrieverParser(RetrieverParser parser) {
        globalRetrieverParser = parser;
    }

    /**
     * Get the global RetrieverParser for parsing retriever types. Returns null if not yet initialized
     * (e.g., in unit tests without full module setup).
     */
    public static RetrieverParser getGlobalRetrieverParser() {
        return globalRetrieverParser;
    }

    /** Reset the global parser (for testing only). */
    static void resetGlobalRetrieverParser() {
        globalRetrieverParser = null;
    }

    /**
     * Parse the retriever field value from XContent using the registry-based parser.
     *
     * @param parser          positioned at the START_OBJECT of the "retriever" field value
     * @param retrieverParser the registry-based parser for retriever types
     * @return the parsed RetrieverBuilder
     */
    public static RetrieverBuilder parseRetriever(XContentParser parser, RetrieverParser retrieverParser) throws IOException {
        return retrieverParser.parse(parser);
    }

    /**
     * Validate that the retriever is not combined with incompatible top-level fields. Called at the end of
     * {@link SearchSourceBuilder#parseXContent} after all fields are parsed. Fails fast with a clear error
     * message identifying the conflict and the reason it is blocked.
     *
     * @param source the fully-parsed search source builder
     * @throws IllegalArgumentException if retriever is combined with a blocked field
     */
    public static void validateCompatibility(SearchSourceBuilder source) {
        if (source.retriever() == null) {
            return;
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
            throw new IllegalArgumentException("cannot use [retriever] and [include_named_queries_score] together");
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
        // Search pipelines are blocked wholesale with retrievers for now. A pipeline can carry request,
        // response, and phase-results processors; whether any given processor is safe with retriever-managed
        // result transformation (normalization, fusion, reranking, and the query↔fetch boundary the tree
        // controls) must be evaluated per processor. Rather than maintain an implicit allow/deny by processor
        // category, we block all search pipelines until a processor can explicitly declare itself
        // retriever-compatible (a future opt-in), at which point each is evaluated on its own merits.
        if (source.pipeline() != null || (source.searchPipelineSource() != null && !source.searchPipelineSource().isEmpty())) {
            throw new IllegalArgumentException(
                "cannot use [retriever] and [search_pipeline] together; search pipelines are not yet supported with "
                    + "retrievers — a processor must be explicitly evaluated and marked retriever-compatible before it "
                    + "can run with a retriever (the retriever tree controls result transformation and the query/fetch boundary)"
            );
        }

        // from + size must fit within the tree's available window. Checked once at the root via the
        // recursive RetrieverBuilder#getMaxOutputSize() contract, so it's correct regardless of tree
        // depth/shape (compound rank_window_size, transformer pass-through or shrink, leaf size).
        int effectiveFrom = source.from() < 0 ? SearchService.DEFAULT_FROM : source.from();
        int effectiveSize = source.size() < 0 ? SearchService.DEFAULT_SIZE : source.size();
        int maxOutputSize = source.retriever().getMaxOutputSize();
        if (effectiveFrom + effectiveSize > maxOutputSize) {
            throw new IllegalArgumentException(
                "[from] ("
                    + effectiveFrom
                    + ") + [size] ("
                    + effectiveSize
                    + ") exceeds the retriever's "
                    + "available window ("
                    + maxOutputSize
                    + "); increase [rank_window_size] on the enclosing "
                    + "compound retriever (or [window_size] on a wrapping [rescore]), or reduce [from]/[size]"
            );
        }
    }
}
