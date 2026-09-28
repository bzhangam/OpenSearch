/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.annotation.PublicApi;

/**
 * Immutable state threaded top-down through {@link RetrieverBuilder#prepareLeaves(LeafPreparationContext)}
 * as a compound prepares its subtree. Carrying a context object (rather than positional parameters) means a
 * new top-down concern can be added as a field plus a {@code withX(...)} derivation without changing the
 * {@code prepareLeaves} signature or every override.
 * <p>
 * <b>Fusion window.</b> A fusion compound (e.g. {@link RankFusionRetrieverBuilder}) requires every leg in
 * its subtree to contribute exactly {@code rank_window_size} candidates — otherwise the fused window is
 * incomplete and its membership/order is not reproducible across requests (which breaks pagination). Such a
 * node derives {@link #underFusion(int)} to push its window down and mark the subtree fusion-governed; every
 * node below propagates the same context unchanged. {@link #isFusionGoverned()} is monotonic: once
 * {@code true} it never clears, because no current retriever type can guarantee it emits a complete, stable
 * window while letting a descendant leg size itself.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public final class LeafPreparationContext {

    /** Sentinel for {@link #getInheritedWindow()} when no fusion ancestor has set a window. */
    public static final int NO_WINDOW = -1;

    private final int inheritedWindow;
    private final boolean fusionGoverned;
    private final boolean explain;
    private final boolean profile;

    private LeafPreparationContext(int inheritedWindow, boolean fusionGoverned, boolean explain, boolean profile) {
        this.inheritedWindow = inheritedWindow;
        this.fusionGoverned = fusionGoverned;
        this.explain = explain;
        this.profile = profile;
    }

    /** The root context: nothing above, not fusion-governed, no inherited window, explain/profile off. */
    public static LeafPreparationContext root() {
        return new LeafPreparationContext(NO_WINDOW, false, false, false);
    }

    /**
     * The root context with the request-level {@code explain} and {@code profile} flags. Both are top-down
     * request concerns: they propagate unchanged to every leg so each leaf sub-search can run with Lucene
     * explain and/or query profiling enabled.
     */
    public static LeafPreparationContext root(boolean explain, boolean profile) {
        return new LeafPreparationContext(NO_WINDOW, false, explain, profile);
    }

    /**
     * Derive the context a fusion node hands to its children: mark the subtree fusion-governed and set the
     * window every descendant leg must fetch. An inner fusion is its own scope for the depth it needs, so it
     * substitutes its own window regardless of any window inherited from above. The {@code explain} and
     * {@code profile} flags are preserved unchanged.
     *
     * @param window the fusion node's effective {@code rank_window_size}
     */
    public LeafPreparationContext underFusion(int window) {
        return new LeafPreparationContext(window, true, explain, profile);
    }

    /** The fetch depth handed down from the nearest fusion ancestor, or {@link #NO_WINDOW} if none. */
    public int getInheritedWindow() {
        return inheritedWindow;
    }

    /** {@code true} once any ancestor is a fusion compound; monotonic down the tree. */
    public boolean isFusionGoverned() {
        return fusionGoverned;
    }

    /** {@code true} when the request set {@code explain: true}; every leg runs its sub-search with explain. */
    public boolean isExplain() {
        return explain;
    }

    /** {@code true} when the request set {@code profile: true}; every leg runs its sub-search with profiling. */
    public boolean isProfile() {
        return profile;
    }
}
