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
 * Opt-in contract for a {@link org.opensearch.index.query.QueryBuilder} used as the query of a
 * {@code standard} retriever leg that sits under a fusion compound (e.g. {@link RankFusionRetrieverBuilder}).
 * <p>
 * A fusion-governed leg must contribute exactly {@code rank_window_size} candidates so the fused window is
 * complete and reproducible across requests (see {@link RetrieverBuilder#prepareLeaves(LeafPreparationContext)}).
 * For most queries the leg's {@code size} alone controls how many candidates it returns. But some query types
 * have their own internal candidate cap that {@code size} does not touch — the canonical case is the {@code knn}
 * query's {@code k} (return the {@code k} nearest). If that cap is smaller than the window, the leg silently
 * under-produces and the fused window is incomplete regardless of the leg's {@code size}.
 * <p>
 * Such a query implements {@code RetrieverWindowAware} so the retriever can hand it the fusion window and let the query
 * decide how (or whether) to honor it. Core stays agnostic of any plugin query type — it only knows this
 * interface — and each query owns its own mode-specific logic (e.g. {@code knn} in its count-bounded mode
 * fills an unset {@code k} with the window, accepts an explicit {@code k >= window}, and rejects an explicit
 * {@code k < window} that could not fill the window; and no-ops in its {@code min_score}/{@code max_distance}
 * threshold modes, where {@code size} truncation already applies).
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public interface RetrieverWindowAware {

    /**
     * Ensure this query can produce at least {@code window} candidates when used as a fusion-governed retriever
     * leg. Implementations should raise or fill an internal cap so it is at least {@code window}, must not shrink
     * a user's larger value, and should reject (throw {@link IllegalArgumentException}) an explicitly-set cap that
     * is smaller than {@code window} — since a leg that cannot supply the window silently breaks the fused
     * window's completeness. Implementations whose candidate count is not bounded by such a cap should no-op.
     *
     * @param window the enclosing fusion's {@code rank_window_size}
     * @throws IllegalArgumentException if the query has an explicit candidate cap smaller than {@code window}
     */
    void applyRetrieverWindow(int window);
}
