/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A typed interface for modifications that compound/transformer retrievers propagate to leaf
 * {@link StandardRetrieverBuilder} nodes during the top-down preparation phase.
 * <p>
 * New retriever types add new modifiers without changing {@link StandardRetrieverBuilder} or
 * {@link RetrieverContext} — just implement this interface and register it via
 * {@link RetrieverContext#withModifier(LeafModifier)}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
@FunctionalInterface
public interface LeafModifier {

    /**
     * Apply this modification to the given leaf retriever.
     *
     * @param leaf the standard retriever builder to modify
     */
    void apply(StandardRetrieverBuilder leaf);
}
