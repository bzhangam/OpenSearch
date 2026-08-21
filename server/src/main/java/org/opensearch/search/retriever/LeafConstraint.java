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
 * A typed interface for constraints that compound/transformer retrievers enforce on leaf
 * {@link StandardRetrieverBuilder} nodes during the top-down validation phase.
 * <p>
 * Implementations throw {@link IllegalArgumentException} if a leaf violates the constraint.
 * New retriever types add new constraints without changing {@link StandardRetrieverBuilder} or
 * {@link RetrieverContext}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
@FunctionalInterface
public interface LeafConstraint {

    /**
     * Validate that the given leaf retriever satisfies this constraint.
     *
     * @param leaf the standard retriever builder to validate
     * @throws IllegalArgumentException if the leaf violates this constraint
     */
    void validate(StandardRetrieverBuilder leaf);
}
