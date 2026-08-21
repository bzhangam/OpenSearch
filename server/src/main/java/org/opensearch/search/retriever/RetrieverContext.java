/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable context that flows top-down through the retriever tree during validation and
 * leaf preparation. Carries accumulated {@link LeafConstraint}s and {@link LeafModifier}s
 * from ancestor retrievers.
 * <p>
 * Each retriever node can augment the context for its children by calling
 * {@link #withConstraint(LeafConstraint)} or {@link #withModifier(LeafModifier)},
 * which returns a new instance (original unchanged).
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class RetrieverContext {

    private static final RetrieverContext ROOT = new RetrieverContext(Collections.emptyList(), Collections.emptyList());

    private final List<LeafConstraint> constraints;
    private final List<LeafModifier> modifiers;

    private RetrieverContext(List<LeafConstraint> constraints, List<LeafModifier> modifiers) {
        this.constraints = Collections.unmodifiableList(constraints);
        this.modifiers = Collections.unmodifiableList(modifiers);
    }

    /**
     * Returns the root context with no constraints or modifiers.
     */
    public static RetrieverContext root() {
        return ROOT;
    }

    /**
     * Returns a new context with the given constraint appended. Original is unchanged.
     */
    public RetrieverContext withConstraint(LeafConstraint constraint) {
        List<LeafConstraint> updated = new ArrayList<>(constraints);
        updated.add(constraint);
        return new RetrieverContext(updated, modifiers);
    }

    /**
     * Returns a new context with the given modifier appended. Original is unchanged.
     */
    public RetrieverContext withModifier(LeafModifier modifier) {
        List<LeafModifier> updated = new ArrayList<>(modifiers);
        updated.add(modifier);
        return new RetrieverContext(constraints, updated);
    }

    /**
     * Returns the accumulated constraints from ancestor retrievers.
     */
    public List<LeafConstraint> getConstraints() {
        return constraints;
    }

    /**
     * Returns the accumulated modifiers from ancestor retrievers.
     */
    public List<LeafModifier> getModifiers() {
        return modifiers;
    }
}
