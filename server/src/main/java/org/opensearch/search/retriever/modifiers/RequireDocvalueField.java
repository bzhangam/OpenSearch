/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever.modifiers;

import org.opensearch.search.retriever.LeafModifier;
import org.opensearch.search.retriever.StandardRetrieverBuilder;

import java.util.Objects;

/**
 * Adds a required docvalue field to leaf retrievers. Used by retrievers that need
 * specific field data from leaves (e.g., MMR needs the vector field for diversity computation).
 *
 * @opensearch.internal
 */
public final class RequireDocvalueField implements LeafModifier {

    private final String field;

    public RequireDocvalueField(String field) {
        this.field = Objects.requireNonNull(field, "field must not be null");
    }

    @Override
    public void apply(StandardRetrieverBuilder leaf) {
        leaf.addDocvalueField(field);
    }

    public String getField() {
        return field;
    }
}
