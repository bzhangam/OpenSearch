/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever.constraints;

import org.opensearch.search.retriever.LeafConstraint;
import org.opensearch.search.retriever.StandardRetrieverBuilder;

import java.util.Objects;

/**
 * Rejects leaves that explicitly set {@code trackScores = false}. Used by compound
 * retrievers that require scores for normalization/fusion.
 *
 * @opensearch.internal
 */
public final class DisallowTrackScoresFalse implements LeafConstraint {

    private final String parentName;

    public DisallowTrackScoresFalse(String parentName) {
        this.parentName = Objects.requireNonNull(parentName, "parentName must not be null");
    }

    @Override
    public void validate(StandardRetrieverBuilder leaf) {
        if (leaf.getTrackScores() != null && !leaf.getTrackScores()) {
            throw new IllegalArgumentException(
                "[standard] cannot disable [track_scores] inside [" + parentName + "]"
            );
        }
    }
}
