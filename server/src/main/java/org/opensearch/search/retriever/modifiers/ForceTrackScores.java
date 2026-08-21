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

/**
 * Forces {@code trackScores = true} on leaf retrievers. Required by compound retrievers
 * that need raw scores for normalization (e.g., score_fusion).
 *
 * @opensearch.internal
 */
public final class ForceTrackScores implements LeafModifier {

    public static final ForceTrackScores INSTANCE = new ForceTrackScores();

    private ForceTrackScores() {}

    @Override
    public void apply(StandardRetrieverBuilder leaf) {
        leaf.setTrackScores(true);
    }
}
