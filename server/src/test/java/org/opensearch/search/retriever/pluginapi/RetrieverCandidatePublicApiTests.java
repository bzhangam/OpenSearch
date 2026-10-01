/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever.pluginapi;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.retriever.RetrieverCandidate;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Verifies that {@link RetrieverCandidate} is usable as public API from <b>outside</b> the
 * {@code org.opensearch.search.retriever} package — i.e. by a plugin-provided reranker (such as the k-NN
 * {@code diversify} retriever) that overrides {@code TransformerRetrieverBuilder.reshape(...)}. The whole
 * point of this test living in a different package is that it would fail to <i>compile</i> if the type or
 * any member it touches were package-private.
 */
public class RetrieverCandidatePublicApiTests extends OpenSearchTestCase {

    private static ShardId shard() {
        return new ShardId(new Index("idx", "uuid"), 0);
    }

    public void testPublicConstructorAndAccessors() {
        RetrieverCandidate c = new RetrieverCandidate("idx", shard(), "doc-1", 1.5f, 3);
        assertEquals("idx", c.index());
        assertEquals("doc-1", c.id());
        assertEquals(0, c.shardId().id());
        assertEquals(1.5f, c.score(), 0.0f);
        assertEquals(3, c.position());
        assertNull("no explanation when not provided", c.explanation());
    }

    public void testWithScoreAndPositionPreservesIdentity() {
        RetrieverCandidate c = new RetrieverCandidate("idx", shard(), "doc-1", 1.5f, 3);
        RetrieverCandidate re = c.withScoreAndPosition(0.25f, 0);
        // identity preserved
        assertEquals(c.index(), re.index());
        assertEquals(c.id(), re.id());
        assertEquals(c.shardId().id(), re.shardId().id());
        // score/position updated
        assertEquals(0.25f, re.score(), 0.0f);
        assertEquals(0, re.position());
    }

    /**
     * Simulates the shape of a plugin reshape: read candidates via public accessors and emit a re-ranked
     * copy list using the public {@code withScoreAndPosition}. Compiles only because the surface is public.
     */
    public void testSimulatedPluginReshapeOverPublicSurface() {
        List<RetrieverCandidate> window = List.of(
            new RetrieverCandidate("idx", shard(), "a", 0.9f, 0),
            new RetrieverCandidate("idx", shard(), "b", 0.8f, 1)
        );
        // Reverse order, renumber — the kind of thing a reranker does.
        RetrieverCandidate first = window.get(1).withScoreAndPosition(window.get(1).score(), 0);
        RetrieverCandidate second = window.get(0).withScoreAndPosition(window.get(0).score(), 1);
        List<RetrieverCandidate> reshaped = List.of(first, second);
        assertEquals("b", reshaped.get(0).id());
        assertEquals(0, reshaped.get(0).position());
        assertEquals("a", reshaped.get(1).id());
        assertEquals(1, reshaped.get(1).position());
    }
}
