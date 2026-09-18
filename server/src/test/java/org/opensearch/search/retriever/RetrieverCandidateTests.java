/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the coordinator-only {@link RetrieverCandidate} DTO: construction, projection to the
 * wire {@link RankDoc} (narrowing {@link ShardId} to {@code int}), and copy-on-rewrite.
 */
public class RetrieverCandidateTests extends OpenSearchTestCase {

    private static ShardId shard(String index, int shard) {
        return new ShardId(new Index(index, "_na_"), shard);
    }

    public void testGettersAndConstruction() {
        ShardId sid = shard("products", 2);
        RetrieverCandidate c = new RetrieverCandidate("products", sid, "a", 0.9f, 3);
        assertEquals("products", c.index());
        assertSame(sid, c.shardId());
        assertEquals("a", c.id());
        assertEquals(0.9f, c.score(), 0.0f);
        assertEquals(3, c.position());
    }

    public void testToRankDocNarrowsShardIdToInt() {
        RetrieverCandidate c = new RetrieverCandidate("products", shard("products", 2), "a", 0.9f, 3);
        RankDoc rd = c.toRankDoc();
        assertEquals("products", rd.index());
        assertEquals(2, rd.shardId()); // full ShardId narrowed to the int shard number
        assertEquals("a", rd.id());
        assertEquals(0.9f, rd.score(), 0.0f);
        assertEquals(3, rd.position());
    }

    public void testWithScoreAndPositionPreservesIdentity() {
        ShardId sid = shard("products", 1);
        RetrieverCandidate original = new RetrieverCandidate("products", sid, "a", 0.5f, 0);
        RetrieverCandidate rewritten = original.withScoreAndPosition(0.8f, 4);

        // identity preserved
        assertEquals("products", rewritten.index());
        assertSame(sid, rewritten.shardId());
        assertEquals("a", rewritten.id());
        // score/position overridden
        assertEquals(0.8f, rewritten.score(), 0.0f);
        assertEquals(4, rewritten.position());
        // original unchanged (immutable)
        assertEquals(0.5f, original.score(), 0.0f);
        assertEquals(0, original.position());
    }

    public void testNullIndexRejected() {
        expectThrows(NullPointerException.class, () -> new RetrieverCandidate(null, shard("products", 0), "a", 0.1f, 0));
    }

    public void testNullShardIdRejected() {
        expectThrows(NullPointerException.class, () -> new RetrieverCandidate("products", null, "a", 0.1f, 0));
    }

    public void testNullIdRejected() {
        expectThrows(NullPointerException.class, () -> new RetrieverCandidate("products", shard("products", 0), null, 0.1f, 0));
    }
}
