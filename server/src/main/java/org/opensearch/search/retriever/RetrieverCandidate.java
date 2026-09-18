/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.index.shard.ShardId;

import java.util.Objects;

/**
 * Coordinator-only working record for one candidate document as it flows through the retriever tree
 * during resolution.
 * <p>
 * It is a <b>candidate</b> — not a hit and not the final result. The executor builds these from each
 * leg's query-phase {@code SearchHit}s; compound/transformer nodes re-score (fuse) and re-order (reshape)
 * them; and only the surviving candidates are projected to {@link RankDoc}s and fetched into real hits by
 * the final {@code RankDocsQuery}. Because its {@code score}/{@code position} change as it moves up the
 * tree, and no single query produced its post-fusion score, it is deliberately distinct from
 * {@code SearchHit} (a fetched result) and from {@code RankDoc} (the wire form).
 * <p>
 * <b>Not {@link org.opensearch.core.common.io.stream.Writeable}</b>: it never crosses the wire. The
 * executor narrows it to a {@link RankDoc} at {@code toQueryBuilder()} via {@link #toRankDoc()}; only the
 * {@code RankDoc} is broadcast to shards. Immutable — rewrites produce a copy via
 * {@link #withScoreAndPosition(float, int)}.
 *
 * @opensearch.internal
 */
final class RetrieverCandidate {

    private final String index;
    private final ShardId shardId;
    private final String id;
    private final float score;
    private final int position;

    RetrieverCandidate(String index, ShardId shardId, String id, float score, int position) {
        this.index = Objects.requireNonNull(index, "index");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.id = Objects.requireNonNull(id, "id");
        this.score = score;
        this.position = position;
    }

    String index() {
        return index;
    }

    ShardId shardId() {
        return shardId;
    }

    String id() {
        return id;
    }

    float score() {
        return score;
    }

    int position() {
        return position;
    }

    /**
     * Project this coordinator-side candidate to the wire {@link RankDoc}, narrowing the full
     * {@link ShardId} to the {@code int} shard number the broadcast query filters on. Coordinator-only
     * state (explanation/timing, added in later sub-features) is intentionally dropped here.
     */
    RankDoc toRankDoc() {
        return new RankDoc(index, shardId.id(), id, score, position);
    }

    /**
     * A copy with a new score and position, preserving identity ({@code index}/{@code shardId}/{@code id}).
     * Used when a fusion/reshape node re-ranks a candidate without changing which document it is.
     */
    RetrieverCandidate withScoreAndPosition(float newScore, int newPosition) {
        return new RetrieverCandidate(index, shardId, id, newScore, newPosition);
    }

    @Override
    public String toString() {
        return "RetrieverCandidate{index='"
            + index
            + "', shardId="
            + shardId
            + ", id='"
            + id
            + "', score="
            + score
            + ", position="
            + position
            + '}';
    }
}
