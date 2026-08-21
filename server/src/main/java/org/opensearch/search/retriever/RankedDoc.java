/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

/**
 * Immutable record representing a document with its retriever-computed score and shard location.
 * This is the universal currency between retriever nodes — every retriever produces a list of these.
 * <p>
 * Optionally carries a Lucene {@link Explanation} for explain support — populated when
 * {@code explain: true} is set on the search request.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class RankedDoc implements Writeable {

    private final String id;
    private final String index;
    private final ShardId shardId;
    private final float score;
    private final int position;
    private final Explanation explanation;

    /**
     * Full constructor with all fields including explanation.
     *
     * @param id          the document _id
     * @param index       the index name the document belongs to
     * @param shardId     the shard that owns this document
     * @param score       the retriever-computed score
     * @param position    the 0-based position in the retriever output
     * @param explanation the Lucene explanation (nullable, only present when explain=true)
     */
    public RankedDoc(String id, String index, ShardId shardId, float score, int position, Explanation explanation) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.index = Objects.requireNonNull(index, "index must not be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId must not be null");
        this.score = score;
        this.position = position;
        this.explanation = explanation;
    }

    /**
     * Constructor without explanation (backward-compatible).
     */
    public RankedDoc(String id, String index, ShardId shardId, float score, int position) {
        this(id, index, shardId, score, position, null);
    }

    /**
     * Convenience constructor without position (defaults to 0) or explanation.
     */
    public RankedDoc(String id, String index, ShardId shardId, float score) {
        this(id, index, shardId, score, 0, null);
    }

    public RankedDoc(StreamInput in) throws IOException {
        this.id = in.readString();
        this.index = in.readString();
        this.shardId = new ShardId(in);
        this.score = in.readFloat();
        this.position = in.readVInt();
        this.explanation = null; // Explanation is transient — not serialized over the wire
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(index);
        shardId.writeTo(out);
        out.writeFloat(score);
        out.writeVInt(position);
    }

    public String id() {
        return id;
    }

    public String index() {
        return index;
    }

    public ShardId shardId() {
        return shardId;
    }

    public float score() {
        return score;
    }

    public int position() {
        return position;
    }

    /**
     * The Lucene explanation for this document's score in the leg that produced it.
     * Null when explain is not requested.
     */
    public Explanation explanation() {
        return explanation;
    }

    /**
     * Create a copy with a different score, position, and explanation.
     * Used during fusion when a doc's score/position changes but its identity stays the same.
     */
    public RankedDoc withScoreAndPosition(float newScore, int newPosition, Explanation newExplanation) {
        return new RankedDoc(id, index, shardId, newScore, newPosition, newExplanation);
    }

    /**
     * Create a copy with a different score and position, preserving the original explanation.
     */
    public RankedDoc withScoreAndPosition(float newScore, int newPosition) {
        return new RankedDoc(id, index, shardId, newScore, newPosition, explanation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankedDoc rankedDoc = (RankedDoc) o;
        return Float.compare(rankedDoc.score, score) == 0
            && position == rankedDoc.position
            && Objects.equals(id, rankedDoc.id)
            && Objects.equals(index, rankedDoc.index)
            && Objects.equals(shardId, rankedDoc.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index, shardId, score, position);
    }

    @Override
    public String toString() {
        return "RankedDoc{id='" + id + "', index='" + index + "', shardId=" + shardId
            + ", score=" + score + ", position=" + position
            + (explanation != null ? ", explained" : "") + "}";
    }
}
