/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.Map;
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
 * <p>
 * <b>Public API.</b> This is the input to {@link TransformerRetrieverBuilder#reshape(java.util.List)}, so a
 * plugin-provided reranker (registered via {@code RetrieverPlugin}, e.g. the {@code diversify} retriever in
 * the k-NN plugin) reads and re-ranks these across package boundaries. It is therefore public API, despite
 * being a coordinator-only working record that never serializes.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public final class RetrieverCandidate {

    private final String index;
    private final ShardId shardId;
    private final String id;
    private final float score;
    private final int position;
    // Coordinator-only: this candidate's leg-search Lucene explanation, captured when explain is requested.
    // Null when explain was not requested (the common path) or for a candidate produced purely by fusion.
    private final Explanation explanation;
    // Coordinator-only: doc-value field values captured from the leg's query-phase hit, for a transformer
    // that injected a docvalue_field onto the leg and reads it back here (e.g. the diversify retriever's
    // vector_field ride-along). Empty for the common path. Keyed by field name; value is the raw
    // docvalue list for that field (as SearchHit.field(name).getValues() returns).
    private final Map<String, Object> fields;

    public RetrieverCandidate(String index, ShardId shardId, String id, float score, int position) {
        this(index, shardId, id, score, position, null, Collections.emptyMap());
    }

    public RetrieverCandidate(String index, ShardId shardId, String id, float score, int position, Explanation explanation) {
        this(index, shardId, id, score, position, explanation, Collections.emptyMap());
    }

    public RetrieverCandidate(
        String index,
        ShardId shardId,
        String id,
        float score,
        int position,
        Explanation explanation,
        Map<String, Object> fields
    ) {
        this.index = Objects.requireNonNull(index, "index");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.id = Objects.requireNonNull(id, "id");
        this.score = score;
        this.position = position;
        this.explanation = explanation;
        this.fields = fields == null || fields.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new java.util.HashMap<>(fields));
    }

    public String index() {
        return index;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String id() {
        return id;
    }

    public float score() {
        return score;
    }

    public int position() {
        return position;
    }

    /** This candidate's leg-search Lucene explanation, or {@code null} if explain was not requested. */
    public Explanation explanation() {
        return explanation;
    }

    /**
     * Doc-value field values captured from the leg's query-phase hit, keyed by field name; empty on the
     * common path. A transformer that injected a {@code docvalue_field} onto the leg (e.g. the
     * {@code diversify} retriever's {@code vector_field}) reads the value back here instead of issuing a
     * second fetch. The value shape matches {@code SearchHit.field(name).getValues()} (a {@code List}).
     */
    public Map<String, Object> fields() {
        return fields;
    }

    /** Convenience: the captured doc-value for a single field name, or {@code null} if not present. */
    public Object field(String name) {
        return fields.get(name);
    }

    /**
     * A copy carrying the given doc-value fields, preserving identity, score, position, and explanation.
     * Used by the leaf when it captures injected {@code docvalue_fields} off a leg hit.
     */
    public RetrieverCandidate withFields(Map<String, Object> newFields) {
        return new RetrieverCandidate(index, shardId, id, score, position, explanation, newFields);
    }

    /**
     * Project this coordinator-side candidate to the wire {@link RankDoc}, narrowing the full
     * {@link ShardId} to the {@code int} shard number the broadcast query filters on. Coordinator-only
     * state (explanation/fields/timing) is intentionally dropped here.
     * <p>
     * Package-private: only the same-package base ({@link TransformerRetrieverBuilder}/{@code CompoundRetrieverBuilder})
     * narrows candidates to {@link RankDoc} at {@code toQueryBuilder()}. A plugin reshape never needs it, so it
     * stays off the public surface (which keeps {@link RankDoc}, the wire form, out of public API).
     */
    RankDoc toRankDoc() {
        return new RankDoc(index, shardId.id(), id, score, position);
    }

    /**
     * A copy with a new score and position, preserving identity ({@code index}/{@code shardId}/{@code id}),
     * explanation, and any captured doc-value fields. Used when a fusion/reshape node re-ranks a candidate
     * without changing which document it is.
     */
    public RetrieverCandidate withScoreAndPosition(float newScore, int newPosition) {
        return new RetrieverCandidate(index, shardId, id, newScore, newPosition, explanation, fields);
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
