/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.SortField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortFieldAndFormat;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Coordinator-side, serializable companion to {@link RankDocsFieldComparatorSource} — the sort analogue of
 * {@link RankDocsQueryBuilder}. The retriever framework injects this on the final search's {@code sort}
 * when the resolved root's order is decoupled from {@code _score} (e.g. {@code pinned}) and the user gave
 * no top-level sort. It carries the whole resolved window and is broadcast to every shard; {@link #build}
 * then runs <em>on each shard</em> — where {@link QueryShardContext#getShardId()} is available — scopes
 * the window to that shard's {@code (index, shardId)}, and produces the shard-local {@link SortField}.
 * <p>
 * <b>Transport-safe by construction.</b> {@link #build} returns a <em>plain</em> {@link SortField}
 * ({@code new SortField(NAME, comparatorSource, reverse)}), not a {@code SortField} subclass:
 * {@code org.opensearch.common.lucene.Lucene#writeSortField} rejects subclasses, so a subclass would
 * throw {@code Cannot serialize SortField impl} on the coordinator↔data-node reduce path whenever a shard
 * lives on a non-coordinator node. The window rides inside the {@link RankDocsFieldComparatorSource}; the
 * emitted {@code SortField} serializes via its {@code reducedType()}/{@code missingValue()}. Mirrors
 * {@code ShardDocSortBuilder}.
 * <p>
 * Like {@link RankDocsQueryBuilder}, this is purely internal to the retriever framework: it is registered
 * so it serializes to data nodes, but it cannot be authored directly in a {@code _search} body —
 * {@link #fromXContent} always rejects, and it is only ever reconstructed on data nodes over the binary
 * {@link StreamInput} path.
 * <p>
 * <b>Set only on the uncommon path.</b> This sort is injected only when the resolved order is decoupled
 * from {@code _score} (pinning, reranking, explicit positions); the common retriever case needs only
 * {@link RankDocsQueryBuilder} because the score order already reproduces the ranking. When it is added,
 * the same window is serialized twice in the request (once under {@code query.rank_docs}, once under
 * {@code sort.rank_docs_sort}) and broadcast per shard. That duplication is accepted, not optimized: it
 * occurs only on the decoupled-order path and the window is bounded by the caller's
 * {@code from + size} / {@code rank_window_size} (these builders do not re-cap it).
 *
 * @opensearch.internal
 */
public final class RankDocsSortBuilder extends SortBuilder<RankDocsSortBuilder> {

    public static final String NAME = "rank_docs_sort";

    private final List<RankDoc> rankDocs;

    public RankDocsSortBuilder(List<RankDoc> rankDocs) {
        // The retriever executor sets an immutable resolved window; nothing mutates it after, so no copy.
        this.rankDocs = Objects.requireNonNull(rankDocs, "rankDocs");
    }

    public RankDocsSortBuilder(StreamInput in) throws IOException {
        this.rankDocs = in.readList(RankDoc::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(rankDocs);
    }

    /** The resolved window this sort carries. Package-visible for tests. */
    List<RankDoc> rankDocs() {
        return rankDocs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(NAME);
        for (RankDoc rd : rankDocs) {
            rd.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Always rejects: {@code rank_docs_sort} is internal to the retriever framework and cannot be authored
     * in a {@code _search} body. It is only ever built in-process on the coordinator and transported to data
     * nodes over the binary {@link StreamInput}/{@link StreamOutput} path (see {@link #RankDocsSortBuilder(StreamInput)}),
     * never parsed from XContent. This method exists solely to satisfy the {@code SortSpec} registration
     * contract, which requires a parser.
     */
    public static RankDocsSortBuilder fromXContent(XContentParser parser, String fieldName) {
        throw new ParsingException(
            parser.getTokenLocation(),
            "[" + NAME + "] sort is internal to the retriever framework and cannot be used directly in a search request"
        );
    }

    @Override
    protected SortFieldAndFormat build(QueryShardContext context) {
        final String indexName = context.index().getName();
        final int shardId = context.getShardId();
        // Scope the broadcast window to this shard HERE (shard id is only available on the shard), so the
        // comparator source only carries — and only resolves — the docs that can exist on this shard.
        // Emit a PLAIN SortField (not a subclass) so it survives the reduce-path serialization.
        final RankDocsFieldComparatorSource comparatorSource = new RankDocsFieldComparatorSource(
            RankDocsQueryBuilder.filterToShard(rankDocs, indexName, shardId),
            indexName,
            shardId
        );
        return new SortFieldAndFormat(new SortField(NAME, comparatorSource, false), DocValueFormat.RAW);
    }

    @Override
    public BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra) {
        throw new UnsupportedOperationException("bucketed sort not supported for " + NAME);
    }

    @Override
    public RankDocsSortBuilder rewrite(QueryRewriteContext ctx) {
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return rankDocs.equals(((RankDocsSortBuilder) o).rankDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankDocs);
    }
}
