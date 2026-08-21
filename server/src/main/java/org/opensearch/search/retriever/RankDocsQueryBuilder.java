/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Internal-only {@link AbstractQueryBuilder} that carries pre-computed document IDs and scores
 * from retriever fusion to data node shards. Not registered in the named query registry for XContent
 * parsing (it is never user-facing), but IS registered as a {@code NamedWriteable} so it can be
 * serialized to remote shards during the query phase.
 * <p>
 * The full fused result set is broadcast to every shard; {@link #doToQuery} filters the docs down to
 * the ones belonging to that shard's index (by index name), which both prevents cross-index
 * {@code _id} collisions and avoids seeking for docs that live in other indices.
 *
 * @opensearch.internal
 */
public class RankDocsQueryBuilder extends AbstractQueryBuilder<RankDocsQueryBuilder> {

    public static final String NAME = "rank_docs_internal";

    private final List<RankedDoc> rankedDocs;

    /**
     * Construct from a list of ranked docs for a single shard.
     */
    public RankDocsQueryBuilder(List<RankedDoc> rankedDocs) {
        this.rankedDocs = rankedDocs != null ? rankedDocs : List.of();
    }

    /**
     * Read from stream.
     */
    public RankDocsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        this.rankedDocs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            this.rankedDocs.add(new RankedDoc(in));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(rankedDocs.size());
        for (RankedDoc doc : rankedDocs) {
            doc.writeTo(out);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (rankedDocs.isEmpty()) {
            return new MatchNoDocsQuery("no retriever results on this shard");
        }
        // Multi-index correctness: this query builder carries the full fused result set and is
        // broadcast to every shard. _id is only unique *within* an index, so on a shard of index A
        // we must match ONLY the fused docs that actually belong to index A — otherwise a doc from
        // index B that happens to share an _id with a doc in index A would be matched and assigned
        // the wrong (index B's) score. Filtering by the shard's index here also eliminates wasted
        // _id seeks for docs that belong to other indices.
        final String shardIndex = context.index().getName();
        List<RankedDoc> forThisIndex = new ArrayList<>(rankedDocs.size());
        for (RankedDoc doc : rankedDocs) {
            if (shardIndex.equals(doc.index())) {
                forThisIndex.add(doc);
            }
        }
        if (forThisIndex.isEmpty()) {
            return new MatchNoDocsQuery("no retriever results for index [" + shardIndex + "] on this shard");
        }
        String[] docIds = new String[forThisIndex.size()];
        float[] scores = new float[forThisIndex.size()];
        for (int i = 0; i < forThisIndex.size(); i++) {
            docIds[i] = forThisIndex.get(i).id();
            scores[i] = forThisIndex.get(i).score();
        }
        return new RankDocsQuery(docIds, scores);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("[" + NAME + "] is internal-only and cannot be serialized to XContent");
    }

    @Override
    protected boolean doEquals(RankDocsQueryBuilder other) {
        return Objects.equals(this.rankedDocs, other.rankedDocs);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rankedDocs);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public List<RankedDoc> getRankedDocs() {
        return rankedDocs;
    }
}
