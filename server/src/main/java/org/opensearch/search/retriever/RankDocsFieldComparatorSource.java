/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The comparator source behind the {@code rank_docs_sort} sort. It orders documents by their
 * retriever-assigned {@code position} whenever the retriever's desired order is decoupled from
 * {@code _score} — i.e. the final ranking cannot be reproduced by sorting on score (pinning, reranking,
 * or any retriever that assigns an explicit position independent of the score it reports).
 * <p>
 * <b>Why a comparator source and not a {@code SortField} subclass.</b> The per-shard {@code SortField}
 * is serialized on the coordinator↔data-node reduce path, and
 * {@code org.opensearch.common.lucene.Lucene#writeSortField} rejects any {@code SortField} subclass
 * ({@code Cannot serialize SortField impl}). By carrying the window in an
 * {@link IndexFieldData.XFieldComparatorSource} and emitting a <em>plain</em> {@link SortField}
 * ({@code new SortField(name, comparatorSource, reverse)} — see {@code RankDocsSortBuilder#build}), the
 * sort survives transport: {@code writeSortField} serializes it via {@link #reducedType()} and
 * {@link #missingValue(boolean)}, and the receiving node rebuilds a plain {@code INT} {@code SortField}
 * to merge the already-sorted per-shard positions. This mirrors the {@code _shard_doc}
 * {@code ShardDocFieldComparatorSource} pattern.
 * <p>
 * The value being sorted on — {@code position} — does not exist in the index; it is a per-request number
 * the coordinator computed. The only identity stable across the fresh readers of the final search is
 * {@code _id}, so the comparator resolves the window's {@code _id}s to <em>this segment's</em> Lucene doc
 * ids <b>once</b> per segment in {@link FieldComparator#getLeafComparator(LeafReaderContext)} — building
 * a {@code docId -> position} lookup via {@link RankDocsResolver} — after which every comparison is an
 * O(1) array lookup. This is deliberately independent of {@link RankDocsQuery}'s own {@code _id} seek.
 * <p>
 * Documents that are not part of the window sort <em>after</em> all positioned docs (sentinel position
 * {@link #UNPINNED_POSITION}).
 *
 * @opensearch.internal
 */
public final class RankDocsFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    /** Position assigned to any doc not present in the retriever window (sorts last). */
    static final int UNPINNED_POSITION = Integer.MAX_VALUE;

    private final List<RankDoc> rankDocs;
    private final String indexName;
    private final int shardId;

    public RankDocsFieldComparatorSource(List<RankDoc> rankDocs, String indexName, int shardId) {
        // No fielddata missing value: unpinned docs are handled inside the comparator via UNPINNED_POSITION,
        // so the coordinator needs no missing-value hint (matches ShardDocFieldComparatorSource).
        super(null, MultiValueMode.MIN, null);
        // Already scoped to this shard and made immutable by RankDocsSortBuilder#build; store the reference.
        this.rankDocs = Objects.requireNonNull(rankDocs, "rankDocs");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.shardId = shardId;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.INT;
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new UnsupportedOperationException("bucketed sort not supported for " + RankDocsSortBuilder.NAME);
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
        return new FieldComparator<Integer>() {
            private final int[] values = new int[numHits];
            private int bottom;
            private int topValue;

            @Override
            public int compare(int slot1, int slot2) {
                return Integer.compare(values[slot1], values[slot2]);
            }

            @Override
            public void setTopValue(Integer value) {
                this.topValue = value;
            }

            @Override
            public Integer value(int slot) {
                return values[slot];
            }

            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                // Resolve the window to this segment's doc ids ONCE, then serve O(1) lookups from an
                // O(window)-sized map (NOT an O(maxDoc) dense array): the window (10s-100s) is tiny next
                // to maxDoc (millions), so a dense array would cost O(maxDoc) alloc+fill on every query.
                final RankDocsResolver.Resolved resolved = RankDocsResolver.resolve(context, rankDocs);
                final DocIdToPosition positions = new DocIdToPosition(resolved.size());
                for (int i = 0; i < resolved.size(); i++) {
                    positions.put(resolved.docIds[i], resolved.docs[i].position());
                }

                return new LeafFieldComparator() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void setBottom(int slot) {
                        bottom = values[slot];
                    }

                    @Override
                    public int compareBottom(int doc) {
                        return Integer.compare(bottom, positions.getOrDefault(doc));
                    }

                    @Override
                    public void copy(int slot, int doc) {
                        values[slot] = positions.getOrDefault(doc);
                    }

                    @Override
                    public int compareTop(int doc) {
                        return Integer.compare(topValue, positions.getOrDefault(doc));
                    }
                };
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RankDocsFieldComparatorSource other = (RankDocsFieldComparatorSource) o;
        return shardId == other.shardId && indexName.equals(other.indexName) && rankDocs.equals(other.rankDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankDocs, indexName, shardId);
    }

    /**
     * A minimal primitive {@code int -> int} open-addressing hash map sized to the window, returning
     * {@link #UNPINNED_POSITION} for absent keys. Avoids the boxing of {@code HashMap<Integer,Integer>}
     * and the O(maxDoc) footprint of a dense array — footprint and build cost are O(window).
     */
    static final class DocIdToPosition {
        private final int[] keys;
        private final int[] vals;
        private final int mask;
        private static final int EMPTY = -1;

        DocIdToPosition(int expected) {
            // Next power of two >= expected / 0.6 (load factor ~0.6), min 16.
            int cap = 16;
            final int target = Math.max(1, (int) (expected / 0.6f) + 1);
            while (cap < target) {
                cap <<= 1;
            }
            this.keys = new int[cap];
            this.vals = new int[cap];
            this.mask = cap - 1;
            Arrays.fill(keys, EMPTY); // O(window), not O(maxDoc)
        }

        void put(int key, int value) {
            int i = hash(key) & mask;
            while (keys[i] != EMPTY && keys[i] != key) {
                i = (i + 1) & mask;
            }
            keys[i] = key;
            vals[i] = value;
        }

        int getOrDefault(int key) {
            int i = hash(key) & mask;
            while (keys[i] != EMPTY) {
                if (keys[i] == key) {
                    return vals[i];
                }
                i = (i + 1) & mask;
            }
            return UNPINNED_POSITION;
        }

        private static int hash(int key) {
            // Fibonacci-style mixing to spread sequential doc ids.
            int h = key * 0x9E3779B1;
            return h ^ (h >>> 16);
        }
    }
}
