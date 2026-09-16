/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.retriever.RankDocsFieldComparatorSource.DocIdToPosition;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene-level tests for {@link RankDocsFieldComparatorSource}: position ordering (decoupled from score
 * and docId order), unpinned docs sorting last, {@code reducedType}/{@code missingValue}/{@code
 * newBucketedSort} contract, multi-segment resolution, and the {@link DocIdToPosition} primitive map.
 */
public class RankDocsFieldComparatorSourceTests extends OpenSearchTestCase {

    private static final String INDEX = "products";
    private static final int SHARD = 0;
    private static final String STORED_ID = "stored_id";

    /** Index each id as one doc: the encoded _id field (for the seek) plus a stored copy (to read back). */
    private static void indexIds(RandomIndexWriter w, List<String> ids) throws IOException {
        for (String id : ids) {
            Document doc = new Document();
            doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
            doc.add(new StringField(STORED_ID, id, Field.Store.YES));
            w.addDocument(doc);
        }
    }

    private static RankDoc doc(String id, float score, int position) {
        return new RankDoc(INDEX, SHARD, id, score, position);
    }

    private static SortField sortField(List<RankDoc> window) {
        return new SortField(RankDocsSortBuilder.NAME, new RankDocsFieldComparatorSource(window, INDEX, SHARD), false);
    }

    /** The stored _id string of a returned hit. */
    private static String idOfHit(IndexSearcher searcher, int docId) throws IOException {
        return searcher.storedFields().document(docId).get(STORED_ID);
    }

    public void testOrdersByPositionNotScoreOrDocId() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c")); // docIds a=0,b=1,c=2
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // Positions inverted vs both score and docId order: c(pos0) < b(pos1) < a(pos2).
            List<RankDoc> window = List.of(doc("a", 0.90f, 2), doc("b", 0.80f, 1), doc("c", 0.10f, 0));
            TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, new Sort(sortField(window)));
            assertEquals(3, td.totalHits.value());

            assertEquals("c", idOfHit(searcher, td.scoreDocs[0].doc)); // position 0
            assertEquals("b", idOfHit(searcher, td.scoreDocs[1].doc)); // position 1
            assertEquals("a", idOfHit(searcher, td.scoreDocs[2].doc)); // position 2
            reader.close();
        }
    }

    public void testUnpinnedDocsSortLast() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c", "d"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // Only b and d are in the window; a and c are unpinned -> must come after b and d.
            List<RankDoc> window = List.of(doc("d", 0.5f, 0), doc("b", 0.4f, 1));
            TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, new Sort(sortField(window)));
            assertEquals(4, td.totalHits.value());

            assertEquals("d", idOfHit(searcher, td.scoreDocs[0].doc));
            assertEquals("b", idOfHit(searcher, td.scoreDocs[1].doc));
            // last two are the unpinned a and c, in either order
            List<String> tail = List.of(idOfHit(searcher, td.scoreDocs[2].doc), idOfHit(searcher, td.scoreDocs[3].doc));
            assertTrue("unpinned docs must sort last", tail.contains("a") && tail.contains("c"));
            reader.close();
        }
    }

    public void testMultiSegmentPositions() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            w.commit();
            indexIds(w, List.of("c", "d"));
            w.commit();
            IndexReader reader = w.getReader();
            w.close();
            assertTrue("expected multiple segments", reader.leaves().size() >= 2);
            IndexSearcher searcher = newSearcher(reader);

            // Window spans both segments; d (seg2) pos0, a (seg1) pos1; b,c unpinned.
            List<RankDoc> window = List.of(doc("d", 0.9f, 0), doc("a", 0.8f, 1));
            TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, new Sort(sortField(window)));
            assertEquals(4, td.totalHits.value());
            assertEquals("d", idOfHit(searcher, td.scoreDocs[0].doc));
            assertEquals("a", idOfHit(searcher, td.scoreDocs[1].doc));
            reader.close();
        }
    }

    public void testReducedTypeIsInt() {
        RankDocsFieldComparatorSource src = new RankDocsFieldComparatorSource(List.of(), INDEX, SHARD);
        assertEquals(SortField.Type.INT, src.reducedType());
    }

    public void testMissingValueIsNull() {
        // We handle unpinned docs inside the comparator (UNPINNED_POSITION), so no fielddata missing-value
        // hint is serialized; the base default (null) is correct.
        RankDocsFieldComparatorSource src = new RankDocsFieldComparatorSource(List.of(), INDEX, SHARD);
        assertNull(src.missingValue(false));
        assertNull(src.missingValue(true));
    }

    public void testNewBucketedSortThrows() {
        RankDocsFieldComparatorSource src = new RankDocsFieldComparatorSource(List.of(), INDEX, SHARD);
        expectThrows(
            UnsupportedOperationException.class,
            () -> src.newBucketedSort(BigArrays.NON_RECYCLING_INSTANCE, null, DocValueFormat.RAW, 1, null)
        );
    }

    public void testEqualsAndHashCode() {
        List<RankDoc> window = List.of(doc("a", 0.9f, 0));
        RankDocsFieldComparatorSource base = new RankDocsFieldComparatorSource(window, INDEX, SHARD);
        assertEquals(base, new RankDocsFieldComparatorSource(window, INDEX, SHARD));
        assertEquals(base.hashCode(), new RankDocsFieldComparatorSource(window, INDEX, SHARD).hashCode());
        assertNotEquals(base, new RankDocsFieldComparatorSource(List.of(doc("b", 0.9f, 0)), INDEX, SHARD));
        assertNotEquals(base, new RankDocsFieldComparatorSource(window, "reviews", SHARD));
        assertNotEquals(base, new RankDocsFieldComparatorSource(window, INDEX, 1));
    }

    // ---- DocIdToPosition ----

    public void testDocIdToPositionPutGet() {
        DocIdToPosition m = new DocIdToPosition(4);
        m.put(10, 0);
        m.put(20, 1);
        m.put(30, 2);
        assertEquals(0, m.getOrDefault(10));
        assertEquals(1, m.getOrDefault(20));
        assertEquals(2, m.getOrDefault(30));
    }

    public void testDocIdToPositionAbsentKeyReturnsUnpinned() {
        DocIdToPosition m = new DocIdToPosition(2);
        m.put(5, 0);
        assertEquals(RankDocsFieldComparatorSource.UNPINNED_POSITION, m.getOrDefault(999));
    }

    public void testDocIdToPositionEmptyMapReturnsUnpinned() {
        assertEquals(RankDocsFieldComparatorSource.UNPINNED_POSITION, new DocIdToPosition(0).getOrDefault(0));
    }

    public void testDocIdToPositionManyEntriesWithCollisions() {
        // Insert far more entries than the min capacity to force resize-free open-addressing collisions.
        int n = 500;
        DocIdToPosition m = new DocIdToPosition(n);
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int key = i * 7 + 3; // spread but deterministic
            keys.add(key);
            m.put(key, i);
        }
        for (int i = 0; i < n; i++) {
            assertEquals(i, m.getOrDefault(keys.get(i)));
        }
        assertEquals(RankDocsFieldComparatorSource.UNPINNED_POSITION, m.getOrDefault(-12345));
    }

    public void testDocIdToPositionZeroPosition() {
        DocIdToPosition m = new DocIdToPosition(1);
        m.put(0, 0); // docId 0, position 0 — both zeros must round-trip, not be mistaken for empty
        assertEquals(0, m.getOrDefault(0));
    }
}
