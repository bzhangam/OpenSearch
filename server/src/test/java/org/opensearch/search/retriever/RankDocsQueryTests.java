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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Lucene-level unit tests for {@link RankDocsQuery}: score replay, boost, docId ordering, missing/deleted
 * doc omission, empty window, multi-segment resolution, {@code explain} rejection, cacheability, and
 * equals/hashCode. The position-sort path is covered by {@code RankDocsFieldComparatorSourceTests}.
 */
public class RankDocsQueryTests extends OpenSearchTestCase {

    private static final String INDEX = "products";
    private static final int SHARD = 0;

    /** Index {@code ids} as documents whose only field is the encoded {@code _id}, one doc per id. */
    private static void indexIds(RandomIndexWriter w, List<String> ids) throws IOException {
        for (String id : ids) {
            Document doc = new Document();
            doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
            w.addDocument(doc);
        }
    }

    private static RankDoc doc(String id, float score, int position) {
        return new RankDoc(INDEX, SHARD, id, score, position);
    }

    /** score for the hit whose stored _id matches rd, or -1 if the hit isn't rd. */
    private static float scoreOfId(IndexSearcher searcher, TopDocs td, String id) throws IOException {
        int wanted = docIdOf(searcher, id);
        for (ScoreDoc sd : td.scoreDocs) {
            if (sd.doc == wanted) {
                return sd.score;
            }
        }
        return -1f;
    }

    private static int docIdOf(IndexSearcher searcher, String id) throws IOException {
        TopDocs td = searcher.search(new org.apache.lucene.search.TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(id))), 1);
        return td.scoreDocs.length > 0 ? td.scoreDocs[0].doc : -1;
    }

    public void testScoreReplay() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c", "d", "e"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.95f, 0), doc("c", 0.82f, 1), doc("e", 0.40f, 2));
            TopDocs td = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10);
            assertEquals(3, td.totalHits.value());
            assertEquals(0.95f, scoreOfId(searcher, td, "a"), 0.0f);
            assertEquals(0.82f, scoreOfId(searcher, td, "c"), 0.0f);
            assertEquals(0.40f, scoreOfId(searcher, td, "e"), 0.0f);
            reader.close();
        }
    }

    public void testBoostMultipliesScore() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.5f, 0));
            RankDocsQuery q = new RankDocsQuery(window, INDEX, SHARD);
            Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE, 2.0f);
            LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
            var supplier = weight.scorerSupplier(leaf);
            var scorer = supplier.get(Long.MAX_VALUE);
            scorer.iterator().nextDoc();
            assertEquals(1.0f, scorer.score(), 1e-6f); // 0.5 * 2.0
            reader.close();
        }
    }

    public void testOrderingByDocIdWhenWindowOrderDiffers() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            // docIds assigned in insertion order: a=0, b=1, c=2
            indexIds(w, List.of("a", "b", "c"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // Window (ranking) order is c, a, b — unrelated to docId order. Scorer must still walk
            // forward-only by ascending docId and attach the right score to each doc.
            List<RankDoc> window = List.of(doc("c", 0.3f, 0), doc("a", 0.9f, 1), doc("b", 0.6f, 2));
            TopDocs td = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10);
            assertEquals(3, td.totalHits.value());
            assertEquals(0.9f, scoreOfId(searcher, td, "a"), 0.0f);
            assertEquals(0.6f, scoreOfId(searcher, td, "b"), 0.0f);
            assertEquals(0.3f, scoreOfId(searcher, td, "c"), 0.0f);
            reader.close();
        }
    }

    public void testMissingIdDropsOut() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.9f, 0), doc("missing", 0.8f, 1), doc("b", 0.7f, 2), doc("zzz", 0.6f, 3));
            TopDocs td = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10);
            assertEquals(2, td.totalHits.value());
            reader.close();
        }
    }

    public void testDeletedDocDropsOut() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c"));
            w.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId("b")));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.9f, 0), doc("b", 0.8f, 1), doc("c", 0.7f, 2));
            TopDocs td = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10);
            assertEquals("deleted b must drop out", 2, td.totalHits.value());
            reader.close();
        }
    }

    public void testEmptyWindowMatchesNothing() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            TopDocs td = searcher.search(new RankDocsQuery(List.of(), INDEX, SHARD), 10);
            assertEquals(0, td.totalHits.value());
            reader.close();
        }
    }

    public void testScorerSupplierNullOnEmptySegmentMatch() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // Window references only ids not present -> nothing resolves in the segment -> null supplier.
            RankDocsQuery q = new RankDocsQuery(List.of(doc("nope", 0.5f, 0)), INDEX, SHARD);
            Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
            LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
            assertNull(weight.scorerSupplier(leaf));
            reader.close();
        }
    }

    public void testMultiSegmentResolution() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            // Force >= 2 segments by committing between batches.
            indexIds(w, List.of("a", "b"));
            w.commit();
            indexIds(w, List.of("c", "d"));
            w.commit();
            IndexReader reader = w.getReader();
            w.close();
            assertTrue("expected multiple segments", reader.leaves().size() >= 2);
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.9f, 0), doc("d", 0.6f, 1));
            TopDocs td = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10);
            assertEquals(2, td.totalHits.value());
            assertEquals(0.9f, scoreOfId(searcher, td, "a"), 0.0f);
            assertEquals(0.6f, scoreOfId(searcher, td, "d"), 0.0f);
            reader.close();
        }
    }

    public void testExplainIsUnsupported() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            RankDocsQuery q = new RankDocsQuery(List.of(doc("a", 0.9f, 0)), INDEX, SHARD);
            Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
            LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
            expectThrows(UnsupportedOperationException.class, () -> weight.explain(leaf, 0));
            reader.close();
        }
    }

    public void testNotCacheable() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            RankDocsQuery q = new RankDocsQuery(List.of(doc("a", 0.9f, 0)), INDEX, SHARD);
            Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
            LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
            assertFalse(weight.isCacheable(leaf));
            reader.close();
        }
    }

    public void testEqualsAndHashCode() {
        List<RankDoc> window = List.of(doc("a", 0.9f, 0), doc("b", 0.5f, 1));
        RankDocsQuery base = new RankDocsQuery(window, INDEX, SHARD);
        assertEquals(base, new RankDocsQuery(window, INDEX, SHARD));
        assertEquals(base.hashCode(), new RankDocsQuery(window, INDEX, SHARD).hashCode());

        assertNotEquals(base, new RankDocsQuery(List.of(doc("a", 0.9f, 0)), INDEX, SHARD));
        assertNotEquals(base, new RankDocsQuery(window, "reviews", SHARD));
        assertNotEquals(base, new RankDocsQuery(window, INDEX, 1));
    }
}
