/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link RankDocsQuery} and {@link RankDocsQueryBuilder}.
 */
public class RankDocsQueryTests extends OpenSearchTestCase {

    // === RankDocsQuery Lucene query tests ===

    public void testRankDocsQueryMatchesCorrectDocs() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);

        // Index documents with _id field
        addDoc(writer, "doc1");
        addDoc(writer, "doc2");
        addDoc(writer, "doc3");
        addDoc(writer, "doc4");
        addDoc(writer, "doc5");
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Query for doc2 and doc4
        RankDocsQuery query = new RankDocsQuery(
            new String[]{"doc2", "doc4"},
            new float[]{0.9f, 0.7f}
        );

        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(2, topDocs.totalHits.value());

        reader.close();
        dir.close();
    }

    public void testRankDocsQueryReturnsCorrectScores() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);

        addDoc(writer, "alpha");
        addDoc(writer, "beta");
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        RankDocsQuery query = new RankDocsQuery(
            new String[]{"alpha", "beta"},
            new float[]{0.95f, 0.85f}
        );

        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(2, topDocs.totalHits.value());

        // Verify scores are the pre-computed ones (order by lucene doc id, not score)
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue(scoreDoc.score == 0.95f || scoreDoc.score == 0.85f);
        }

        reader.close();
        dir.close();
    }

    public void testRankDocsQueryMissesNonexistentDocs() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);

        addDoc(writer, "existing");
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Query includes a doc that doesn't exist
        RankDocsQuery query = new RankDocsQuery(
            new String[]{"existing", "nonexistent"},
            new float[]{0.9f, 0.5f}
        );

        TopDocs topDocs = searcher.search(query, 10);
        // Only "existing" should match
        assertEquals(1, topDocs.totalHits.value());

        reader.close();
        dir.close();
    }

    public void testRankDocsQueryEmptyInput() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);
        addDoc(writer, "doc1");
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        RankDocsQuery query = new RankDocsQuery(new String[]{}, new float[]{});
        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(0, topDocs.totalHits.value());

        reader.close();
        dir.close();
    }

    public void testRankDocsQueryMismatchedArraysThrows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new RankDocsQuery(new String[]{"doc1"}, new float[]{0.5f, 0.6f})
        );
    }

    public void testRankDocsQueryEqualityAndHashCode() {
        RankDocsQuery q1 = new RankDocsQuery(new String[]{"a", "b"}, new float[]{1.0f, 2.0f});
        RankDocsQuery q2 = new RankDocsQuery(new String[]{"a", "b"}, new float[]{1.0f, 2.0f});
        RankDocsQuery q3 = new RankDocsQuery(new String[]{"a", "c"}, new float[]{1.0f, 2.0f});

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, q3);
    }

    public void testRankDocsQueryToString() {
        RankDocsQuery query = new RankDocsQuery(new String[]{"a", "b", "c"}, new float[]{1f, 2f, 3f});
        String str = query.toString("field");
        assertTrue(str.contains("RankDocsQuery"));
        assertTrue(str.contains("docCount=3"));
    }

    // === RankDocsQueryBuilder tests ===

    public void testRankDocsQueryBuilderSerialization() throws IOException {
        ShardId shardId = new ShardId(new Index("products", "uuid1"), 0);
        List<RankedDoc> docs = new ArrayList<>();
        docs.add(new RankedDoc("doc1", "products", shardId, 0.9f, 0));
        docs.add(new RankedDoc("doc2", "products", shardId, 0.8f, 1));

        RankDocsQueryBuilder original = new RankDocsQueryBuilder(docs);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RankDocsQueryBuilder deserialized = new RankDocsQueryBuilder(in);

        assertEquals(original.getRankedDocs().size(), deserialized.getRankedDocs().size());
        assertEquals(original.getRankedDocs().get(0), deserialized.getRankedDocs().get(0));
        assertEquals(original.getRankedDocs().get(1), deserialized.getRankedDocs().get(1));
    }

    public void testRankDocsQueryBuilderRetainsAllDocsForBroadcast() {
        // The fused result set is broadcast to every shard as-is; doToQuery(QueryShardContext) filters
        // it down to the shard's own index at execution time (verified by integration tests, which
        // need a real QueryShardContext). Here we just assert the builder preserves the full,
        // multi-index result set for that broadcast.
        ShardId shard0 = new ShardId(new Index("products", "uuid"), 0);
        ShardId shard1 = new ShardId(new Index("products", "uuid"), 1);
        ShardId reviewsShard = new ShardId(new Index("reviews", "uuid2"), 0);

        List<RankedDoc> allDocs = new ArrayList<>();
        allDocs.add(new RankedDoc("doc_a", "products", shard0, 0.95f, 0));
        allDocs.add(new RankedDoc("doc_b", "reviews", reviewsShard, 0.91f, 1));
        allDocs.add(new RankedDoc("doc_c", "products", shard1, 0.82f, 2));
        allDocs.add(new RankedDoc("doc_d", "reviews", reviewsShard, 0.78f, 3));

        RankDocsQueryBuilder builder = new RankDocsQueryBuilder(allDocs);
        assertEquals(4, builder.getRankedDocs().size());
        // Both indices are represented in the single broadcast builder.
        assertTrue(builder.getRankedDocs().stream().anyMatch(d -> d.index().equals("products")));
        assertTrue(builder.getRankedDocs().stream().anyMatch(d -> d.index().equals("reviews")));
    }

    public void testRankDocsQueryBuilderXContentThrows() throws Exception {
        RankDocsQueryBuilder builder = new RankDocsQueryBuilder(List.of());
        XContentBuilder xContent = XContentFactory.jsonBuilder();
        expectThrows(UnsupportedOperationException.class, () -> builder.toXContent(xContent, ToXContent.EMPTY_PARAMS));
    }

    public void testRankDocsQueryBuilderEmptyListProducesNoMatch() throws IOException {
        RankDocsQueryBuilder builder = new RankDocsQueryBuilder(List.of());
        assertEquals("rank_docs_internal", builder.getWriteableName());
    }

    public void testRankDocsQueryBuilderEquality() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        List<RankedDoc> docs1 = List.of(new RankedDoc("a", "idx", shardId, 1.0f, 0));
        List<RankedDoc> docs2 = List.of(new RankedDoc("a", "idx", shardId, 1.0f, 0));
        List<RankedDoc> docs3 = List.of(new RankedDoc("b", "idx", shardId, 1.0f, 0));

        RankDocsQueryBuilder b1 = new RankDocsQueryBuilder(docs1);
        RankDocsQueryBuilder b2 = new RankDocsQueryBuilder(docs2);
        RankDocsQueryBuilder b3 = new RankDocsQueryBuilder(docs3);

        assertEquals(b1, b2);
        assertNotEquals(b1, b3);
    }

    // === Helper ===

    private void addDoc(IndexWriter writer, String id) throws IOException {
        Document doc = new Document();
        // Store _id as encoded bytes, matching how OpenSearch indexes the _id field
        BytesRef encodedId = Uid.encodeId(id);
        doc.add(new StringField("_id", encodedId, Field.Store.YES));
        writer.addDocument(doc);
    }
}
