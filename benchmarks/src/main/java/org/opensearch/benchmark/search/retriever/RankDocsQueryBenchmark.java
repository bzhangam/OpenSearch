/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.retriever;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.retriever.RankDoc;
import org.opensearch.search.retriever.RankDocsFieldComparatorSource;
import org.opensearch.search.retriever.RankDocsQuery;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmark for the {@code rank_docs} replay paths over a real on-disk Lucene index. Measures a
 * full {@link IndexSearcher#search} and compares:
 * <ul>
 *   <li><b>rankDocs</b> — {@link RankDocsQuery}: re-identify each windowed doc by seeking its {@code _id}
 *       in the {@code _id} terms dictionary, per segment (the current implementation).</li>
 *   <li><b>boolConstantScore</b> — the alternative baseline the custom query replaces: a {@code bool.should}
 *       of {@code ConstantScore(TermQuery(_id))} clauses. {@code rankDocs} MUST beat this.</li>
 *   <li><b>rankDocsSortByPosition</b> — the position sort ({@link RankDocsFieldComparatorSource}); also does
 *       a per-segment {@code _id} seek. Reported for the sort-path characterization (uncommon path).</li>
 * </ul>
 * Vary {@code rankWindowSize} and {@code segments}. Lower µs/op is better.
 *
 * Run: {@code ./gradlew -p benchmarks run -Pbenchmark=RankDocsQueryBenchmark} (or the JMH uberjar).
 */
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class RankDocsQueryBenchmark {

    private static final String INDEX = "products";
    private static final int SHARD = 0;

    @Param({ "100", "500", "5000" })
    public int rankWindowSize;

    @Param({ "1", "8" })
    public int segments;

    private static final int NUM_DOCS = 1_000_000;

    private Path dir;
    private FSDirectory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    private RankDocsQuery rankDocsQuery;
    private BooleanQuery boolConstantScoreQuery;
    private Sort positionSort;

    @Setup
    public void setup() throws IOException {
        dir = Files.createTempDirectory("rankdocs-bench");
        directory = FSDirectory.open(dir);

        IndexWriterConfig cfg = new IndexWriterConfig(new KeywordAnalyzer());
        // Control segment count deterministically: flush NUM_DOCS/segments docs per commit.
        try (IndexWriter writer = new IndexWriter(directory, cfg)) {
            int perSegment = NUM_DOCS / segments;
            int written = 0;
            for (int s = 0; s < segments; s++) {
                int end = (s == segments - 1) ? NUM_DOCS : written + perSegment;
                for (; written < end; written++) {
                    Document doc = new Document();
                    doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(written)), IdFieldMapper.Defaults.FIELD_TYPE));
                    writer.addDocument(doc);
                }
                writer.commit();
                writer.flush();
            }
            writer.forceMerge(segments, true);
        }

        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // Build a window of rankWindowSize ids spread pseudo-randomly across the id space.
        Random r = new Random(42);
        List<RankDoc> window = new ArrayList<>(rankWindowSize);
        BooleanQuery.Builder bool = new BooleanQuery.Builder();
        for (int i = 0; i < rankWindowSize; i++) {
            String id = Integer.toString(r.nextInt(NUM_DOCS));
            float score = 0.001f + r.nextFloat();
            window.add(new RankDoc(INDEX, SHARD, id, score, i));
            TermQuery tq = new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(id)));
            bool.add(new BoostQuery(new ConstantScoreQuery(tq), score), BooleanClause.Occur.SHOULD);
        }
        rankDocsQuery = new RankDocsQuery(window, INDEX, SHARD);
        boolConstantScoreQuery = bool.build();
        positionSort = new Sort(new SortField(INDEX, new RankDocsFieldComparatorSource(window, INDEX, SHARD), false));
    }

    @TearDown
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
        // Best-effort cleanup of the temp index directory.
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {}
            });
        }
    }

    @Benchmark
    public TopDocs rankDocs() throws IOException {
        return searcher.search(rankDocsQuery, rankWindowSize);
    }

    @Benchmark
    public TopDocs boolConstantScore() throws IOException {
        return searcher.search(boolConstantScoreQuery, rankWindowSize);
    }

    @Benchmark
    public TopDocs rankDocsSortByPosition() throws IOException {
        return searcher.search(rankDocsQuery, rankWindowSize, positionSort);
    }
}
