/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.SortField;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.sort.AbstractSortTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization + contract tests for {@link RankDocsSortBuilder}. Extends {@link AbstractSortTestCase} so
 * the framework harness exercises the real serialization ({@code testSerialization}) and the build path
 * ({@code testBuildSortField}). This is the exact coverage the #22842 review flagged as missing: the
 * original hand-rolled round-trip never went through the reduce-path {@code SortField} serialization, so a
 * {@code SortField} subclass that {@link Lucene#writeSortField} rejects slipped through.
 * <p>
 * {@code rank_docs_sort} is internal-only, so {@link RankDocsSortBuilder#fromXContent} always rejects; the
 * inherited {@code testFromXContent} (which assumes an authorable sort) is overridden to assert rejection.
 */
public class RankDocsSortBuilderTests extends AbstractSortTestCase<RankDocsSortBuilder> {

    private static List<RankDoc> randomWindow() {
        int n = randomIntBetween(0, 6);
        List<RankDoc> docs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            docs.add(
                new RankDoc(
                    randomAlphaOfLengthBetween(1, 8),
                    randomIntBetween(0, 4),
                    randomAlphaOfLengthBetween(1, 8),
                    Math.abs(randomFloat()) * 10f,
                    i
                )
            );
        }
        return docs;
    }

    @Override
    protected RankDocsSortBuilder createTestItem() {
        return new RankDocsSortBuilder(randomWindow());
    }

    @Override
    protected RankDocsSortBuilder mutate(RankDocsSortBuilder original) throws IOException {
        List<RankDoc> mutated = new ArrayList<>(original.rankDocs());
        mutated.add(new RankDoc("extra-" + randomAlphaOfLength(4), 0, randomAlphaOfLength(4), 0.5f, mutated.size()));
        return new RankDocsSortBuilder(mutated);
    }

    @Override
    protected RankDocsSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return RankDocsSortBuilder.fromXContent(parser, fieldName);
    }

    /**
     * Assert {@link RankDocsSortBuilder#build} yields a transport-safe sort: a PLAIN {@link SortField}
     * (never a subclass — {@link Lucene#writeSortField} rejects subclasses) whose comparator source is a
     * {@link RankDocsFieldComparatorSource}, reduced type INT, format RAW.
     */
    @Override
    protected void sortFieldAssertions(RankDocsSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertSame("build() must emit a plain SortField, not a subclass", SortField.class, sortField.getClass());
        assertNotNull(sortField.getComparatorSource());
        assertTrue(sortField.getComparatorSource() instanceof RankDocsFieldComparatorSource);
        assertEquals(SortField.Type.INT, ((RankDocsFieldComparatorSource) sortField.getComparatorSource()).reducedType());
        assertEquals(DocValueFormat.RAW, format);
    }

    /**
     * The inherited test assumes an authorable sort; {@code rank_docs_sort} is internal-only, so instead
     * assert {@code fromXContent} rejects (message names the sort + "internal").
     */
    @Override
    public void testFromXContent() throws IOException {
        String json = "{\"rank_docs_sort\":[{\"index\":\"products\",\"shard\":0,\"id\":\"a\",\"score\":0.9,\"position\":0}]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME rank_docs_sort
            ParsingException e = expectThrows(
                ParsingException.class,
                () -> RankDocsSortBuilder.fromXContent(parser, RankDocsSortBuilder.NAME)
            );
            assertTrue(
                "message should name the sort and explain it is internal, got: " + e.getMessage(),
                e.getMessage().contains(RankDocsSortBuilder.NAME) && e.getMessage().contains("internal")
            );
        }
    }

    /**
     * The exact defect from #22842: the per-shard {@link SortField} must survive the reduce-path
     * {@link Lucene#writeSortField}/{@link Lucene#readSortField} round-trip without throwing
     * {@code Cannot serialize SortField impl}. The read-back field is a plain INT SortField (the receiving
     * node merges the already-sorted per-shard positions).
     */
    public void testBuiltSortFieldSurvivesLuceneTransport() throws IOException {
        RankDocsSortBuilder builder = new RankDocsSortBuilder(
            List.of(new RankDoc("products", 0, "a", 0.9f, 0), new RankDoc("products", 0, "b", 0.4f, 1))
        );
        SortField built = builder.build(createMockShardContext()).field;
        assertSame(SortField.class, built.getClass());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Lucene.writeSortField(out, built); // must NOT throw IllegalArgumentException
            try (StreamInput in = out.bytes().streamInput()) {
                SortField readBack = Lucene.readSortField(in);
                assertEquals(RankDocsSortBuilder.NAME, readBack.getField());
                assertEquals(SortField.Type.INT, readBack.getType());
                assertFalse(readBack.getReverse());
            }
        }
    }

    public void testBuildBucketedSortThrows() {
        RankDocsSortBuilder builder = new RankDocsSortBuilder(List.of());
        expectThrows(UnsupportedOperationException.class, () -> builder.buildBucketedSort(createMockShardContext(), 1, null));
    }

    public void testRewriteReturnsSelf() throws IOException {
        RankDocsSortBuilder builder = new RankDocsSortBuilder(List.of(new RankDoc("products", 0, "a", 0.9f, 0)));
        assertSame(builder, builder.rewrite(null));
    }

    public void testGetWriteableName() {
        assertEquals("rank_docs_sort", new RankDocsSortBuilder(List.of()).getWriteableName());
    }
}
