/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization + contract tests for {@link RankDocsQueryBuilder}. Extends
 * {@link AbstractWireSerializingTestCase} so {@code testSerialization} exercises the real
 * {@code writeTo}/{@code readFrom} round-trip the coordinator→data-node hop uses — not a hand-rolled
 * one (the review on #22842 called out hand-rolled round-trips that bypassed base classes). The
 * {@code rank_docs} query is internal-only, so {@link RankDocsQueryBuilder#fromXContent} must always
 * reject.
 */
public class RankDocsQueryBuilderTests extends AbstractWireSerializingTestCase<RankDocsQueryBuilder> {

    private static List<RankDoc> randomWindow() {
        int n = randomIntBetween(0, 8);
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
    protected RankDocsQueryBuilder createTestInstance() {
        RankDocsQueryBuilder b = new RankDocsQueryBuilder(randomWindow());
        if (randomBoolean()) {
            b.boost(Math.abs(randomFloat()) * 3f);
        }
        if (randomBoolean()) {
            b.queryName(randomAlphaOfLengthBetween(1, 8));
        }
        return b;
    }

    @Override
    protected Writeable.Reader<RankDocsQueryBuilder> instanceReader() {
        return RankDocsQueryBuilder::new;
    }

    public void testFromXContentAlwaysRejects() throws IOException {
        String json = "{\"rank_docs\":{\"docs\":[{\"index\":\"products\",\"shard\":0,\"id\":\"a\",\"score\":0.9,\"position\":0}]}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME rank_docs
            parser.nextToken(); // START_OBJECT (body)
            ParsingException e = expectThrows(ParsingException.class, () -> RankDocsQueryBuilder.fromXContent(parser));
            assertTrue(
                "message should name the query and explain it is internal, got: " + e.getMessage(),
                e.getMessage().contains(RankDocsQueryBuilder.NAME) && e.getMessage().contains("internal")
            );
        }
    }

    public void testGetWriteableName() {
        assertEquals("rank_docs", new RankDocsQueryBuilder(List.of()).getWriteableName());
    }

    public void testFilterToShardMatchesOnlyIndexAndShard() {
        List<RankDoc> window = List.of(
            new RankDoc("products", 0, "a", 0.9f, 0),
            new RankDoc("products", 1, "b", 0.8f, 1), // wrong shard
            new RankDoc("reviews", 0, "c", 0.7f, 2),   // wrong index
            new RankDoc("products", 0, "d", 0.6f, 3)
        );
        List<RankDoc> scoped = RankDocsQueryBuilder.filterToShard(window, "products", 0);
        assertEquals(2, scoped.size());
        assertEquals("a", scoped.get(0).id());
        assertEquals("d", scoped.get(1).id()); // order preserved
    }

    public void testFilterToShardEmptyWhenNoneMatch() {
        List<RankDoc> window = List.of(new RankDoc("products", 1, "b", 0.8f, 0));
        assertTrue(RankDocsQueryBuilder.filterToShard(window, "products", 0).isEmpty());
    }

    public void testDoEqualsTracksWindow() {
        RankDocsQueryBuilder a = new RankDocsQueryBuilder(List.of(new RankDoc("products", 0, "a", 0.9f, 0)));
        RankDocsQueryBuilder b = new RankDocsQueryBuilder(List.of(new RankDoc("products", 0, "a", 0.9f, 0)));
        RankDocsQueryBuilder c = new RankDocsQueryBuilder(List.of(new RankDoc("products", 0, "b", 0.9f, 0)));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
