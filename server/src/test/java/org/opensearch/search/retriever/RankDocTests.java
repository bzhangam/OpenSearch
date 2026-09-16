/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class RankDocTests extends OpenSearchTestCase {

    private static RankDoc randomRankDoc() {
        return new RankDoc(
            randomAlphaOfLengthBetween(1, 12),
            randomIntBetween(0, 16),
            randomAlphaOfLengthBetween(1, 12),
            Math.abs(randomFloat()) * 100f,
            randomIntBetween(0, 1000)
        );
    }

    public void testWireRoundTripPreservesAllFields() throws IOException {
        RankDoc original = new RankDoc("products", 3, "abc", 0.75f, 5);
        RankDoc copy = copyOverWire(original);
        assertEquals("products", copy.index());
        assertEquals(3, copy.shardId());
        assertEquals("abc", copy.id());
        assertEquals(0.75f, copy.score(), 0.0f);
        assertEquals(5, copy.position());
        assertEquals(original, copy);
    }

    public void testRandomWireRoundTrip() throws IOException {
        for (int i = 0; i < 50; i++) {
            RankDoc original = randomRankDoc();
            assertEquals(original, copyOverWire(original));
        }
    }

    public void testToXContentRendersAllFields() throws IOException {
        RankDoc doc = new RankDoc("products", 2, "a", 0.9f, 0);
        XContentBuilder builder = JsonXContent.contentBuilder();
        doc.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
        assertEquals("products", map.get(RankDoc.INDEX_FIELD));
        assertEquals(2, map.get(RankDoc.SHARD_FIELD));
        assertEquals("a", map.get(RankDoc.ID_FIELD));
        assertEquals(0.9, ((Number) map.get(RankDoc.SCORE_FIELD)).doubleValue(), 1e-6);
        assertEquals(0, map.get(RankDoc.POSITION_FIELD));
        assertEquals(5, map.size());
    }

    public void testEqualsAndHashCode() {
        RankDoc base = new RankDoc("products", 1, "a", 0.5f, 0);
        assertEquals(base, new RankDoc("products", 1, "a", 0.5f, 0));
        assertEquals(base.hashCode(), new RankDoc("products", 1, "a", 0.5f, 0).hashCode());

        assertNotEquals(base, new RankDoc("reviews", 1, "a", 0.5f, 0));
        assertNotEquals(base, new RankDoc("products", 2, "a", 0.5f, 0));
        assertNotEquals(base, new RankDoc("products", 1, "b", 0.5f, 0));
        assertNotEquals(base, new RankDoc("products", 1, "a", 0.6f, 0));
        assertNotEquals(base, new RankDoc("products", 1, "a", 0.5f, 1));
    }

    public void testNullIndexThrows() {
        expectThrows(NullPointerException.class, () -> new RankDoc(null, 0, "a", 0.5f, 0));
    }

    public void testNullIdThrows() {
        expectThrows(NullPointerException.class, () -> new RankDoc("products", 0, null, 0.5f, 0));
    }

    public void testNegativeScoreRejectedByPublicConstructor() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RankDoc("products", 0, "a", -0.01f, 0));
        assertTrue(e.getMessage(), e.getMessage().contains("non-negative"));
    }

    public void testNaNScoreRejectedByPublicConstructor() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RankDoc("products", 0, "a", Float.NaN, 0));
        assertTrue(e.getMessage(), e.getMessage().contains("non-NaN") || e.getMessage().contains("non-negative"));
    }

    public void testZeroScoreAccepted() {
        RankDoc doc = new RankDoc("products", 0, "a", 0f, 0);
        assertEquals(0f, doc.score(), 0.0f);
    }

    public void testNegativeScoreOnWireRejected() throws IOException {
        // A hand-crafted wire message carrying a negative score must be rejected by the StreamInput ctor,
        // not silently accepted — the guard must exist on the wire path, not only the public path.
        BytesReference bytes = writeRawRankDoc("products", 0, "a", -1.0f, 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RankDoc(bytes.streamInput()));
        assertTrue(e.getMessage(), e.getMessage().contains("non-negative"));
    }

    public void testNaNScoreOnWireRejected() throws IOException {
        BytesReference bytes = writeRawRankDoc("products", 0, "a", Float.NaN, 0);
        expectThrows(IllegalArgumentException.class, () -> new RankDoc(bytes.streamInput()));
    }

    private static RankDoc copyOverWire(RankDoc original) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            return new RankDoc(out.bytes().streamInput());
        }
    }

    /**
     * Serialize the raw wire fields in {@link RankDoc#writeTo} order WITHOUT going through the guarded
     * public constructor, so we can inject an invalid score and prove the {@link RankDoc#RankDoc} wire
     * constructor rejects it.
     */
    private static BytesReference writeRawRankDoc(String index, int shardId, String id, float score, int position) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            StreamOutput streamOutput = out;
            streamOutput.writeString(index);
            streamOutput.writeVInt(shardId);
            streamOutput.writeString(id);
            streamOutput.writeFloat(score);
            streamOutput.writeVInt(position);
            return out.bytes();
        }
    }
}
