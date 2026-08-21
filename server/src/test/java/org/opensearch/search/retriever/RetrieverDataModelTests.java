/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.retriever.constraints.DisallowTrackScoresFalse;
import org.opensearch.search.retriever.modifiers.ForceTrackScores;
import org.opensearch.search.retriever.modifiers.RequireDocvalueField;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Unit tests for the retriever framework data model: RankedDoc, RetrieverContext,
 * LeafModifier and LeafConstraint implementations.
 */
public class RetrieverDataModelTests extends OpenSearchTestCase {

    // === RankedDoc tests ===

    public void testRankedDocConstructionAndGetters() {
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);
        RankedDoc doc = new RankedDoc("doc1", "test-index", shardId, 0.95f, 0);

        assertEquals("doc1", doc.id());
        assertEquals("test-index", doc.index());
        assertEquals(shardId, doc.shardId());
        assertEquals(0.95f, doc.score(), 0.0001f);
        assertEquals(0, doc.position());
    }

    public void testRankedDocConvenienceConstructor() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 1);
        RankedDoc doc = new RankedDoc("doc2", "idx", shardId, 0.8f);

        assertEquals("doc2", doc.id());
        assertEquals(0, doc.position()); // defaults to 0
    }

    public void testRankedDocEquality() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        RankedDoc doc1 = new RankedDoc("doc1", "idx", shardId, 0.5f, 1);
        RankedDoc doc2 = new RankedDoc("doc1", "idx", shardId, 0.5f, 1);
        RankedDoc doc3 = new RankedDoc("doc2", "idx", shardId, 0.5f, 1);

        assertEquals(doc1, doc2);
        assertEquals(doc1.hashCode(), doc2.hashCode());
        assertNotEquals(doc1, doc3);
    }

    public void testRankedDocSerialization() throws IOException {
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 2);
        RankedDoc original = new RankedDoc("myDoc", "test-index", shardId, 0.75f, 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RankedDoc deserialized = new RankedDoc(in);

        assertEquals(original, deserialized);
        assertEquals(original.id(), deserialized.id());
        assertEquals(original.index(), deserialized.index());
        assertEquals(original.shardId(), deserialized.shardId());
        assertEquals(original.score(), deserialized.score(), 0.0001f);
        assertEquals(original.position(), deserialized.position());
    }

    public void testRankedDocNullIdThrows() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        expectThrows(NullPointerException.class, () -> new RankedDoc(null, "idx", shardId, 0.5f));
    }

    public void testRankedDocNullIndexThrows() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        expectThrows(NullPointerException.class, () -> new RankedDoc("doc1", null, shardId, 0.5f));
    }

    public void testRankedDocNullShardIdThrows() {
        expectThrows(NullPointerException.class, () -> new RankedDoc("doc1", "idx", null, 0.5f));
    }

    // === RetrieverContext tests ===

    public void testRetrieverContextRoot() {
        RetrieverContext root = RetrieverContext.root();
        assertNotNull(root);
        assertTrue(root.getConstraints().isEmpty());
        assertTrue(root.getModifiers().isEmpty());
    }

    public void testRetrieverContextWithConstraintProducesNewInstance() {
        RetrieverContext root = RetrieverContext.root();
        LeafConstraint constraint = leaf -> {};
        RetrieverContext child = root.withConstraint(constraint);

        // Original unchanged
        assertTrue(root.getConstraints().isEmpty());
        // Child has the constraint
        assertEquals(1, child.getConstraints().size());
        assertSame(constraint, child.getConstraints().get(0));
    }

    public void testRetrieverContextWithModifierProducesNewInstance() {
        RetrieverContext root = RetrieverContext.root();
        LeafModifier modifier = leaf -> {};
        RetrieverContext child = root.withModifier(modifier);

        // Original unchanged
        assertTrue(root.getModifiers().isEmpty());
        // Child has the modifier
        assertEquals(1, child.getModifiers().size());
        assertSame(modifier, child.getModifiers().get(0));
    }

    public void testRetrieverContextAccumulatesMultipleConstraints() {
        LeafConstraint c1 = leaf -> {};
        LeafConstraint c2 = leaf -> {};

        RetrieverContext ctx = RetrieverContext.root()
            .withConstraint(c1)
            .withConstraint(c2);

        assertEquals(2, ctx.getConstraints().size());
        assertSame(c1, ctx.getConstraints().get(0));
        assertSame(c2, ctx.getConstraints().get(1));
    }

    public void testRetrieverContextAccumulatesMultipleModifiers() {
        LeafModifier m1 = leaf -> {};
        LeafModifier m2 = leaf -> {};

        RetrieverContext ctx = RetrieverContext.root()
            .withModifier(m1)
            .withModifier(m2);

        assertEquals(2, ctx.getModifiers().size());
        assertSame(m1, ctx.getModifiers().get(0));
        assertSame(m2, ctx.getModifiers().get(1));
    }

    public void testRetrieverContextImmutability() {
        RetrieverContext root = RetrieverContext.root();
        // Lists should be unmodifiable
        expectThrows(UnsupportedOperationException.class, () -> root.getConstraints().add(leaf -> {}));
        expectThrows(UnsupportedOperationException.class, () -> root.getModifiers().add(leaf -> {}));
    }

    // === ForceTrackScores modifier tests ===

    public void testForceTrackScoresApplies() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        assertNull(leaf.getTrackScores());

        ForceTrackScores.INSTANCE.apply(leaf);
        assertTrue(leaf.getTrackScores());
    }

    public void testForceTrackScoresOverridesExplicitFalse() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        leaf.setTrackScores(false);

        ForceTrackScores.INSTANCE.apply(leaf);
        assertTrue(leaf.getTrackScores());
    }

    // === RequireDocvalueField modifier tests ===

    public void testRequireDocvalueFieldAddsField() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        assertNull(leaf.getDocvalueFields());

        new RequireDocvalueField("embedding").apply(leaf);

        assertNotNull(leaf.getDocvalueFields());
        assertEquals(1, leaf.getDocvalueFields().size());
        assertEquals("embedding", leaf.getDocvalueFields().get(0).field);
    }

    public void testRequireDocvalueFieldNoDuplicates() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        RequireDocvalueField modifier = new RequireDocvalueField("embedding");

        modifier.apply(leaf);
        modifier.apply(leaf); // apply same field again

        assertEquals(1, leaf.getDocvalueFields().size());
    }

    public void testRequireDocvalueFieldMultipleFields() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();

        new RequireDocvalueField("field_a").apply(leaf);
        new RequireDocvalueField("field_b").apply(leaf);

        assertEquals(2, leaf.getDocvalueFields().size());
    }

    // === DisallowTrackScoresFalse constraint tests ===

    public void testDisallowTrackScoresFalsePassesWhenNull() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        leaf.setTrackScores(null);

        // Should not throw
        new DisallowTrackScoresFalse("score_fusion").validate(leaf);
    }

    public void testDisallowTrackScoresFalsePassesWhenTrue() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        leaf.setTrackScores(true);

        // Should not throw
        new DisallowTrackScoresFalse("score_fusion").validate(leaf);
    }

    public void testDisallowTrackScoresFalseThrowsWhenFalse() {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder();
        leaf.setTrackScores(false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DisallowTrackScoresFalse("score_fusion").validate(leaf)
        );
        assertTrue(e.getMessage().contains("[standard] cannot disable [track_scores] inside [score_fusion]"));
    }
}
