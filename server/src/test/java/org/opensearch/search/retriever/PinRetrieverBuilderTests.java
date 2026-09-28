/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link PinRetrieverBuilder}: XContent round-trip, validation, the require_match:true
 * reorder/dedup arithmetic over hand-resolved child candidates, the synthetic-score assignment, and the
 * buildExplanation / buildProfile shapes. The always-pin id sub-search (require_match:false injecting a
 * non-matching doc) needs a live cluster and is covered end-to-end in {@code PinRetrieverIT}.
 */
public class PinRetrieverBuilderTests extends OpenSearchTestCase {

    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Register query parsers (match_all, ...) and the global retriever parser (standard/pin/...),
        // so nested-child parsing in fromXContent dispatches through the real registry.
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private static final String INDEX = "products";
    private static final ShardId SHARD = new ShardId(new Index(INDEX, "uuid"), 0);

    private static RetrieverCandidate cand(String id, float score, int position) {
        return new RetrieverCandidate(INDEX, SHARD, id, score, position);
    }

    private static StandardRetrieverBuilder resolvedLeaf(List<RetrieverCandidate> candidates) {
        StandardRetrieverBuilder leaf = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        leaf.setSearchResult(candidates);
        leaf.doResolve();
        return leaf;
    }

    private static PinRetrieverBuilder pinByIds(List<String> ids, RetrieverBuilder child) {
        List<PinRetrieverBuilder.PinnedDoc> pins = ids.stream().map(id -> new PinRetrieverBuilder.PinnedDoc(id, null)).toList();
        return new PinRetrieverBuilder(pins, child);
    }

    private static String render(ToXContent x) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        x.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    // ---- XContent ----

    public void testParseIdsForm() throws Exception {
        String json = "{\"ids\":[\"a\",\"b\"],\"retriever\":{\"standard\":{\"query\":{\"match_all\":{}}}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // START_OBJECT
        PinRetrieverBuilder pin = PinRetrieverBuilder.fromXContent(parser);
        assertEquals(2, pin.getPinnedDocs().size());
        assertEquals("a", pin.getPinnedDocs().get(0).id());
        assertNull("ids form has no _index", pin.getPinnedDocs().get(0).index());
        assertFalse("require_match defaults to false", pin.isRequireMatch());
        assertNotNull(pin.getRetriever());
    }

    public void testParseDocsFormAndRequireMatch() throws Exception {
        String json = "{\"docs\":[{\"_id\":\"a\",\"_index\":\"i1\"},{\"_id\":\"b\",\"_index\":\"i2\"}],"
            + "\"require_match\":true,\"retriever\":{\"standard\":{\"query\":{\"match_all\":{}}}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        PinRetrieverBuilder pin = PinRetrieverBuilder.fromXContent(parser);
        assertEquals(2, pin.getPinnedDocs().size());
        assertEquals("i1", pin.getPinnedDocs().get(0).index());
        assertTrue(pin.isRequireMatch());
    }

    public void testToXContentIdsRoundTrip() throws Exception {
        PinRetrieverBuilder pin = pinByIds(List.of("a", "b"), new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        String json = render(pin);
        assertTrue(json, json.contains("\"pin\":{"));
        assertTrue(json, json.contains("\"ids\":[\"a\",\"b\"]"));
        assertTrue(json, json.contains("\"retriever\":{"));
        assertFalse("require_match omitted at default", json.contains("require_match"));
    }

    public void testToXContentDocsFormAndRequireMatchEmitted() throws Exception {
        PinRetrieverBuilder pin = new PinRetrieverBuilder(
            List.of(new PinRetrieverBuilder.PinnedDoc("a", "i1")),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );
        pin.setRequireMatch(true);
        String json = render(pin);
        assertTrue(json, json.contains("\"docs\":[{\"_id\":\"a\",\"_index\":\"i1\"}]"));
        assertTrue(json, json.contains("\"require_match\":true"));
    }

    public void testRenderThenReparseRoundTrip() throws Exception {
        PinRetrieverBuilder original = new PinRetrieverBuilder(
            List.of(new PinRetrieverBuilder.PinnedDoc("a", "i1"), new PinRetrieverBuilder.PinnedDoc("b", "i1")),
            new StandardRetrieverBuilder(new MatchAllQueryBuilder())
        );
        original.setRequireMatch(true);
        // render() emits {"pin":{...}} (the wrapper object + the pin's own startObject(NAME)).
        String rendered = render(original);
        XContentParser parser = createParser(JsonXContent.jsonXContent, rendered);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken()); // outer {
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());   // "pin"
        assertEquals("pin", parser.currentName());
        parser.nextToken(); // START_OBJECT of the pin body
        PinRetrieverBuilder reparsed = PinRetrieverBuilder.fromXContent(parser);
        assertEquals(2, reparsed.getPinnedDocs().size());
        assertEquals("a", reparsed.getPinnedDocs().get(0).id());
        assertEquals("i1", reparsed.getPinnedDocs().get(0).index());
        assertTrue(reparsed.isRequireMatch());
        assertNotNull(reparsed.getRetriever());
    }

    // ---- validation ----

    public void testValidateRejectsMissingChild() {
        PinRetrieverBuilder pin = pinByIds(List.of("a"), null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, pin::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("requires a [retriever]"));
    }

    public void testValidateRejectsEmptyPins() {
        PinRetrieverBuilder pin = new PinRetrieverBuilder(List.of(), new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, pin::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("non-empty"));
    }

    public void testParseRejectsBothIdsAndDocs() throws Exception {
        String json = "{\"ids\":[\"a\"],\"docs\":[{\"_id\":\"b\"}],\"retriever\":{\"standard\":{\"query\":{\"match_all\":{}}}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PinRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("exactly one of"));
    }

    public void testParseRejectsNeitherIdsNorDocs() throws Exception {
        String json = "{\"retriever\":{\"standard\":{\"query\":{\"match_all\":{}}}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PinRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("exactly one of"));
    }

    public void testPrepareLeavesRejectsInsideFusion() {
        PinRetrieverBuilder pin = pinByIds(List.of("a"), resolvedLeaf(List.of(cand("a", 1f, 0))));
        // A fusion-governed context (window inherited) must be rejected: pin is top-level only.
        LeafPreparationContext fusionCtx = LeafPreparationContext.root(false, false).underFusion(10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> pin.prepareLeaves(fusionCtx));
        assertTrue(e.getMessage(), e.getMessage().contains("only allowed at the top level"));
    }

    // ---- assignPinnedScores arithmetic ----

    public void testAssignPinnedScoresStrictlyDescendingAboveMax() {
        List<RetrieverCandidate> pins = List.of(cand("p1", 0f, 0), cand("p2", 0f, 0), cand("p3", 0f, 0));
        float organicMax = 5.0f;
        List<RetrieverCandidate> scored = PinRetrieverBuilder.assignPinnedScores(pins, organicMax);
        assertEquals(3, scored.size());
        assertTrue("p1 > p2", scored.get(0).score() > scored.get(1).score());
        assertTrue("p2 > p3", scored.get(1).score() > scored.get(2).score());
        assertTrue("last pin still > organicMax", scored.get(2).score() > organicMax);
        assertEquals(0, scored.get(0).position());
        assertEquals(2, scored.get(2).position());
    }

    public void testAssignPinnedScoresHandlesNonFiniteMax() {
        // No organic docs -> organicMax is -inf; base falls back to 0, scores still strictly descending > 0.
        List<RetrieverCandidate> pins = List.of(cand("p1", 0f, 0), cand("p2", 0f, 0));
        List<RetrieverCandidate> scored = PinRetrieverBuilder.assignPinnedScores(pins, Float.NEGATIVE_INFINITY);
        assertTrue(scored.get(0).score() > scored.get(1).score());
        assertTrue(scored.get(1).score() > 0f);
    }

    // ---- require_match:true reorder + dedup (doResolve over a hand-resolved child) ----

    public void testRequireMatchTrueReordersAndDedups() {
        // Child window: a(3), b(2), c(1). Pin [c, a] -> c,a on top, then organic b; a/c removed from tail.
        StandardRetrieverBuilder child = resolvedLeaf(List.of(cand("a", 3f, 0), cand("b", 2f, 1), cand("c", 1f, 2)));
        PinRetrieverBuilder pin = pinByIds(List.of("c", "a"), child);
        pin.setRequireMatch(true);
        pin.doResolve();
        List<RetrieverCandidate> result = pin.getResolvedResult();
        assertEquals(3, result.size());
        assertEquals("c pinned first", "c", result.get(0).id());
        assertEquals("a pinned second", "a", result.get(1).id());
        assertEquals("b organic after pins", "b", result.get(2).id());
        assertTrue(result.get(0).score() > result.get(1).score());
        assertTrue(result.get(1).score() > result.get(2).score());
        assertEquals(0, result.get(0).position());
        assertEquals(2, result.get(2).position());
    }

    public void testRequireMatchTrueDropsNonMatchingPin() {
        // Pin [z, a] but z is not in the child window and require_match:true -> z dropped, a pinned.
        StandardRetrieverBuilder child = resolvedLeaf(List.of(cand("a", 3f, 0), cand("b", 2f, 1)));
        PinRetrieverBuilder pin = pinByIds(List.of("z", "a"), child);
        pin.setRequireMatch(true);
        pin.doResolve();
        List<RetrieverCandidate> result = pin.getResolvedResult();
        assertEquals(2, result.size());
        assertEquals("a", result.get(0).id());
        assertEquals("b", result.get(1).id());
        for (RetrieverCandidate c : result) {
            assertNotEquals("z must not appear", "z", c.id());
        }
    }

    // ---- explanation / profile shape ----

    public void testBuildExplanationPinnedVsOrganic() {
        StandardRetrieverBuilder child = resolvedLeaf(List.of(cand("a", 3f, 0), cand("b", 2f, 1)));
        PinRetrieverBuilder pin = pinByIds(List.of("b"), child);
        pin.setRequireMatch(true);
        pin.doResolve();

        Explanation pinned = pin.buildExplanation(INDEX, "b");
        assertNotNull(pinned);
        assertTrue(pinned.getDescription(), pinned.getDescription().contains("pinned to rank 1"));

        Explanation organic = pin.buildExplanation(INDEX, "a");
        assertNotNull(organic);
        assertFalse("organic is not described as pinned", organic.getDescription().contains("pinned to rank"));

        assertNull("doc not in result -> null", pin.buildExplanation(INDEX, "missing"));
    }

    public void testBuildProfileShape() {
        StandardRetrieverBuilder child = resolvedLeaf(List.of(cand("a", 3f, 0)));
        child.nodeElapsedNanos = 1_000_000L;
        PinRetrieverBuilder pin = pinByIds(List.of("a"), child);
        pin.setRequireMatch(true);
        pin.doResolve();
        pin.nodeElapsedNanos = 1_500_000L; // wall > child -> positive orchestration_overhead

        RetrieverProfile.Node node = pin.buildProfile();
        assertEquals("pin", node.getType());
        assertEquals(1, node.getChildren().size());
        assertEquals("standard", node.getChildren().get(0).getType());
        assertTrue("orchestration_overhead present", node.getBreakdown().containsKey("orchestration_overhead"));
        long orchestration = node.getBreakdown().get("orchestration_overhead");
        assertEquals(
            "pin total == orchestration_overhead + child total",
            node.getTotalTimeInNanos(),
            orchestration + node.getChildren().get(0).getTotalTimeInNanos()
        );
    }
}
