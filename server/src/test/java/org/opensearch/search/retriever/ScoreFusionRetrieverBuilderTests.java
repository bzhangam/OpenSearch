/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.settings.Settings.EMPTY;
import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Unit tests for {@link ScoreFusionRetrieverBuilder}: min-max normalization (incl. all-equal and the
 * exact-zero floor), weighted arithmetic-mean combination over present legs, weights validation, window
 * truncation, fused min_score, (index,_id) keying, and parse/round-trip.
 */
public class ScoreFusionRetrieverBuilderTests extends OpenSearchTestCase {

    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(EMPTY, Collections.emptyList());
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

    private static ScoreFusionRetrieverBuilder scoreFusion() {
        List<RetrieverBuilder> children = new ArrayList<>();
        children.add(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        children.add(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        return new ScoreFusionRetrieverBuilder(children);
    }

    private static List<String> ids(List<RetrieverCandidate> fused) {
        List<String> ids = new ArrayList<>();
        for (RetrieverCandidate c : fused) {
            ids.add(c.id());
        }
        return ids;
    }

    private static float scoreOf(List<RetrieverCandidate> fused, String id) {
        for (RetrieverCandidate c : fused) {
            if (c.id().equals(id)) {
                return c.score();
            }
        }
        throw new AssertionError("id not found: " + id);
    }

    public void testMinMaxNormalizationAndEqualWeightMean() {
        // Leg A scores: a=10, b=6, c=2 -> min 2, max 10 -> norm a=1.0, b=0.5, c=0.0->0.001(floor)
        // Leg B scores: b=8, c=4, d=0 -> min 0, max 8 -> norm b=1.0, c=0.5, d=0.0->0.001(floor)
        List<RetrieverCandidate> legA = List.of(cand("a", 10, 0), cand("b", 6, 1), cand("c", 2, 2));
        List<RetrieverCandidate> legB = List.of(cand("b", 8, 0), cand("c", 4, 1), cand("d", 0, 2));

        List<RetrieverCandidate> fused = scoreFusion().fuse(List.of(legA, legB));

        // Equal weights (1,1); divide by the TOTAL number of legs (2), an absent leg contributes 0:
        // a: only A -> (1.0 + 0)/2 = 0.5
        // b: (0.5 + 1.0)/2 = 0.75
        // c: (0.001 + 0.5)/2 = 0.2505
        // d: only B -> (0 + 0.001)/2 = 0.0005
        assertEquals(0.5f, scoreOf(fused, "a"), 1e-6f);
        assertEquals(0.75f, scoreOf(fused, "b"), 1e-6f);
        assertEquals(0.2505f, scoreOf(fused, "c"), 1e-6f);
        assertEquals(0.0005f, scoreOf(fused, "d"), 1e-6f);
        // Order by descending fused score: b(0.75), a(0.5), c(0.2505), d(0.0005)
        assertEquals(List.of("b", "a", "c", "d"), ids(fused));
        // positions renumbered
        for (int i = 0; i < fused.size(); i++) {
            assertEquals(i, fused.get(i).position());
        }
    }

    public void testWeightsShiftRanking() {
        // Same legs; weight leg B (index 1) heavily so a B-strong doc can outrank an A-only doc.
        List<RetrieverCandidate> legA = List.of(cand("a", 10, 0), cand("b", 6, 1));
        List<RetrieverCandidate> legB = List.of(cand("b", 8, 0), cand("a", 4, 1));
        ScoreFusionRetrieverBuilder rf = scoreFusion();
        rf.setWeights(new float[] { 1.0f, 3.0f });

        // Leg A: a=10,b=6 -> min6,max10 -> a=1.0, b=0.001
        // Leg B: b=8,a=4 -> min4,max8 -> b=1.0, a=0.001
        // a: (1*1.0 + 3*0.001)/4 = (1.0+0.003)/4 = 0.25075
        // b: (1*0.001 + 3*1.0)/4 = (0.001+3.0)/4 = 0.75025
        List<RetrieverCandidate> fused = rf.fuse(List.of(legA, legB));
        assertEquals(0.25075f, scoreOf(fused, "a"), 1e-6f);
        assertEquals(0.75025f, scoreOf(fused, "b"), 1e-6f);
        assertEquals(List.of("b", "a"), ids(fused));
    }

    public void testAllEqualScoresNormalizeToOne() {
        // A leg where every score is identical -> each normalizes to 1.0 (min==max). With an empty second
        // leg, each doc is present in 1 of 2 legs, so fused = (1.0 + 0)/2 = 0.5.
        List<RetrieverCandidate> legA = List.of(cand("a", 5, 0), cand("b", 5, 1), cand("c", 5, 2));
        List<RetrieverCandidate> legB = List.of();
        List<RetrieverCandidate> fused = scoreFusion().fuse(List.of(legA, legB));
        for (RetrieverCandidate c : fused) {
            assertEquals(0.5f, c.score(), 1e-6f);
        }
        assertEquals(3, fused.size());
    }

    public void testSingleDocLegNormalizesToOne() {
        // A leg with one doc: min==max==that score -> normalized 1.0. Present in 1 of 2 legs -> fused 0.5.
        List<RetrieverCandidate> legA = List.of(cand("a", 42, 0));
        List<RetrieverCandidate> legB = List.of(cand("z", 7, 0));
        List<RetrieverCandidate> fused = scoreFusion().fuse(List.of(legA, legB));
        assertEquals(0.5f, scoreOf(fused, "a"), 1e-6f);
        assertEquals(0.5f, scoreOf(fused, "z"), 1e-6f);
    }

    public void testRankWindowSizeTruncates() {
        List<RetrieverCandidate> leg = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            leg.add(cand("d" + i, 10 - i, i)); // descending scores d0..d9
        }
        ScoreFusionRetrieverBuilder rf = scoreFusion();
        rf.setRankWindowSize(3);
        List<RetrieverCandidate> fused = rf.fuse(List.of(leg, List.of()));
        assertEquals(3, fused.size());
        assertEquals(List.of("d0", "d1", "d2"), ids(fused));
    }

    public void testFusedMinScoreDropsLowScorers() {
        // Leg A: a=10,b=2 -> a=1.0, b=0.001 ; only-A docs -> fused == norm.
        List<RetrieverCandidate> legA = List.of(cand("a", 10, 0), cand("b", 2, 1));
        ScoreFusionRetrieverBuilder rf = scoreFusion();
        rf.setMinScore(0.5f);
        List<RetrieverCandidate> fused = rf.fuse(List.of(legA, List.of()));
        assertEquals(List.of("a"), ids(fused));
    }

    public void testCrossIndexIdKeyingKeepsDocsDistinct() {
        ShardId otherShard = new ShardId(new Index("other", "uuid2"), 0);
        RetrieverCandidate a1 = new RetrieverCandidate(INDEX, SHARD, "dup", 5f, 0);
        RetrieverCandidate a2 = new RetrieverCandidate("other", otherShard, "dup", 5f, 0);
        List<RetrieverCandidate> fused = scoreFusion().fuse(List.of(List.of(a1), List.of(a2)));
        assertEquals(2, fused.size());
    }

    public void testValidateRequiresTwoChildren() {
        ScoreFusionRetrieverBuilder oneChild = new ScoreFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, oneChild::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("at least 2"));
        scoreFusion().validate(); // 2 children ok
    }

    public void testValidateWeightsLengthMismatch() {
        ScoreFusionRetrieverBuilder rf = scoreFusion(); // 2 children
        rf.setWeights(new float[] { 1.0f, 2.0f, 3.0f });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, rf::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("[combination.parameters.weights] length"));
    }

    public void testInvalidNormalizationRejected() {
        expectThrows(IllegalArgumentException.class, () -> scoreFusion().setNormalization("l2"));
        scoreFusion().setNormalization("min_max"); // ok
    }

    public void testRankWindowSizeValidation() {
        expectThrows(IllegalArgumentException.class, () -> scoreFusion().setRankWindowSize(0));
        scoreFusion().setRankWindowSize(1); // boundary ok
    }

    public void testGetName() {
        assertEquals("score_fusion", scoreFusion().getName());
    }

    public void testFromXContentParsesFieldsAndNesting() throws Exception {
        String json = "{"
            + "\"retrievers\":["
            + "  {\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "  {\"score_fusion\":{\"retrievers\":["
            + "     {\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "     {\"standard\":{\"query\":{\"match_all\":{}}}}]}}"
            + "],"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[1.0,2.0]}},"
            + "\"rank_window_size\":25,\"min_score\":0.1}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken(); // START_OBJECT
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        assertEquals(2, rf.getChildRetrievers().size());
        assertArrayEquals(new float[] { 1.0f, 2.0f }, rf.getWeights(), 0f);
        assertEquals(25, rf.getRankWindowSize());
        assertEquals(0.1f, rf.getMinScore(), 0f);
        assertEquals("min_max", rf.getNormalization());
        assertEquals("arithmetic_mean", rf.getCombination());
        assertTrue(rf.getChildRetrievers().get(1) instanceof ScoreFusionRetrieverBuilder);
    }

    public void testFromXContentUnknownFieldRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],\"bogus\":1}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown field [bogus]"));
    }

    public void testFromXContentInvalidNormalizationRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],\"normalization\":\"l2\"}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
    }

    public void testFromXContentNormalizationObjectForm() throws Exception {
        // Object form: {"technique": "min_max"} parses to the same technique as the bare string.
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"normalization\":{\"technique\":\"min_max\"}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        assertEquals("min_max", rf.getNormalization());
    }

    public void testFromXContentNormalizationObjectEmptyParametersOk() throws Exception {
        // An explicit but empty parameters object is allowed (min_max takes no params).
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"normalization\":{\"technique\":\"min_max\",\"parameters\":{}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        assertEquals("min_max", rf.getNormalization());
    }

    public void testFromXContentNormalizationObjectInvalidTechniqueRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"normalization\":{\"technique\":\"l2\"}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
    }

    public void testFromXContentNormalizationObjectNonEmptyParametersRejected() throws Exception {
        // Non-empty parameters are rejected (not silently ignored) — min_max takes none today.
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"normalization\":{\"technique\":\"min_max\",\"parameters\":{\"floor\":0.1}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("does not support the given [parameters]"));
    }

    public void testFromXContentNormalizationObjectMissingTechniqueRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"normalization\":{\"parameters\":{}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("requires a [technique]"));
    }

    public void testFromXContentNormalizationObjectUnknownFieldRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"normalization\":{\"technique\":\"min_max\",\"bogus\":1}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown field [bogus]"));
    }

    public void testToXContentRoundTrip() throws Exception {
        // Full serialize -> parse round-trip. RetrieverBuilder#toXContent emits a named object
        // ("score_fusion":{...}), so wrap it in an enclosing object; children are written as wrapped
        // objects inside the retrievers array (the serialization bug this guards against).
        ScoreFusionRetrieverBuilder rf = scoreFusion();
        rf.setWeights(new float[] { 2.0f, 1.0f });
        rf.setRankWindowSize(25);
        rf.setMinScore(0.1f);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        rf.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String rendered = builder.toString();
        assertTrue(rendered, rendered.contains("\"normalization\":{\"technique\":\"min_max\"}"));
        assertTrue(
            rendered,
            rendered.contains("\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[2.0,1.0]}}")
        );
        assertTrue(rendered, rendered.contains("\"retrievers\":[{\"standard\":"));

        XContentParser parser = createParser(jsonXContent, rendered);
        parser.nextToken(); // START_OBJECT (wrapper)
        parser.nextToken(); // FIELD_NAME score_fusion
        parser.nextToken(); // START_OBJECT (inside score_fusion)
        ScoreFusionRetrieverBuilder reparsed = ScoreFusionRetrieverBuilder.fromXContent(parser);

        assertEquals(2, reparsed.getChildRetrievers().size());
        assertArrayEquals(new float[] { 2.0f, 1.0f }, reparsed.getWeights(), 0f);
        assertEquals(25, reparsed.getRankWindowSize());
        assertEquals(0.1f, reparsed.getMinScore(), 0f);
        assertEquals("min_max", reparsed.getNormalization());
        assertEquals("arithmetic_mean", reparsed.getCombination());
    }

    public void testFromXContentCombinationBareStringForm() throws Exception {
        // Bare string form for combination, symmetric to normalization.
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":\"arithmetic_mean\"}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        assertEquals("arithmetic_mean", rf.getCombination());
        assertNull(rf.getWeights());
    }

    public void testFromXContentCombinationObjectWithWeights() throws Exception {
        // Object form with weights under parameters.
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[3.0,1.0]}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        assertEquals("arithmetic_mean", rf.getCombination());
        assertArrayEquals(new float[] { 3.0f, 1.0f }, rf.getWeights(), 0f);
    }

    public void testFromXContentCombinationObjectNoParametersOk() throws Exception {
        // technique only, no parameters -> no weights.
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        assertEquals("arithmetic_mean", rf.getCombination());
        assertNull(rf.getWeights());
    }

    public void testFromXContentCombinationInvalidTechniqueRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":{\"technique\":\"geometric_mean\"}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("only supports [arithmetic_mean]"));
    }

    public void testFromXContentCombinationMissingTechniqueRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":{\"parameters\":{\"weights\":[1.0,1.0]}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("requires a [technique]"));
    }

    public void testFromXContentCombinationUnknownParameterRejected() throws Exception {
        // A parameter other than weights is rejected, not silently ignored.
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"bogus\":1}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScoreFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("does not support the given [parameters]"));
    }

    public void testFromXContentCombinationWeightsLengthMismatchRejected() throws Exception {
        // weights length must match the number of retrievers (enforced in validate()).
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[1.0,2.0,3.0]}}}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        ScoreFusionRetrieverBuilder rf = ScoreFusionRetrieverBuilder.fromXContent(parser);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, rf::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("[combination.parameters.weights] length"));
    }
}
