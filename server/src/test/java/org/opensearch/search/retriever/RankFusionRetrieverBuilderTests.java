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
 * Unit tests for {@link RankFusionRetrieverBuilder}: the RRF fusion math (deterministic fixtures),
 * (index,_id) keying, window truncation, fused min_score, rank_constant validation, and parse/round-trip.
 */
public class RankFusionRetrieverBuilderTests extends OpenSearchTestCase {

    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Register query parsers (match_all, ...) and the global retriever parser (standard + rank_fusion),
        // so nested-child parsing in fromXContent dispatches through the real registry.
        SearchModule searchModule = new SearchModule(EMPTY, Collections.emptyList());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private static final String INDEX = "products";
    private static final ShardId SHARD = new ShardId(new Index(INDEX, "uuid"), 0);

    private static RetrieverCandidate cand(String id, int position) {
        // score is irrelevant to RRF (rank-based); use position as a placeholder score.
        return new RetrieverCandidate(INDEX, SHARD, id, 1.0f / (position + 1), position);
    }

    private static RankFusionRetrieverBuilder rankFusion() {
        // two dummy standard children so validate()/child count are satisfied where needed
        List<RetrieverBuilder> children = new ArrayList<>();
        children.add(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        children.add(new StandardRetrieverBuilder(new MatchAllQueryBuilder()));
        return new RankFusionRetrieverBuilder(children);
    }

    private static float rrf(int rankConstant, int... ranks) {
        double sum = 0;
        for (int rank : ranks) {
            sum += 1.0 / (rankConstant + rank);
        }
        return (float) sum;
    }

    private static List<String> ids(List<RetrieverCandidate> fused) {
        List<String> ids = new ArrayList<>();
        for (RetrieverCandidate c : fused) {
            ids.add(c.id());
        }
        return ids;
    }

    public void testRrfMathAndOrder() {
        // Leg A ranking: a(1), b(2), c(3). Leg B ranking: b(1), c(2), d(3).
        List<RetrieverCandidate> legA = List.of(cand("a", 0), cand("b", 1), cand("c", 2));
        List<RetrieverCandidate> legB = List.of(cand("b", 0), cand("c", 1), cand("d", 2));

        RankFusionRetrieverBuilder rf = rankFusion(); // rank_constant default 60
        List<RetrieverCandidate> fused = rf.fuse(List.of(legA, legB));

        // b is ranked in both (A rank 2, B rank 1) → highest fused score; c also in both (A3, B2);
        // a only in A (rank 1); d only in B (rank 3).
        float sb = rrf(60, 2, 1);
        float sc = rrf(60, 3, 2);
        float sa = rrf(60, 1);
        float sd = rrf(60, 3);
        // Order by descending fused score: b, c, a, d (verify against computed values).
        assertTrue("b outranks c", sb > sc);
        assertTrue("c outranks a", sc > sa);
        assertTrue("a outranks d", sa > sd);
        assertEquals(List.of("b", "c", "a", "d"), ids(fused));

        // Exact fused scores replayed on the candidates.
        assertEquals(sb, fused.get(0).score(), 1e-7f);
        assertEquals(sc, fused.get(1).score(), 1e-7f);
        assertEquals(sa, fused.get(2).score(), 1e-7f);
        assertEquals(sd, fused.get(3).score(), 1e-7f);

        // Positions renumbered 0..n in fused order.
        for (int i = 0; i < fused.size(); i++) {
            assertEquals(i, fused.get(i).position());
        }
    }

    public void testAbsentInLegContributesZero() {
        // a only in leg A (rank 1); its fused score must equal exactly leg-A's term, no leg-B term.
        List<RetrieverCandidate> legA = List.of(cand("a", 0));
        List<RetrieverCandidate> legB = List.of(cand("z", 0));
        RankFusionRetrieverBuilder rf = rankFusion();
        List<RetrieverCandidate> fused = rf.fuse(List.of(legA, legB));
        // both a and z present, each with a single leg's contribution 1/(60+1).
        for (RetrieverCandidate c : fused) {
            assertEquals(rrf(60, 1), c.score(), 1e-7f);
        }
    }

    public void testCrossIndexIdKeyingKeepsDocsDistinct() {
        ShardId otherShard = new ShardId(new Index("other", "uuid2"), 0);
        RetrieverCandidate a1 = new RetrieverCandidate(INDEX, SHARD, "dup", 1f, 0);
        RetrieverCandidate a2 = new RetrieverCandidate("other", otherShard, "dup", 1f, 0);
        RankFusionRetrieverBuilder rf = rankFusion();
        List<RetrieverCandidate> fused = rf.fuse(List.of(List.of(a1), List.of(a2)));
        // Same _id in two indices → two distinct fused entries, not merged into one.
        assertEquals(2, fused.size());
    }

    public void testRankWindowSizeTruncates() {
        List<RetrieverCandidate> leg = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            leg.add(cand("d" + i, i));
        }
        RankFusionRetrieverBuilder rf = rankFusion();
        rf.setRankWindowSize(3);
        List<RetrieverCandidate> fused = rf.fuse(List.of(leg, List.of()));
        assertEquals(3, fused.size());
        // Keeps the top 3 by fused score (best ranks): d0, d1, d2.
        assertEquals(List.of("d0", "d1", "d2"), ids(fused));
    }

    public void testFusedMinScoreDropsLowScorers() {
        List<RetrieverCandidate> leg = List.of(cand("a", 0), cand("b", 1));
        RankFusionRetrieverBuilder rf = rankFusion();
        // a: 1/61 ≈ 0.01639; b: 1/62 ≈ 0.01613. Threshold between them keeps only a.
        rf.setMinScore(0.0163f);
        List<RetrieverCandidate> fused = rf.fuse(List.of(leg, List.of()));
        assertEquals(List.of("a"), ids(fused));
    }

    public void testRankConstantValidation() {
        expectThrows(IllegalArgumentException.class, () -> rankFusion().setRankConstant(0));
        expectThrows(IllegalArgumentException.class, () -> rankFusion().setRankConstant(10001));
        rankFusion().setRankConstant(1);      // boundary ok
        rankFusion().setRankConstant(10000);  // boundary ok
    }

    public void testValidateRequiresTwoChildren() {
        RankFusionRetrieverBuilder oneChild = new RankFusionRetrieverBuilder(
            List.of(new StandardRetrieverBuilder(new MatchAllQueryBuilder()))
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, oneChild::validate);
        assertTrue(e.getMessage(), e.getMessage().contains("at least 2"));
        // 2 children validates (recurses into each child's validate, which passes for a match_all standard).
        rankFusion().validate();
    }

    public void testGetName() {
        assertEquals("rank_fusion", rankFusion().getName());
    }

    public void testFromXContentParsesFieldsAndNesting() throws Exception {
        String json = "{"
            + "\"retrievers\":["
            + "  {\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "  {\"rank_fusion\":{\"retrievers\":["
            + "     {\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "     {\"standard\":{\"query\":{\"match_all\":{}}}}]}}"
            + "],"
            + "\"rank_constant\":42,\"rank_window_size\":25,\"min_score\":0.1}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken(); // START_OBJECT
        RankFusionRetrieverBuilder rf = RankFusionRetrieverBuilder.fromXContent(parser);
        assertEquals(2, rf.getChildRetrievers().size());
        assertEquals(42, rf.getRankConstant());
        assertEquals(25, rf.getRankWindowSize());
        assertEquals(0.1f, rf.getMinScore(), 0f);
        // nested rank_fusion child parsed via the shared registry
        assertTrue(rf.getChildRetrievers().get(1) instanceof RankFusionRetrieverBuilder);
    }

    public void testFromXContentUnknownFieldRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],\"bogus\":1}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RankFusionRetrieverBuilder.fromXContent(parser));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown field [bogus]"));
    }

    public void testFromXContentInvalidRankConstantRejected() throws Exception {
        String json = "{\"retrievers\":[{\"standard\":{\"query\":{\"match_all\":{}}}},"
            + "{\"standard\":{\"query\":{\"match_all\":{}}}}],\"rank_constant\":0}";
        XContentParser parser = createParser(jsonXContent, json);
        parser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> RankFusionRetrieverBuilder.fromXContent(parser));
    }

    public void testToXContentRoundTrip() throws Exception {
        // Full serialize -> parse round-trip. Children are written as wrapped objects inside the retrievers
        // array; writing them unwrapped emits a field name with no enclosing object and fails serialization.
        RankFusionRetrieverBuilder rf = rankFusion();
        rf.setRankConstant(42);
        rf.setRankWindowSize(25);
        rf.setMinScore(0.1f);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        rf.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String rendered = builder.toString();
        assertTrue(rendered, rendered.contains("\"retrievers\":[{\"standard\":"));

        XContentParser parser = createParser(jsonXContent, rendered);
        parser.nextToken(); // START_OBJECT (wrapper)
        parser.nextToken(); // FIELD_NAME rank_fusion
        parser.nextToken(); // START_OBJECT (inside rank_fusion)
        RankFusionRetrieverBuilder reparsed = RankFusionRetrieverBuilder.fromXContent(parser);

        assertEquals(2, reparsed.getChildRetrievers().size());
        assertEquals(42, reparsed.getRankConstant());
        assertEquals(25, reparsed.getRankWindowSize());
        assertEquals(0.1f, reparsed.getMinScore(), 0f);
    }
}
