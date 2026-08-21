/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compound retriever implementing Reciprocal Rank Fusion (RRF).
 * Combines N children by rank position rather than raw scores, making it robust to score scale differences.
 * <p>
 * RRF formula: {@code score(d) = Σ 1 / (rank_constant + rank_i(d))}
 * where {@code rank_i(d)} is the 1-based position of document d in leg i.
 * Documents not present in a leg contribute 0.
 *
 * @opensearch.internal
 */
public class RankFusionRetrieverBuilder extends CompoundRetrieverBuilder {

    public static final String NAME = "rank_fusion";
    public static final int DEFAULT_RANK_CONSTANT = 60;

    private int rankConstant = DEFAULT_RANK_CONSTANT;

    public RankFusionRetrieverBuilder() {}

    public RankFusionRetrieverBuilder(List<RetrieverBuilder> children, int rankConstant) {
        this.childRetrievers = children;
        setRankConstant(rankConstant);
    }

    public int getRankConstant() {
        return rankConstant;
    }

    public void setRankConstant(int rankConstant) {
        if (rankConstant < 1 || rankConstant > 10_000) {
            throw new IllegalArgumentException(
                "[rank_fusion] rank_constant must be between 1 and 10000, got " + rankConstant
            );
        }
        this.rankConstant = rankConstant;
    }

    @Override
    protected List<RankedDoc> fuse(List<List<RankedDoc>> childResults) {
        // Compute RRF: score(d) = Σ 1/(rank_constant + rank_i(d))
        // rank_i(d) is 1-based position in leg i
        Map<String, Float> rrfScores = new HashMap<>();
        Map<String, RankedDoc> docLookup = new HashMap<>();

        for (List<RankedDoc> legResults : childResults) {
            for (int position = 0; position < legResults.size(); position++) {
                RankedDoc doc = legResults.get(position);
                String docKey = doc.index() + "|" + doc.id();
                float contribution = 1.0f / (rankConstant + position + 1); // 1-based rank
                rrfScores.merge(docKey, contribution, Float::sum);
                docLookup.putIfAbsent(docKey, doc);
            }
        }

        // Apply min_score filter on fused scores
        if (minScore != null) {
            rrfScores.entrySet().removeIf(e -> e.getValue() < minScore);
        }

        // Sort by fused score descending, take top size
        List<Map.Entry<String, Float>> sorted = new ArrayList<>(rrfScores.entrySet());
        sorted.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));

        List<RankedDoc> results = new ArrayList<>();
        int limit = Math.min(rankWindowSize, sorted.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, Float> entry = sorted.get(i);
            RankedDoc original = docLookup.get(entry.getKey());
            results.add(new RankedDoc(original.id(), original.index(), original.shardId(), entry.getValue(), i));
        }
        return results;
    }

    @Override
    protected Explanation buildFusionExplanation(String docId, String docIndex) {
        // Find the doc's rank in each leg and compute per-leg RRF contributions
        List<Explanation> legDetails = new ArrayList<>();
        float totalScore = 0.0f;

        for (int legIdx = 0; legIdx < childRetrievers.size(); legIdx++) {
            RetrieverBuilder child = childRetrievers.get(legIdx);
            List<RankedDoc> childResult = child.getResolvedResult();

            // Find the doc's position in this leg
            int rank = -1;
            if (childResult != null) {
                for (int pos = 0; pos < childResult.size(); pos++) {
                    RankedDoc doc = childResult.get(pos);
                    if (doc.id().equals(docId) && doc.index().equals(docIndex)) {
                        rank = pos + 1; // 1-based
                        break;
                    }
                }
            }

            if (rank > 0) {
                float contribution = 1.0f / (rankConstant + rank);
                totalScore += contribution;
                Explanation childExplain = getChildExplanation(child, docId, docIndex);
                Explanation legExplain = Explanation.match(
                    contribution,
                    "1/(" + rankConstant + "+" + rank + ") = " + contribution + " [leg " + legIdx + ", rank " + rank + "]",
                    childExplain != null ? List.of(childExplain) : List.of()
                );
                legDetails.add(legExplain);
            } else {
                legDetails.add(Explanation.noMatch("leg " + legIdx + ": not present (contribution: 0)"));
            }
        }

        return Explanation.match(
            totalScore,
            "rank_fusion [rank_constant=" + rankConstant + "]",
            legDetails
        );
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("rank_constant", rankConstant);
        builder.field("rank_window_size", rankWindowSize);
        if (minScore != null) {
            builder.field("min_score", minScore);
        }
        builder.startArray("retrievers");
        for (RetrieverBuilder child : childRetrievers) {
            child.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Parse from XContent. Expected: { "retrievers": [...], "rank_constant": 60, "rank_window_size": 100 }
     * Each entry in "retrievers" may be any registered retriever type — including nested
     * compound/transformer retrievers, not just "standard" — dispatched via the shared registry.
     */
    public static RankFusionRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        RankFusionRetrieverBuilder builder = new RankFusionRetrieverBuilder();
        List<RetrieverBuilder> children = new ArrayList<>();

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                switch (fieldName) {
                    case "retrievers":
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                                children.add(RetrieverBuilder.parseInnerRetrieverBuilder(parser));
                            }
                        }
                        break;
                    case "rank_constant":
                        builder.setRankConstant(parser.intValue());
                        break;
                    case "rank_window_size":
                        builder.setRankWindowSize(parser.intValue());
                        break;
                    case "min_score":
                        builder.setMinScore(parser.floatValue());
                        break;
                    default:
                        throw new IllegalArgumentException("[rank_fusion] unknown field [" + fieldName + "]");
                }
            }
        }

        builder.childRetrievers = children;
        return builder;
    }
}
