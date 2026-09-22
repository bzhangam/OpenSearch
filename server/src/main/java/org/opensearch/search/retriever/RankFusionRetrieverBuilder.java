/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A compound retriever that fuses its children's rankings with <b>Reciprocal Rank Fusion (RRF)</b>.
 * <p>
 * Each document's fused score is the sum, over the children that ranked it, of
 * {@code 1 / (rank_constant + rank)}, where {@code rank} is the document's 1-based position in that
 * child's ranking. A document absent from a child contributes nothing (0) for that child. Documents are
 * keyed by {@code (index, _id)} so the same {@code _id} in different indices stays distinct. The fused
 * ranking is sorted by descending fused score and truncated to {@code rank_window_size}; an optional
 * fused {@code min_score} drops documents below the threshold.
 * <p>
 * RRF is pure coordinator arithmetic — no async round, no plugin dependency.
 *
 * @opensearch.api
 */
public class RankFusionRetrieverBuilder extends CompoundRetrieverBuilder {

    public static final String NAME = "rank_fusion";

    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final int MIN_RANK_CONSTANT = 1;
    public static final int MAX_RANK_CONSTANT = 10000;
    public static final int DEFAULT_RANK_WINDOW_SIZE = 100;

    static final String RETRIEVERS_FIELD = "retrievers";
    static final String RANK_CONSTANT_FIELD = "rank_constant";
    static final String RANK_WINDOW_SIZE_FIELD = "rank_window_size";
    static final String MIN_SCORE_FIELD = "min_score";

    private int rankConstant = DEFAULT_RANK_CONSTANT;
    private int rankWindowSize = DEFAULT_RANK_WINDOW_SIZE;
    private Float minScore;

    public RankFusionRetrieverBuilder(List<RetrieverBuilder> children) {
        super(children);
    }

    public int getRankConstant() {
        return rankConstant;
    }

    public void setRankConstant(int rankConstant) {
        validateRankConstant(rankConstant);
        this.rankConstant = rankConstant;
    }

    public int getRankWindowSize() {
        return rankWindowSize;
    }

    public void setRankWindowSize(int rankWindowSize) {
        if (rankWindowSize < 1) {
            throw new IllegalArgumentException("[" + NAME + "] [" + RANK_WINDOW_SIZE_FIELD + "] must be >= 1, got " + rankWindowSize);
        }
        this.rankWindowSize = rankWindowSize;
    }

    public Float getMinScore() {
        return minScore;
    }

    public void setMinScore(Float minScore) {
        this.minScore = minScore;
    }

    private static void validateRankConstant(int rankConstant) {
        if (rankConstant < MIN_RANK_CONSTANT || rankConstant > MAX_RANK_CONSTANT) {
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] ["
                    + RANK_CONSTANT_FIELD
                    + "] must be in ["
                    + MIN_RANK_CONSTANT
                    + ", "
                    + MAX_RANK_CONSTANT
                    + "], got "
                    + rankConstant
            );
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Fusion governs the fetch depth of its entire subtree. Every leg must contribute exactly
     * {@code rank_window_size} candidates so the fused window is complete and reproducible (see
     * {@link RetrieverBuilder#prepareLeaves(LeafPreparationContext)}). Push this node's window down to all children
     * and mark the subtree {@code fusionGoverned}, so descendant legs adopt it and reject an explicit
     * {@code size}. The window this node passes down is its own {@code rank_window_size}, independent of
     * any {@code inheritedWindow} from above — an inner fusion is its own scope for the depth it needs.
     */
    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        LeafPreparationContext childContext = context.underFusion(rankWindowSize);
        for (RetrieverBuilder child : children) {
            child.prepareLeaves(childContext);
        }
    }

    /**
     * Reciprocal Rank Fusion. For each child, a document's contribution is {@code 1/(rank_constant+rank)}
     * with {@code rank} its 1-based position in that child; the fused score sums those contributions across
     * children. Keyed by {@code (index, _id)}. Result is sorted by descending fused score, truncated to
     * {@code rank_window_size}, then (if set) filtered by fused {@code min_score}, with positions renumbered.
     */
    @Override
    protected List<RetrieverCandidate> fuse(List<List<RetrieverCandidate>> childResults) {
        // Accumulate fused score per (index, _id); keep one representative candidate to preserve identity.
        // LinkedHashMap for deterministic iteration on score ties (first-seen order).
        Map<String, Double> fusedScore = new LinkedHashMap<>();
        Map<String, RetrieverCandidate> representative = new LinkedHashMap<>();

        for (List<RetrieverCandidate> childResult : childResults) {
            int rank = 1; // 1-based rank within this child's ranking
            for (RetrieverCandidate candidate : childResult) {
                String key = fusionKey(candidate);
                double contribution = 1.0 / (rankConstant + rank);
                fusedScore.merge(key, contribution, Double::sum);
                representative.putIfAbsent(key, candidate);
                rank++;
            }
        }

        return finalizeFusion(fusedScore, representative, rankWindowSize, minScore);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        writeChildrenArray(builder, params, RETRIEVERS_FIELD);
        if (rankConstant != DEFAULT_RANK_CONSTANT) {
            builder.field(RANK_CONSTANT_FIELD, rankConstant);
        }
        if (rankWindowSize != DEFAULT_RANK_WINDOW_SIZE) {
            builder.field(RANK_WINDOW_SIZE_FIELD, rankWindowSize);
        }
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD, minScore);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Parse a RankFusionRetrieverBuilder from XContent. The parser is positioned inside the
     * {@code rank_fusion} object (past its START_OBJECT).
     */
    public static RankFusionRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        List<RetrieverBuilder> children = new ArrayList<>();
        Integer rankConstant = null;
        Integer rankWindowSize = null;
        Float minScore = null;

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (RETRIEVERS_FIELD.equals(fieldName) && token == XContentParser.Token.START_ARRAY) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    // each element is a { "<type>": {...} } object dispatched through the shared registry
                    children.add(RetrieverBuilder.parseInnerRetrieverBuilder(parser));
                }
            } else if (token.isValue()) {
                switch (fieldName) {
                    case RANK_CONSTANT_FIELD:
                        rankConstant = parser.intValue();
                        break;
                    case RANK_WINDOW_SIZE_FIELD:
                        rankWindowSize = parser.intValue();
                        break;
                    case MIN_SCORE_FIELD:
                        minScore = parser.floatValue();
                        break;
                    default:
                        throw new IllegalArgumentException("[" + NAME + "] unknown field [" + fieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("[" + NAME + "] unknown field [" + fieldName + "]");
            }
        }

        RankFusionRetrieverBuilder builder = new RankFusionRetrieverBuilder(children);
        if (rankConstant != null) {
            builder.setRankConstant(rankConstant);
        }
        if (rankWindowSize != null) {
            builder.setRankWindowSize(rankWindowSize);
        }
        if (minScore != null) {
            builder.setMinScore(minScore);
        }
        return builder;
    }
}
