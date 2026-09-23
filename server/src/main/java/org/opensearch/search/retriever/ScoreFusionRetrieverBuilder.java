/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A compound retriever that fuses its children's rankings by <b>score</b>: each child's raw {@code _score}s
 * are min-max normalized within that child, then combined into a fused score with a <b>weighted arithmetic
 * mean</b> over the children that ranked the document. This is the most widely used score-fusion technique
 * and mirrors the classic hybrid query's {@code min_max} normalization + {@code arithmetic_mean} combination.
 * <p>
 * <b>Normalization</b> (per child, over its returned candidates): {@code (score - min) / (max - min)}. If all
 * of a child's scores are equal ({@code min == max}) every doc normalizes to {@code 1.0}. A normalized value
 * of exactly {@code 0.0} (the child's minimum doc) is floored to {@link #MIN_NORMALIZED_SCORE} so the lowest
 * doc still contributes.
 * <p>
 * <b>Combination</b>: {@code fused = sum(weight_i * norm_i) / sum(weight_i)} over <b>all</b> children — a
 * child that did not rank the document contributes {@code 0} to the numerator, but its weight still counts
 * in the denominator (matching the classic hybrid query's arithmetic mean). Default weight is {@code 1.0}
 * per child, so with equal weights a doc present in only one of two legs scores {@code norm/2}.
 * <p>
 * Documents are keyed by {@code (index, _id)}. The fused ranking is sorted by descending fused score,
 * truncated to {@code rank_window_size}, then (if set) filtered by a fused {@code min_score}.
 * <p>
 * <b>Configuring normalization and combination.</b> Both {@code normalization} and {@code combination}
 * accept either a bare technique string ({@code "normalization": "min_max"}) or an object carrying the
 * technique and its parameters ({@code "combination": {"technique": "arithmetic_mean", "parameters":
 * {"weights": [2.0, 1.0]}}}). Per-leg {@code weights} are a parameter of the combination technique and live
 * under {@code combination.parameters.weights} (one weight per child; defaults to {@code 1.0} each). Only
 * {@code min_max} normalization and {@code arithmetic_mean} combination are implemented today; the object
 * forms exist so future techniques and parameters can be added without a breaking wire-format change. The
 * object forms are canonical on output.
 *
 * @opensearch.api
 */
public class ScoreFusionRetrieverBuilder extends CompoundRetrieverBuilder {

    public static final String NAME = "score_fusion";

    public static final int DEFAULT_RANK_WINDOW_SIZE = 100;
    public static final float DEFAULT_WEIGHT = 1.0f;
    /** The only normalization technique implemented today. */
    public static final String MIN_MAX = "min_max";
    /** The only combination technique implemented today. */
    public static final String ARITHMETIC_MEAN = "arithmetic_mean";
    /** Floor for a normalized score that computes to exactly 0.0 (matches the classic hybrid convention). */
    static final float MIN_NORMALIZED_SCORE = 0.001f;
    private static final float NORMALIZED_SINGLE_SCORE = 1.0f;

    static final String RETRIEVERS_FIELD = "retrievers";
    static final String WEIGHTS_FIELD = "weights";
    static final String NORMALIZATION_FIELD = "normalization";
    static final String COMBINATION_FIELD = "combination";
    static final String TECHNIQUE_FIELD = "technique";
    static final String PARAMETERS_FIELD = "parameters";
    static final String RANK_WINDOW_SIZE_FIELD = "rank_window_size";
    static final String MIN_SCORE_FIELD = "min_score";

    private float[] weights;
    private String normalization = MIN_MAX;
    private String combination = ARITHMETIC_MEAN;
    private int rankWindowSize = DEFAULT_RANK_WINDOW_SIZE;
    private Float minScore;

    public ScoreFusionRetrieverBuilder(List<RetrieverBuilder> children) {
        super(children);
    }

    public float[] getWeights() {
        return weights;
    }

    public void setWeights(float[] weights) {
        this.weights = weights;
    }

    public String getNormalization() {
        return normalization;
    }

    public void setNormalization(String normalization) {
        if (MIN_MAX.equals(normalization) == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] [" + NORMALIZATION_FIELD + "] only supports [" + MIN_MAX + "], got [" + normalization + "]"
            );
        }
        this.normalization = normalization;
    }

    public String getCombination() {
        return combination;
    }

    public void setCombination(String combination) {
        if (ARITHMETIC_MEAN.equals(combination) == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] [" + COMBINATION_FIELD + "] only supports [" + ARITHMETIC_MEAN + "], got [" + combination + "]"
            );
        }
        this.combination = combination;
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

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void validate() {
        super.validate();
        if (weights != null && weights.length != children.size()) {
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] ["
                    + COMBINATION_FIELD
                    + "."
                    + PARAMETERS_FIELD
                    + "."
                    + WEIGHTS_FIELD
                    + "] length ("
                    + weights.length
                    + ") must match the number of [retrievers] ("
                    + children.size()
                    + ")"
            );
        }
    }

    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        LeafPreparationContext childContext = context.underFusion(rankWindowSize);
        for (RetrieverBuilder child : children) {
            child.prepareLeaves(childContext);
        }
    }

    /**
     * Min-max normalize each child's scores, then combine with a weighted arithmetic mean over the children
     * that ranked each document. Keyed by {@code (index, _id)}; the result is finalized (sorted, truncated to
     * {@code rank_window_size}, {@code min_score}-filtered, renumbered) by the shared {@link #finalizeFusion}.
     */
    @Override
    protected List<RetrieverCandidate> fuse(List<List<RetrieverCandidate>> childResults) {
        // Weighted arithmetic mean matching the classic hybrid: a document's fused score is
        // sum(weight_i * norm_i) / sum(weight_i) over ALL legs, where a leg the document is absent from
        // contributes 0 to the numerator but its weight still counts in the denominator. So the denominator
        // is the total weight of every leg (constant across documents), not just the legs that ranked the doc.
        double totalWeight = 0.0;
        for (int childIndex = 0; childIndex < childResults.size(); childIndex++) {
            totalWeight += weightFor(childIndex);
        }

        Map<String, Double> weightedScoreSum = new LinkedHashMap<>();
        Map<String, RetrieverCandidate> representative = new LinkedHashMap<>();

        for (int childIndex = 0; childIndex < childResults.size(); childIndex++) {
            List<RetrieverCandidate> childResult = childResults.get(childIndex);
            double weight = weightFor(childIndex);
            float[] minMax = minMax(childResult);
            float min = minMax[0];
            float max = minMax[1];

            for (RetrieverCandidate candidate : childResult) {
                String key = fusionKey(candidate);
                double norm = normalize(candidate.score(), min, max);
                weightedScoreSum.merge(key, weight * norm, Double::sum);
                representative.putIfAbsent(key, candidate);
            }
        }

        // Reduce to the fused (weighted-mean) score per key, dividing by the total leg weight.
        Map<String, Double> fusedScore = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : weightedScoreSum.entrySet()) {
            fusedScore.put(entry.getKey(), totalWeight == 0.0 ? 0.0 : entry.getValue() / totalWeight);
        }

        return finalizeFusion(fusedScore, representative, rankWindowSize, minScore);
    }

    /**
     * Score-fusion explanation: a root describing the min_max normalization + weighted arithmetic-mean
     * combination, with one detail per child. For a child that ranked the document, the detail shows
     * {@code norm=<v> (raw=<r>, min=<mn>, max=<mx>), weight=<w>} with the child's own explanation nested
     * beneath; for a child that did not, a zero-valued "not present" node. The per-leg min/max and the
     * document's raw score are recomputed from each child's resolved output, matching {@link #fuse}.
     */
    @Override
    protected Explanation buildFusionExplanation(String index, String id, float fusedScore) {
        List<Explanation> legDetails = new ArrayList<>(children.size());
        for (int legIndex = 0; legIndex < children.size(); legIndex++) {
            RetrieverBuilder child = children.get(legIndex);
            double weight = weightFor(legIndex);
            List<RetrieverCandidate> childResult = child.getResolvedResult();
            RetrieverCandidate docInLeg = findCandidate(childResult, index, id);
            if (docInLeg == null) {
                legDetails.add(Explanation.noMatch("leg " + legIndex + ": not present (weight=" + (float) weight + ")"));
                continue;
            }
            float[] minMax = minMax(childResult);
            double norm = normalize(docInLeg.score(), minMax[0], minMax[1]);
            double weighted = weight * norm;
            String description = "leg "
                + legIndex
                + ": norm="
                + (float) norm
                + " (raw="
                + docInLeg.score()
                + ", min="
                + minMax[0]
                + ", max="
                + minMax[1]
                + "), weight="
                + (float) weight;
            legDetails.add(Explanation.match((float) weighted, description, getChildExplanation(child, index, id)));
        }
        return Explanation.match(
            fusedScore,
            "score_fusion(" + normalization + ", " + combination + ") [normalized by total leg weight]",
            legDetails
        );
    }

    /** The candidate for {@code (index,id)} in a child's resolved output, or {@code null} if absent. */
    private static RetrieverCandidate findCandidate(List<RetrieverCandidate> childResult, String index, String id) {
        if (childResult == null) {
            return null;
        }
        for (RetrieverCandidate candidate : childResult) {
            if (candidate.index().equals(index) && candidate.id().equals(id)) {
                return candidate;
            }
        }
        return null;
    }

    private double weightFor(int childIndex) {
        if (weights == null) {
            return DEFAULT_WEIGHT;
        }
        return weights[childIndex];
    }

    /** [min, max] of a child's candidate scores; [0,0] for an empty child (unused — no candidates to normalize). */
    private static float[] minMax(List<RetrieverCandidate> childResult) {
        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;
        for (RetrieverCandidate candidate : childResult) {
            float s = candidate.score();
            if (s < min) {
                min = s;
            }
            if (s > max) {
                max = s;
            }
        }
        return new float[] { min, max };
    }

    /** Min-max normalization matching the classic hybrid convention (all-equal -> 1.0; floor exact 0.0). */
    private static double normalize(float score, float min, float max) {
        if (max == min) {
            return NORMALIZED_SINGLE_SCORE;
        }
        double norm = (score - min) / (double) (max - min);
        return norm == 0.0 ? MIN_NORMALIZED_SCORE : norm;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        writeChildrenArray(builder, params, RETRIEVERS_FIELD);
        // Emit the canonical object forms for normalization and combination: {"technique": "<technique>"},
        // with per-leg weights (when set) under combination.parameters.weights. Always written (even for the
        // defaults) so the round-trip is unambiguous, and forward-compatible with future techniques/params.
        builder.startObject(NORMALIZATION_FIELD);
        builder.field(TECHNIQUE_FIELD, normalization);
        builder.endObject();
        builder.startObject(COMBINATION_FIELD);
        builder.field(TECHNIQUE_FIELD, combination);
        if (weights != null) {
            builder.startObject(PARAMETERS_FIELD);
            builder.startArray(WEIGHTS_FIELD);
            for (float w : weights) {
                builder.value(w);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
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
     * Parse a ScoreFusionRetrieverBuilder from XContent. The parser is positioned inside the
     * {@code score_fusion} object (past its START_OBJECT).
     */
    public static ScoreFusionRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        List<RetrieverBuilder> children = new ArrayList<>();
        List<Float> weights = null;
        String normalization = null;
        String combination = null;
        Integer rankWindowSize = null;
        Float minScore = null;

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (RETRIEVERS_FIELD.equals(fieldName) && token == XContentParser.Token.START_ARRAY) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    children.add(RetrieverBuilder.parseInnerRetrieverBuilder(parser));
                }
            } else if (NORMALIZATION_FIELD.equals(fieldName) && token == XContentParser.Token.START_OBJECT) {
                normalization = parseTechniqueObject(parser, NORMALIZATION_FIELD, false).technique;
            } else if (COMBINATION_FIELD.equals(fieldName) && token == XContentParser.Token.START_OBJECT) {
                TechniqueSpec spec = parseTechniqueObject(parser, COMBINATION_FIELD, true);
                combination = spec.technique;
                weights = spec.weights;
            } else if (token.isValue()) {
                switch (fieldName) {
                    case NORMALIZATION_FIELD:
                        normalization = parser.text();
                        break;
                    case COMBINATION_FIELD:
                        combination = parser.text();
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

        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder(children);
        if (weights != null) {
            float[] w = new float[weights.size()];
            for (int i = 0; i < w.length; i++) {
                w[i] = weights.get(i);
            }
            builder.setWeights(w);
        }
        if (normalization != null) {
            builder.setNormalization(normalization);
        }
        if (combination != null) {
            builder.setCombination(combination);
        }
        if (rankWindowSize != null) {
            builder.setRankWindowSize(rankWindowSize);
        }
        if (minScore != null) {
            builder.setMinScore(minScore);
        }
        return builder;
    }

    /** Parsed technique object: the {@code technique} name plus (for combination) any {@code weights}. */
    private static final class TechniqueSpec {
        final String technique;
        final List<Float> weights;

        TechniqueSpec(String technique, List<Float> weights) {
            this.technique = technique;
            this.weights = weights;
        }
    }

    /**
     * Parse the object form of a technique field: {@code {"technique": "<name>", "parameters": {...}}}. The
     * parser is positioned on the object's START_OBJECT. {@code technique} is required. When
     * {@code allowWeights} is true (the {@code combination} field), a {@code parameters.weights} array is
     * accepted and returned; otherwise (the {@code normalization} field) a non-empty {@code parameters}
     * object is rejected rather than silently ignored. Unknown keys throw. The technique name's validity is
     * enforced later by the corresponding setter.
     */
    private static TechniqueSpec parseTechniqueObject(XContentParser parser, String field, boolean allowWeights) throws IOException {
        String technique = null;
        List<Float> weights = null;
        boolean hasOtherParameters = false;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (TECHNIQUE_FIELD.equals(fieldName) && token.isValue()) {
                technique = parser.text();
            } else if (PARAMETERS_FIELD.equals(fieldName) && token == XContentParser.Token.START_OBJECT) {
                String paramName = null;
                XContentParser.Token paramToken;
                while ((paramToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (paramToken == XContentParser.Token.FIELD_NAME) {
                        paramName = parser.currentName();
                    } else if (allowWeights && WEIGHTS_FIELD.equals(paramName) && paramToken == XContentParser.Token.START_ARRAY) {
                        weights = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            weights.add(parser.floatValue());
                        }
                    } else {
                        hasOtherParameters = true;
                        parser.skipChildren();
                    }
                }
            } else {
                throw new IllegalArgumentException("[" + NAME + "] [" + field + "] unknown field [" + fieldName + "]");
            }
        }
        if (technique == null) {
            throw new IllegalArgumentException("[" + NAME + "] [" + field + "] requires a [" + TECHNIQUE_FIELD + "]");
        }
        if (hasOtherParameters) {
            throw new IllegalArgumentException(
                "[" + NAME + "] [" + field + "] technique [" + technique + "] does not support the given [" + PARAMETERS_FIELD + "]"
            );
        }
        return new TechniqueSpec(technique, weights);
    }
}
