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
import org.opensearch.search.retriever.constraints.DisallowTrackScoresFalse;
import org.opensearch.search.retriever.modifiers.ForceTrackScores;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compound retriever that normalizes per-leg scores and combines them using a configurable technique.
 * Supports per-leg weights, multiple normalization techniques (min_max, l2, z_score, none), and
 * combination techniques (arithmetic_mean, harmonic_mean, geometric_mean).
 *
 * @opensearch.internal
 */
public class ScoreFusionRetrieverBuilder extends CompoundRetrieverBuilder {

    public static final String NAME = "score_fusion";

    private float[] weights;  // parallel to children; null = equal
    private String normalization = "min_max";
    private String combination = "arithmetic_mean";

    public ScoreFusionRetrieverBuilder() {}

    public ScoreFusionRetrieverBuilder(List<RetrieverBuilder> children) {
        this.childRetrievers = children;
    }

    public void setWeights(float[] weights) {
        this.weights = weights;
    }

    public float[] getWeights() {
        return weights;
    }

    public void setNormalization(String normalization) {
        this.normalization = normalization;
    }

    public String getNormalization() {
        return normalization;
    }

    public void setCombination(String combination) {
        this.combination = combination;
    }

    public String getCombination() {
        return combination;
    }

    @Override
    public void validate(RetrieverContext context) {
        if (childRetrievers == null || childRetrievers.size() < 2) {
            throw new IllegalArgumentException("[score_fusion] requires at least 2 child retrievers");
        }
        if (weights != null && weights.length != childRetrievers.size()) {
            throw new IllegalArgumentException(
                "[score_fusion] weights length [" + weights.length + "] must match retrievers length [" + childRetrievers.size() + "]"
            );
        }
        switch (normalization) {
            case "min_max":
            case "l2":
            case "z_score":
            case "none":
                break;
            default:
                throw new IllegalArgumentException(
                    "[score_fusion] unknown normalization [" + normalization + "]; supported: [min_max, l2, z_score, none]"
                );
        }
        switch (combination) {
            case "arithmetic_mean":
            case "harmonic_mean":
            case "geometric_mean":
                break;
            default:
                throw new IllegalArgumentException(
                    "[score_fusion] unknown combination [" + combination + "]; supported: [arithmetic_mean, harmonic_mean, geometric_mean]"
                );
        }
        // score_fusion needs raw scores from leaves — disallow explicit track_scores=false
        RetrieverContext childContext = context.withConstraint(new DisallowTrackScoresFalse("score_fusion"));
        for (RetrieverBuilder child : childRetrievers) {
            child.validate(childContext);
        }
    }

    @Override
    public void prepareLeaves(RetrieverContext context) {
        // Force track_scores on all leaves so we get raw scores for normalization
        RetrieverContext childContext = context.withModifier(ForceTrackScores.INSTANCE);
        for (RetrieverBuilder child : childRetrievers) {
            child.prepareLeaves(childContext);
        }
    }

    @Override
    protected List<RankedDoc> fuse(List<List<RankedDoc>> childResults) {
        int numLegs = childResults.size();
        float[] effectiveWeights = weights != null ? weights : equalWeights(numLegs);

        // Per-leg normalization stats + per-leg raw-score lookup, computed once from the child results.
        LegStats[] stats = new LegStats[numLegs];
        List<Map<String, Float>> legRawScores = new ArrayList<>(numLegs);
        Map<String, RankedDoc> docLookup = new HashMap<>();

        for (int leg = 0; leg < numLegs; leg++) {
            LegStats s = new LegStats();
            Map<String, Float> raw = new HashMap<>();
            List<RankedDoc> legDocs = childResults.get(leg);
            if (legDocs != null) {
                for (RankedDoc doc : legDocs) {
                    String docKey = doc.index() + "|" + doc.id();
                    raw.put(docKey, doc.score());
                    s.add(doc.score());
                    docLookup.putIfAbsent(docKey, doc);
                }
            }
            s.finish();
            stats[leg] = s;
            legRawScores.add(raw);
        }

        // Normalize per leg, combine with weights. A doc absent from a leg contributes a normalized 0
        // (arithmetic mean includes it; harmonic/geometric exclude non-positive scores — see combine*).
        Map<String, Float> fusedScores = new HashMap<>();
        for (String docKey : docLookup.keySet()) {
            float[] normalized = new float[numLegs];
            for (int leg = 0; leg < numLegs; leg++) {
                Float raw = legRawScores.get(leg).get(docKey);
                normalized[leg] = raw != null ? normalizeOne(raw, stats[leg]) : 0.0f;
            }
            fusedScores.put(docKey, combineScores(normalized, effectiveWeights));
        }

        // Apply min_score filter
        if (minScore != null) {
            fusedScores.entrySet().removeIf(e -> e.getValue() < minScore);
        }

        // Sort descending, take top rank_window_size
        List<Map.Entry<String, Float>> sorted = new ArrayList<>(fusedScores.entrySet());
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

    /**
     * Per-leg statistics needed by the normalization techniques, accumulated over that leg's raw scores.
     */
    private static final class LegStats {
        private float min = Float.MAX_VALUE;
        private float max = -Float.MAX_VALUE;
        private double sum = 0.0;
        private double sumSq = 0.0;
        private int count = 0;
        private float l2;
        private float mean;
        private float std;

        void add(float score) {
            min = Math.min(min, score);
            max = Math.max(max, score);
            sum += score;
            sumSq += (double) score * score;
            count++;
        }

        void finish() {
            l2 = (float) Math.sqrt(sumSq);
            mean = count > 0 ? (float) (sum / count) : 0.0f;
            double variance = count > 0 ? (sumSq / count) - ((double) mean * mean) : 0.0;
            std = variance > 0.0 ? (float) Math.sqrt(variance) : 0.0f;
        }
    }

    /**
     * Normalize a single raw score for one leg using the configured technique. Shared by {@link #fuse}
     * and {@link #buildFusionExplanation} so scoring and explain never diverge.
     */
    private float normalizeOne(float raw, LegStats st) {
        switch (normalization) {
            case "min_max":
                float range = st.max - st.min;
                return range == 0.0f ? 1.0f : (raw - st.min) / range;
            case "l2":
                return st.l2 == 0.0f ? 0.0f : raw / st.l2;
            case "z_score":
                // Zero-mean, unit-variance. A degenerate leg (all scores equal → std 0) normalizes to 0.
                return st.std == 0.0f ? 0.0f : (raw - st.mean) / st.std;
            case "none":
                return raw;
            default:
                throw new IllegalArgumentException("[score_fusion] unknown normalization technique [" + normalization + "]");
        }
    }

    private float combineScores(float[] normalizedScores, float[] weights) {
        switch (combination) {
            case "arithmetic_mean":
                return arithmeticMean(normalizedScores, weights);
            case "harmonic_mean":
                return harmonicMean(normalizedScores, weights);
            case "geometric_mean":
                return geometricMean(normalizedScores, weights);
            default:
                throw new IllegalArgumentException("[score_fusion] unknown combination technique [" + combination + "]");
        }
    }

    private float arithmeticMean(float[] scores, float[] weights) {
        // weighted average: sum(w_i * s_i) / sum(w_i) — only for legs where doc is present (score >= 0)
        // After normalization, 0.0 is a valid score (lowest in min-max range), so we include it.
        // NaN was already replaced with 0.0 for absent docs during normalization, but those entries
        // were originally NaN (not present in leg). We use a separate tracking approach:
        // all NaN values were set to 0.0 during normalization. Since we can't distinguish
        // "was NaN (absent)" from "normalized to 0.0 (present but lowest)", we include all.
        // This matches the neural-search behavior where 0-score docs participate in the mean.
        float weightedSum = 0.0f;
        float sumOfWeights = 0.0f;
        for (int i = 0; i < scores.length; i++) {
            weightedSum += weights[i] * scores[i];
            sumOfWeights += weights[i];
        }
        return sumOfWeights == 0.0f ? 0.0f : weightedSum / sumOfWeights;
    }

    private float harmonicMean(float[] scores, float[] weights) {
        // harmonic mean: sum(w_i) / sum(w_i / s_i) — only for legs where score > 0
        // (harmonic mean is undefined for 0 values, so we must exclude them)
        float sumOfWeights = 0.0f;
        float sumOfWeightOverScore = 0.0f;
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] > 0.0f) {
                sumOfWeights += weights[i];
                sumOfWeightOverScore += weights[i] / scores[i];
            }
        }
        return sumOfWeightOverScore == 0.0f ? 0.0f : sumOfWeights / sumOfWeightOverScore;
    }

    private float geometricMean(float[] scores, float[] weights) {
        // geometric mean: exp(sum(w_i * ln(s_i)) / sum(w_i)) — only for legs where score > 0
        // (log(0) is undefined, so we must exclude zero scores)
        double logSum = 0.0;
        float sumOfWeights = 0.0f;
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] > 0.0f) {
                logSum += weights[i] * Math.log(scores[i]);
                sumOfWeights += weights[i];
            }
        }
        return sumOfWeights == 0.0f ? 0.0f : (float) Math.exp(logSum / sumOfWeights);
    }

    private static float[] equalWeights(int n) {
        float[] w = new float[n];
        Arrays.fill(w, 1.0f);
        return w;
    }

    /** Timing: normalization computation time. */
    private long normalizationTimeNanos;
    /** Timing: combination computation time. */
    private long combinationTimeNanos;

    public void setNormalizationTimeNanos(long nanos) {
        this.normalizationTimeNanos = nanos;
    }

    public void setCombinationTimeNanos(long nanos) {
        this.combinationTimeNanos = nanos;
    }

    @Override
    protected Explanation buildFusionExplanation(String docId, String docIndex) {
        int numLegs = childRetrievers.size();
        float[] effectiveWeights = weights != null ? weights : equalWeights(numLegs);
        List<Explanation> legDetails = new ArrayList<>();

        // Recompute per-leg stats and this doc's raw score per leg from the resolved child results,
        // using the SAME helpers as fuse() so explain never diverges from scoring.
        float[] rawScores = new float[numLegs];
        boolean[] present = new boolean[numLegs];
        Arrays.fill(rawScores, Float.NaN);
        LegStats[] stats = new LegStats[numLegs];

        for (int leg = 0; leg < numLegs; leg++) {
            LegStats s = new LegStats();
            List<RankedDoc> childResult = childRetrievers.get(leg).getResolvedResult();
            if (childResult != null) {
                for (RankedDoc doc : childResult) {
                    s.add(doc.score());
                    if (doc.id().equals(docId) && doc.index().equals(docIndex)) {
                        rawScores[leg] = doc.score();
                        present[leg] = true;
                    }
                }
            }
            s.finish();
            stats[leg] = s;
        }

        float[] normalizedScores = new float[numLegs];
        for (int leg = 0; leg < numLegs; leg++) {
            if (present[leg]) {
                float normalized = normalizeOne(rawScores[leg], stats[leg]);
                normalizedScores[leg] = normalized;
                String normDesc = normDescription(leg, rawScores[leg], normalized, stats[leg]);
                Explanation childExplain = getChildExplanation(childRetrievers.get(leg), docId, docIndex);
                float weightedContribution = effectiveWeights[leg] * normalized;
                legDetails.add(
                    Explanation.match(
                        weightedContribution,
                        "leg " + leg + ": " + normDesc + ", weight=" + effectiveWeights[leg],
                        childExplain != null ? List.of(childExplain) : List.of()
                    )
                );
            } else {
                normalizedScores[leg] = 0.0f;
                legDetails.add(Explanation.noMatch("leg " + leg + ": not present (contribution: 0)"));
            }
        }

        float fusedScore = combineScores(normalizedScores, effectiveWeights);

        return Explanation.match(
            fusedScore,
            "score_fusion(" + normalization + ", " + combination + ")"
                + (weights != null ? " [weights=" + Arrays.toString(weights) + "]" : ""),
            legDetails
        );
    }

    /** Human-readable normalization detail for a leg's explain entry. */
    private String normDescription(int leg, float raw, float normalized, LegStats st) {
        switch (normalization) {
            case "min_max":
                return "norm=" + String.format("%.4f", normalized)
                    + " (raw=" + String.format("%.4f", raw)
                    + ", min=" + String.format("%.4f", st.min)
                    + ", max=" + String.format("%.4f", st.max) + ")";
            case "l2":
                return "norm=" + String.format("%.4f", normalized)
                    + " (raw=" + String.format("%.4f", raw)
                    + ", l2_norm=" + String.format("%.4f", st.l2) + ")";
            case "z_score":
                return "norm=" + String.format("%.4f", normalized)
                    + " (raw=" + String.format("%.4f", raw)
                    + ", mean=" + String.format("%.4f", st.mean)
                    + ", stddev=" + String.format("%.4f", st.std) + ")";
            default:
                return "raw=" + String.format("%.4f", raw) + " (no normalization)";
        }
    }

    @Override
    public RetrieverProfile buildProfile() {
        List<RetrieverProfile> legProfiles = new ArrayList<>();
        for (RetrieverBuilder child : childRetrievers) {
            legProfiles.add(child.buildProfile());
        }
        RetrieverProfile.Builder builder = new RetrieverProfile.Builder(getName())
            .totalTimeNanos(getElapsedNanos())
            .legs(legProfiles);
        if (normalizationTimeNanos > 0) {
            builder.addBreakdown("normalization_time_in_nanos", normalizationTimeNanos);
        }
        if (combinationTimeNanos > 0) {
            builder.addBreakdown("combination_time_in_nanos", combinationTimeNanos);
        }
        return builder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("normalization", normalization);
        builder.field("combination", combination);
        builder.field("rank_window_size", rankWindowSize);
        if (weights != null) {
            builder.startArray("weights");
            for (float w : weights) {
                builder.value(w);
            }
            builder.endArray();
        }
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
     * Parse from XContent.
     * Each entry in "retrievers" may be any registered retriever type — including nested
     * compound/transformer retrievers, not just "standard" — dispatched via the shared registry.
     */
    public static ScoreFusionRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        ScoreFusionRetrieverBuilder builder = new ScoreFusionRetrieverBuilder();
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
                    case "weights":
                        List<Float> weightList = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            weightList.add(parser.floatValue());
                        }
                        float[] w = new float[weightList.size()];
                        for (int i = 0; i < w.length; i++) w[i] = weightList.get(i);
                        builder.setWeights(w);
                        break;
                    case "normalization":
                        builder.setNormalization(parser.text());
                        break;
                    case "combination":
                        builder.setCombination(parser.text());
                        break;
                    case "rank_window_size":
                        builder.setRankWindowSize(parser.intValue());
                        break;
                    case "min_score":
                        builder.setMinScore(parser.floatValue());
                        break;
                    default:
                        throw new IllegalArgumentException("[score_fusion] unknown field [" + fieldName + "]");
                }
            }
        }

        builder.childRetrievers = children;
        return builder;
    }
}
