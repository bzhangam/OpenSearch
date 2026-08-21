/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transformer retriever that re-scores the child's top-N results using a secondary query.
 * Combines the original score with the rescore query score using configurable weights.
 * <p>
 * This replaces top-level {@code rescore} (which is blocked with retrievers) and provides
 * positional control — can rescore at any level of the tree (one leg before fusion, or
 * fused results after fusion).
 * <p>
 * Final score = query_weight * original_score + rescore_query_weight * rescore_score
 *
 * @opensearch.internal
 */
public class RescoreRetrieverBuilder extends TransformerRetrieverBuilder {

    public static final String NAME = "rescore";
    public static final int DEFAULT_WINDOW_SIZE = 100;

    private QueryBuilder rescoreQuery;
    private int windowSize = DEFAULT_WINDOW_SIZE;
    private float queryWeight = 1.0f;
    private float rescoreQueryWeight = 1.0f;

    public RescoreRetrieverBuilder() {}

    public RescoreRetrieverBuilder(QueryBuilder rescoreQuery, RetrieverBuilder child) {
        this.rescoreQuery = rescoreQuery;
        this.childRetriever = child;
    }

    public QueryBuilder getRescoreQuery() {
        return rescoreQuery;
    }

    public void setRescoreQuery(QueryBuilder rescoreQuery) {
        this.rescoreQuery = rescoreQuery;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("[rescore] window_size must be positive, got " + windowSize);
        }
        this.windowSize = windowSize;
    }

    public float getQueryWeight() {
        return queryWeight;
    }

    public void setQueryWeight(float queryWeight) {
        this.queryWeight = queryWeight;
    }

    public float getRescoreQueryWeight() {
        return rescoreQueryWeight;
    }

    public void setRescoreQueryWeight(float rescoreQueryWeight) {
        this.rescoreQueryWeight = rescoreQueryWeight;
    }

    @Override
    public void validate(RetrieverContext context) {
        if (rescoreQuery == null) {
            throw new IllegalArgumentException("[rescore] requires [query] for rescoring");
        }
        if (childRetriever instanceof StandardRetrieverBuilder) {
            throw new IllegalArgumentException(
                "[rescore] retriever cannot directly wrap [standard]; "
                    + "define rescore inside the standard retriever's [rescore] field instead"
            );
        }
        super.validate(context);
    }

    /**
     * Rescore's own dispatch caps its result at {@code min(windowSize, childDocs.size())} (see
     * {@link #buildAsyncSearchRequest}) — it can shrink the child's window but never grow it, so
     * the {@link TransformerRetrieverBuilder} default (pass-through) is wrong here.
     */
    @Override
    public int getMaxOutputSize() {
        return Math.min(windowSize, childRetriever.getMaxOutputSize());
    }

    @Override
    protected List<RankedDoc> reshape(List<RankedDoc> childResult) {
        // If the async rescore round has completed, use its results. Otherwise pass the child's
        // results through — resolve() marks this node resolved, needsAsyncResolution() then reports
        // true, and the executor dispatches the rescore round and re-resolves this node.
        if (rescoreResults != null) {
            return rescoreResults;
        }
        return childResult;
    }

    /** Results from the rescore sub-search dispatch, set by the executor via {@link #setAsyncSearchResult}. */
    private List<RankedDoc> rescoreResults;

    // --- Generic async resolution contract ---

    @Override
    public boolean needsAsyncResolution() {
        // Needs the rescore round once we've resolved (passed through) but haven't rescored yet.
        return resolved && rescoreResults == null;
    }

    @Override
    public SearchRequest buildAsyncSearchRequest(String[] indices, SearchRequest originalRequest) {
        // Build a SearchRequest with RankDocsQuery (child's resolved docs) + RescoreBuilder
        List<RankedDoc> childDocs = childRetriever.getResolvedResult();
        int effectiveWindowSize = Math.min(windowSize, childDocs != null ? childDocs.size() : 0);

        RankDocsQueryBuilder rankDocsQuery = new RankDocsQueryBuilder(childDocs != null ? childDocs : List.of());

        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder()
            .query(rankDocsQuery)
            .size(effectiveWindowSize);

        org.opensearch.search.rescore.QueryRescorerBuilder rescorer =
            new org.opensearch.search.rescore.QueryRescorerBuilder(rescoreQuery);
        rescorer.windowSize(effectiveWindowSize);
        rescorer.setQueryWeight(queryWeight);
        rescorer.setRescoreQueryWeight(rescoreQueryWeight);
        source.addRescorer(rescorer);

        SearchRequest req = new SearchRequest(indices);
        req.source(source);
        if (originalRequest != null) {
            req.preference(originalRequest.preference());
            req.routing(originalRequest.routing());
            if (originalRequest.source() != null) {
                if (originalRequest.source().pointInTimeBuilder() != null) {
                    source.pointInTimeBuilder(originalRequest.source().pointInTimeBuilder());
                }
                if (originalRequest.source().timeout() != null) {
                    source.timeout(originalRequest.source().timeout());
                }
            }
        }
        return req;
    }

    @Override
    public void setAsyncSearchResult(List<RankedDoc> result) {
        this.rescoreResults = result;
        // Mark this node dirty so the next resolve() pass recomputes it (reshape now returns the
        // rescored results) and propagates the change to any ancestors — without re-fusing subtrees
        // whose inputs are unchanged.
        this.resolved = false;
    }

    /** Timing: rescore dispatch time. */
    private long rescoreDispatchTimeNanos;

    public void setRescoreDispatchTimeNanos(long nanos) {
        this.rescoreDispatchTimeNanos = nanos;
    }

    @Override
    protected Explanation buildReshapeExplanation(String docId, String docIndex, Explanation childExplanation) {
        // Find the doc's position in child results to check if it was in the rescore window
        List<RankedDoc> childResult = childRetriever.getResolvedResult();
        int childPosition = -1;
        float originalScore = 0.0f;
        if (childResult != null) {
            for (int i = 0; i < childResult.size(); i++) {
                if (childResult.get(i).id().equals(docId) && childResult.get(i).index().equals(docIndex)) {
                    childPosition = i;
                    originalScore = childResult.get(i).score();
                    break;
                }
            }
        }

        if (childPosition >= 0 && childPosition < windowSize) {
            // Doc was inside the rescore window
            float finalScore = findDocScore(docId, docIndex);
            float rescoreContribution = (finalScore - queryWeight * originalScore) / rescoreQueryWeight;

            Explanation originalExplain = Explanation.match(
                queryWeight * originalScore,
                queryWeight + " × original_score(" + String.format("%.6f", originalScore) + ")",
                childExplanation != null ? List.of(childExplanation) : List.of()
            );
            Explanation rescoreExplain = Explanation.match(
                rescoreQueryWeight * rescoreContribution,
                rescoreQueryWeight + " × rescore_query_score(" + String.format("%.6f", rescoreContribution) + ")"
            );

            return Explanation.match(
                finalScore,
                "rescore [query_weight=" + queryWeight + ", rescore_query_weight=" + rescoreQueryWeight
                    + ", window_size=" + windowSize + "]",
                List.of(originalExplain, rescoreExplain)
            );
        } else {
            // Doc outside rescore window — score preserved
            return Explanation.match(
                originalScore,
                "outside rescore window (position " + (childPosition + 1) + ", window_size=" + windowSize
                    + "), original score preserved",
                childExplanation != null ? List.of(childExplanation) : List.of()
            );
        }
    }

    private float findDocScore(String docId, String docIndex) {
        if (resolvedResult != null) {
            for (RankedDoc doc : resolvedResult) {
                if (doc.id().equals(docId) && doc.index().equals(docIndex)) {
                    return doc.score();
                }
            }
        }
        return 0.0f;
    }

    @Override
    public RetrieverProfile buildProfile() {
        RetrieverProfile childProfile = childRetriever.buildProfile();
        RetrieverProfile.Builder builder = new RetrieverProfile.Builder(getName())
            .totalTimeNanos(getElapsedNanos())
            .child(childProfile);
        if (rescoreDispatchTimeNanos > 0) {
            builder.addBreakdown("rescore_dispatch_time_in_nanos", rescoreDispatchTimeNanos);
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
        builder.field("window_size", windowSize);
        builder.field("query_weight", queryWeight);
        builder.field("rescore_query_weight", rescoreQueryWeight);
        builder.startObject("query");
        rescoreQuery.toXContent(builder, params);
        builder.endObject();
        builder.startObject("retriever");
        childRetriever.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Parse from XContent.
     * Expected: { "window_size": 100, "query": {...}, "query_weight": 0.7, "rescore_query_weight": 1.2, "retriever": {...} }
     * The child under "retriever" may be any registered retriever type — including nested
     * compound/transformer retrievers — dispatched via the shared registry.
     */
    public static RescoreRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        RescoreRetrieverBuilder builder = new RescoreRetrieverBuilder();

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                switch (fieldName) {
                    case "window_size":
                        builder.setWindowSize(parser.intValue());
                        break;
                    case "query_weight":
                        builder.setQueryWeight(parser.floatValue());
                        break;
                    case "rescore_query_weight":
                        builder.setRescoreQueryWeight(parser.floatValue());
                        break;
                    case "query":
                        // Full query DSL — same parsing StandardRetrieverBuilder uses for its own
                        // "query"/"filter" fields, so any query type (bool, match_all, knn, ...) works.
                        builder.setRescoreQuery(org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder(parser));
                        break;
                    case "retriever":
                        builder.setChildRetriever(RetrieverBuilder.parseInnerRetrieverBuilder(parser));
                        break;
                    default:
                        throw new IllegalArgumentException("[rescore] unknown field [" + fieldName + "]");
                }
            }
        }

        return builder;
    }
}
