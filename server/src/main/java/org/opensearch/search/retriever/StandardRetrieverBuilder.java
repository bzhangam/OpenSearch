/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.annotation.ExperimentalApi;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The leaf retriever that wraps a standard OpenSearch query with optional result-set operations.
 * This is the bridge between the retriever tree and the existing query DSL.
 * <p>
 * Every retriever tree terminates at {@code standard} leaves which are dispatched as independent
 * sub-searches.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class StandardRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "standard";

    private QueryBuilder queryBuilder;
    private QueryBuilder filterBuilder;
    private List<SortBuilder<?>> sorts;
    private Object[] searchAfter;
    private CollapseBuilder collapse;
    private Float minScore;
    private int from = 0;
    private int size = 100;
    private Boolean trackScores;
    private List<FieldAndFormat> docvalueFields;
    private List<org.opensearch.search.rescore.RescorerBuilder> rescorers;

    // Set by RetrieverExecutor after dispatch
    private List<RankedDoc> searchResult;
    // Shard-level profile results from the leg sub-search (when profile=true)
    private java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> legShardProfiles;

    public StandardRetrieverBuilder() {}

    public StandardRetrieverBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    // --- Getters and setters ---

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public void setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public QueryBuilder getFilterBuilder() {
        return filterBuilder;
    }

    public void setFilterBuilder(QueryBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
    }

    public List<SortBuilder<?>> getSorts() {
        return sorts;
    }

    public void setSorts(List<SortBuilder<?>> sorts) {
        this.sorts = sorts;
    }

    public Object[] getSearchAfter() {
        return searchAfter;
    }

    /**
     * Sets the search_after cursor for this leg. Requires an explicit {@link #setSorts} to be set
     * (a stored-field sort with a tiebreaker) — the cursor values must correspond to that sort order
     * so this leg's shards can seek past them independently of other legs.
     */
    public void setSearchAfter(Object[] searchAfter) {
        this.searchAfter = searchAfter;
    }

    public CollapseBuilder getCollapse() {
        return collapse;
    }

    public void setCollapse(CollapseBuilder collapse) {
        this.collapse = collapse;
    }

    public Float getMinScore() {
        return minScore;
    }

    public void setMinScore(Float minScore) {
        this.minScore = minScore;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getFrom() {
        return from;
    }

    /**
     * Sets an offset into this leg's own candidate ranking, before fusion. Shallow one-off use
     * only — for deep or repeated paging through this leg's candidates, prefer
     * {@link #setSearchAfter} with a stable {@link #setSorts} instead (same guidance as plain
     * {@code _search}: {@code from} cost grows with {@code from + size} per shard).
     * <p>
     * Combining a non-zero {@code from} with {@link #setSearchAfter} is rejected the same way
     * plain {@code _search} rejects it — this leg dispatches as a real {@code SearchRequest}, so
     * that validation applies automatically when the leg executes.
     */
    public void setFrom(int from) {
        this.from = from;
    }

    public Boolean getTrackScores() {
        return trackScores;
    }

    public void setTrackScores(Boolean trackScores) {
        this.trackScores = trackScores;
    }

    public List<FieldAndFormat> getDocvalueFields() {
        return docvalueFields;
    }

    public List<org.opensearch.search.rescore.RescorerBuilder> getRescorers() {
        return rescorers;
    }

    public void addRescorer(org.opensearch.search.rescore.RescorerBuilder rescorer) {
        if (this.rescorers == null) {
            this.rescorers = new ArrayList<>();
        }
        this.rescorers.add(rescorer);
    }

    /**
     * Add a docvalue field (called by {@link org.opensearch.search.retriever.modifiers.RequireDocvalueField}).
     * Does not add duplicates.
     */
    public void addDocvalueField(String field) {
        if (docvalueFields == null) {
            docvalueFields = new ArrayList<>();
        }
        // Avoid duplicates
        for (FieldAndFormat existing : docvalueFields) {
            if (existing.field.equals(field)) {
                return;
            }
        }
        docvalueFields.add(new FieldAndFormat(field, null));
    }

    /**
     * Set search result after dispatch (called by executor).
     */
    public void setSearchResult(List<RankedDoc> result) {
        this.searchResult = result;
    }

    /**
     * Set per-shard profile results from the leg sub-search (called by executor when profile=true).
     */
    public void setLegShardProfiles(java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> profiles) {
        this.legShardProfiles = profiles;
    }

    // --- RetrieverBuilder contract ---

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        return Collections.singletonList(this);
    }

    @Override
    protected void doResolve() {
        // Leaf nodes are already resolved — their results were set by the executor
        this.resolvedResult = searchResult;
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        if (filterBuilder != null) {
            return new BoolQueryBuilder().must(queryBuilder).filter(filterBuilder);
        }
        return queryBuilder;
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        return toQueryBuilder();
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return Collections.emptyList();
    }

    @Override
    public int getMaxOutputSize() {
        return size;
    }

    @Override
    public void validate(RetrieverContext context) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("[standard] requires [query]");
        }
        // The hybrid query has its own multi-sub-query fusion mechanism, which conflicts with the
        // retriever tree's fusion. Users should express fusion with a compound retriever instead.
        // Checked by writeable name so core doesn't depend on the neural-search plugin's class.
        if ("hybrid".equals(queryBuilder.getWriteableName())) {
            throw new IllegalArgumentException(
                "[hybrid] query is not allowed inside [standard] retriever; "
                    + "use a [rank_fusion] or [score_fusion] retriever instead"
            );
        }
        if (searchAfter != null && (sorts == null || sorts.isEmpty())) {
            throw new IllegalArgumentException(
                "[standard] requires [sort] on a stored field when [search_after] is set — "
                    + "this leg's shards seek independently by that field, so the sort must be "
                    + "deterministic (include a tiebreaker such as _id)"
            );
        }
        // Check ancestor constraints
        for (LeafConstraint constraint : context.getConstraints()) {
            constraint.validate(this);
        }
    }

    @Override
    public void prepareLeaves(RetrieverContext context) {
        // Apply ancestor modifiers
        for (LeafModifier modifier : context.getModifiers()) {
            modifier.apply(this);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Explanation buildExplanation(String docId, String docIndex) {
        // For leaf nodes, return the explanation captured from the leg sub-search response
        if (searchResult == null) return null;
        for (RankedDoc doc : searchResult) {
            if (doc.id().equals(docId) && doc.index().equals(docIndex)) {
                return doc.explanation();
            }
        }
        return null;
    }

    @Override
    public RetrieverProfile buildProfile() {
        RetrieverProfile.Builder builder = new RetrieverProfile.Builder(NAME)
            .totalTimeNanos(getElapsedNanos());
        if (legShardProfiles != null && !legShardProfiles.isEmpty()) {
            builder.shardProfiles(legShardProfiles);
        }
        return builder.build();
    }

    /**
     * Build a SearchRequest for dispatching this leaf as an independent sub-search.
     *
     * @param indices         the target indices
     * @param originalRequest the original search request (for PIT, preference, routing)
     * @return a fully configured SearchRequest ready for dispatch
     */
    public SearchRequest toSearchRequest(String[] indices, SearchRequest originalRequest) {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(toQueryBuilder())
            .from(from)
            .size(size)
            .trackScores(trackScores != null ? trackScores : true);

        if (sorts != null) {
            for (SortBuilder<?> sort : sorts) {
                source.sort(sort);
            }
        }
        if (searchAfter != null) {
            source.searchAfter(searchAfter);
        }
        if (collapse != null) {
            source.collapse(collapse);
        }
        if (minScore != null) {
            source.minScore(minScore);
        }
        if (docvalueFields != null) {
            for (FieldAndFormat field : docvalueFields) {
                source.docValueField(field.field, field.format);
            }
        }
        if (rescorers != null) {
            for (org.opensearch.search.rescore.RescorerBuilder rescorer : rescorers) {
                source.addRescorer(rescorer);
            }
        }

        SearchRequest legRequest = new SearchRequest(indices);
        legRequest.source(source);
        if (originalRequest != null) {
            legRequest.preference(originalRequest.preference());
            legRequest.routing(originalRequest.routing());
            if (originalRequest.source() != null) {
                if (originalRequest.source().pointInTimeBuilder() != null) {
                    source.pointInTimeBuilder(originalRequest.source().pointInTimeBuilder());
                }
                // Propagate the request timeout so a slow leg is bounded like a normal search.
                if (originalRequest.source().timeout() != null) {
                    source.timeout(originalRequest.source().timeout());
                }
                // Propagate indices_boost to sub-searches so per-index score multipliers
                // affect fusion quality (scores from boosted indices rank higher before fusion)
                if (originalRequest.source().indexBoosts() != null) {
                    for (SearchSourceBuilder.IndexBoost boost : originalRequest.source().indexBoosts()) {
                        source.indexBoost(boost.getIndex(), boost.getBoost());
                    }
                }
            }
        }
        return legRequest;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", queryBuilder);
        if (filterBuilder != null) {
            builder.field("filter", filterBuilder);
        }
        if (sorts != null && !sorts.isEmpty()) {
            builder.startArray("sort");
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (searchAfter != null) {
            builder.array("search_after", searchAfter);
        }
        if (collapse != null) {
            builder.field("collapse", collapse);
        }
        if (minScore != null) {
            builder.field("min_score", minScore);
        }
        if (from != 0) {
            builder.field("from", from);
        }
        if (size != 100) {
            builder.field("size", size);
        }
        if (trackScores != null) {
            builder.field("track_scores", trackScores);
        }
        if (docvalueFields != null && !docvalueFields.isEmpty()) {
            builder.startArray("docvalue_fields");
            for (FieldAndFormat field : docvalueFields) {
                field.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (rescorers != null && !rescorers.isEmpty()) {
            builder.startArray("rescore");
            for (org.opensearch.search.rescore.RescorerBuilder rescorer : rescorers) {
                rescorer.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Parse a StandardRetrieverBuilder from XContent.
     */
    public static StandardRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        StandardRetrieverBuilder builder = new StandardRetrieverBuilder();
        String fieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                switch (fieldName) {
                    case "query":
                        builder.queryBuilder = org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        break;
                    case "filter":
                        builder.filterBuilder = org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        break;
                    case "sort":
                        builder.sorts = new java.util.ArrayList<>(
                            org.opensearch.search.sort.SortBuilder.fromXContent(parser)
                        );
                        break;
                    case "search_after":
                        builder.searchAfter = org.opensearch.search.searchafter.SearchAfterBuilder.fromXContent(parser).getSortValues();
                        break;
                    case "collapse":
                        builder.collapse = org.opensearch.search.collapse.CollapseBuilder.fromXContent(parser);
                        break;
                    case "min_score":
                        builder.minScore = parser.floatValue();
                        break;
                    case "from":
                        builder.from = parser.intValue();
                        break;
                    case "size":
                        builder.size = parser.intValue();
                        break;
                    case "track_scores":
                        builder.trackScores = parser.booleanValue();
                        break;
                    case "rescore":
                        // Parse rescore — supports single object or array
                        if (token == XContentParser.Token.START_OBJECT) {
                            builder.addRescorer(org.opensearch.search.rescore.RescorerBuilder.parseFromXContent(parser));
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                builder.addRescorer(org.opensearch.search.rescore.RescorerBuilder.parseFromXContent(parser));
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("[standard] unknown field [" + fieldName + "]");
                }
            }
        }
        return builder;
    }
}
