/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.searchafter.SearchAfterBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The leaf retriever that wraps a standard OpenSearch query with optional result-set operations.
 * This is the bridge between the retriever tree and the existing query DSL. Every retriever tree
 * terminates at {@code standard} leaves, which are dispatched as independent sub-searches by the
 * executor.
 *
 * @opensearch.internal
 */
@PublicApi(since = "3.7.0")
public class StandardRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "standard";

    /**
     * Default fetch depth for a leg that is neither fusion-governed nor explicitly sized — matches the
     * default {@code size} of a normal {@code _search} ({@link SearchService#DEFAULT_SIZE}).
     */
    public static final int DEFAULT_SIZE = SearchService.DEFAULT_SIZE;

    private QueryBuilder queryBuilder;
    private QueryBuilder filterBuilder;
    private List<SortBuilder<?>> sorts;
    private Object[] searchAfter;
    private CollapseBuilder collapse;
    private Float minScore;
    private int from = 0;
    // null = user did not set a size. A fusion-governed leg inherits its depth from the enclosing
    // rank_window_size (see #effectiveWindow); an explicit size is only allowed when NOT fusion-governed.
    private Integer size;
    private Boolean trackScores;
    private List<FieldAndFormat> docvalueFields;
    private List<RescorerBuilder> rescorers;

    // Fetch depth handed down by a fusion ancestor during prepareLeaves; NO_WINDOW = not fusion-governed.
    private int effectiveWindow = LeafPreparationContext.NO_WINDOW;

    /** Set by the executor after this leaf's sub-search returns — its dispatched ranked candidates. */
    private List<RetrieverCandidate> searchResult;

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
     * Sets the search_after cursor for this leg. Requires an explicit {@link #setSorts} (a stored-field
     * sort with a tiebreaker) — the cursor values must correspond to that sort order so this leg's shards
     * can seek past them independently of other legs.
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

    /**
     * The effective fetch depth for this leg: the explicit {@code size} if the user set one, else the
     * fusion window inherited from an enclosing fusion (set in {@link #prepareLeaves(LeafPreparationContext)}), else
     * {@link #DEFAULT_SIZE}. Resolving here keeps a single source of truth for the depth used by
     * {@link #toSearchRequest}.
     */
    public int getSize() {
        return resolveFetchDepth();
    }

    /** The user-supplied {@code size}, or {@code null} if unset. Used to detect an explicit override. */
    public Integer getExplicitSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    private int resolveFetchDepth() {
        if (size != null) {
            return size;
        }
        if (effectiveWindow != LeafPreparationContext.NO_WINDOW) {
            return effectiveWindow;
        }
        return DEFAULT_SIZE;
    }

    public int getFrom() {
        return from;
    }

    /**
     * Sets an offset into this leg's own candidate ranking, before fusion. Shallow one-off use only —
     * for deep or repeated paging through this leg's candidates, prefer {@link #setSearchAfter} with a
     * stable {@link #setSorts} instead (same guidance as plain {@code _search}).
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

    public List<RescorerBuilder> getRescorers() {
        return rescorers;
    }

    public void addRescorer(RescorerBuilder rescorer) {
        if (this.rescorers == null) {
            this.rescorers = new ArrayList<>();
        }
        this.rescorers.add(rescorer);
    }

    // --- RetrieverBuilder contract ---

    /** Set this leaf's dispatched ranked candidates (called by the executor after its sub-search returns). */
    void setSearchResult(List<RetrieverCandidate> searchResult) {
        this.searchResult = searchResult;
    }

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        return Collections.singletonList(this);
    }

    @Override
    void doResolve() {
        // A leaf is already resolved — its ranked output is exactly the result its own dispatch produced.
        this.resolvedResult = searchResult;
    }

    @Override
    void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
        // A leaf is the only node that does I/O: dispatch its own sub-search, then resolve on the response.
        client.search(toSearchRequest(indices, original), ActionListener.wrap(response -> {
            this.searchResult = extractCandidates(response);
            doResolve();
            whenDone.onResponse(null);
        }, whenDone::onFailure));
    }

    /** Turn this leg's query-phase hits into ranked {@link RetrieverCandidate}s; position = hit order. */
    private static List<RetrieverCandidate> extractCandidates(SearchResponse response) {
        SearchHit[] hits = response.getHits().getHits();
        List<RetrieverCandidate> candidates = new ArrayList<>(hits.length);
        int position = 0;
        for (SearchHit hit : hits) {
            ShardId shardId = hit.getShard() != null ? hit.getShard().getShardId() : null;
            if (shardId == null) {
                // A hit must carry its shard for the (index, shardId) scoping the RankDocsQuery relies on.
                throw new IllegalStateException("retriever leg hit [" + hit.getId() + "] has no shard target");
            }
            candidates.add(new RetrieverCandidate(hit.getIndex(), shardId, hit.getId(), hit.getScore(), position++));
        }
        return candidates;
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        // Project the resolved candidate window to the wire RankDoc list and build the internal
        // RankDocsQuery that replays this ranking on the final fetch.
        List<RankDoc> window = new ArrayList<>(resolvedResult == null ? 0 : resolvedResult.size());
        if (resolvedResult != null) {
            for (RetrieverCandidate candidate : resolvedResult) {
                window.add(candidate.toRankDoc());
            }
        }
        return new RankDocsQueryBuilder(window);
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        return toLegQuery();
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return Collections.emptyList();
    }

    @Override
    public void validate() {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("[standard] requires [query]");
        }
        // The hybrid query has its own multi-sub-query fusion mechanism, which conflicts with the
        // retriever tree's fusion. Users should express fusion with a compound retriever instead.
        // Checked by writeable name so core does not depend on the neural-search plugin's class.
        if ("hybrid".equals(queryBuilder.getWriteableName())) {
            throw new IllegalArgumentException(
                "[hybrid] query is not allowed inside [standard] retriever; use a [rank_fusion] or [score_fusion] retriever instead"
            );
        }
        if (searchAfter != null && (sorts == null || sorts.isEmpty())) {
            throw new IllegalArgumentException(
                "[standard] requires [sort] on a stored field when [search_after] is set — this leg's shards seek "
                    + "independently by that field, so the sort must be deterministic (include a tiebreaker such as _id)"
            );
        }
    }

    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        if (context.isFusionGoverned()) {
            if (size != null) {
                throw new IllegalArgumentException(
                    "[standard] does not support [size] inside a [rank_fusion] retriever; each leg must fetch exactly "
                        + "[rank_window_size] candidates so the fused window is complete and stable across requests. "
                        + "Control leg depth with the enclosing [rank_fusion] [rank_window_size], and the number of hits "
                        + "returned with the top-level [size]."
                );
            }
            this.effectiveWindow = context.getInheritedWindow();
            // Some query types have their own internal candidate cap that leg [size] does not touch (e.g. the
            // knn query's [k]). If this leg's query is window-aware, hand it the window so it can align that cap;
            // otherwise leg [size] alone governs the candidate count.
            if (queryBuilder instanceof RetrieverWindowAware) {
                ((RetrieverWindowAware) queryBuilder).applyRetrieverWindow(this.effectiveWindow);
            }
        } else {
            // Not fusion-governed: keep any explicit size, otherwise fall back to DEFAULT_SIZE at dispatch.
            this.effectiveWindow = LeafPreparationContext.NO_WINDOW;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * The query this leaf contributes to its own leg sub-search: the plain query, or a {@code bool} of
     * query + filter. This is NOT the tree's final query — see {@link #toQueryBuilder()}.
     */
    QueryBuilder toLegQuery() {
        if (filterBuilder != null) {
            return new BoolQueryBuilder().must(queryBuilder).filter(filterBuilder);
        }
        return queryBuilder;
    }

    /**
     * Build a SearchRequest for dispatching this leaf as an independent sub-search, used by the executor.
     *
     * @param indices         the target indices
     * @param originalRequest the original search request (for PIT, preference, routing, indices_boost)
     * @return a fully configured SearchRequest ready for dispatch
     */
    public SearchRequest toSearchRequest(String[] indices, SearchRequest originalRequest) {
        SearchSourceBuilder source = new SearchSourceBuilder().query(toLegQuery())
            .from(from)
            .size(resolveFetchDepth())
            .trackScores(trackScores != null ? trackScores : true);

        // A leg only needs each candidate's (index, shardId, _id, score) to compute ranks — see
        // extractCandidates. The real payload is loaded once by the final RankDocsQuery fetch, so skip the
        // leg's fetch-phase _source loading (avoids rank_window_size x num_legs wasted _source loads).
        // Only _source is disabled — stored fields are kept because the leg still needs _id (and _id /
        // stored-field retrieval is cheap relative to _source).
        source.fetchSource(false);

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
            for (RescorerBuilder rescorer : rescorers) {
                source.addRescorer(rescorer);
            }
        }

        SearchRequest legRequest = new SearchRequest(indices);
        legRequest.source(source);
        if (originalRequest != null) {
            // Inherit the original request's search type so leg scoring matches a normal search. Without
            // this, a DFS_QUERY_THEN_FETCH request would score its legs with per-shard QUERY_THEN_FETCH
            // statistics (different IDF), diverging from the equivalent plain search.
            legRequest.searchType(originalRequest.searchType());
            legRequest.preference(originalRequest.preference());
            legRequest.routing(originalRequest.routing());
            if (originalRequest.source() != null) {
                if (originalRequest.source().pointInTimeBuilder() != null) {
                    source.pointInTimeBuilder(originalRequest.source().pointInTimeBuilder());
                }
                if (originalRequest.source().timeout() != null) {
                    source.timeout(originalRequest.source().timeout());
                }
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
        if (size != null) {
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
            for (RescorerBuilder rescorer : rescorers) {
                rescorer.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Parse a StandardRetrieverBuilder from XContent. The parser is positioned inside the {@code standard}
     * object (past its START_OBJECT).
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
                        builder.queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        break;
                    case "filter":
                        builder.filterBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        break;
                    case "sort":
                        builder.sorts = new ArrayList<>(SortBuilder.fromXContent(parser));
                        break;
                    case "search_after":
                        builder.searchAfter = SearchAfterBuilder.fromXContent(parser).getSortValues();
                        break;
                    case "collapse":
                        builder.collapse = CollapseBuilder.fromXContent(parser);
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
                        if (token == XContentParser.Token.START_OBJECT) {
                            builder.addRescorer(RescorerBuilder.parseFromXContent(parser));
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                builder.addRescorer(RescorerBuilder.parseFromXContent(parser));
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
