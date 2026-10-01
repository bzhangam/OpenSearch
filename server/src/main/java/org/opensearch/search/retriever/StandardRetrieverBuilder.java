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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.document.DocumentField;
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
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.searchafter.SearchAfterBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // Request-level explain flag captured during prepareLeaves; when true this leg's sub-search runs with
    // Lucene explain enabled so per-hit explanations flow into the coordinator-assembled explanation tree.
    private boolean explain = false;

    // Request-level profile flag captured during prepareLeaves; when true this leg's sub-search runs with
    // query profiling enabled so its per-shard profile results flow into the retriever profile tree.
    private boolean profile = false;

    // This leg's per-shard query profiles, captured from its sub-search response when profiling is on.
    private Map<String, ProfileShardResult> legShardProfiles;

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

    /**
     * Append a doc-value field to this leaf's sub-search. Used by a transformer that needs a field value to
     * ride along on the leg's own fetch rather than issuing a second search — e.g. the {@code diversify}
     * retriever injecting its {@code vector_field} ({@code format=array}) so MMR can read vectors without a
     * separate round trip. Idempotent on an exact duplicate so repeated preparation does not double-add.
     */
    public void addDocvalueField(FieldAndFormat field) {
        if (this.docvalueFields == null) {
            this.docvalueFields = new ArrayList<>();
        }
        if (this.docvalueFields.contains(field) == false) {
            this.docvalueFields.add(field);
        }
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
        final long startNanos = profile ? System.nanoTime() : 0L;
        client.search(toSearchRequest(indices, original), ActionListener.wrap(response -> {
            if (profile) {
                this.nodeElapsedNanos = System.nanoTime() - startNanos;
                this.legShardProfiles = response.getProfileResults();
            }
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
            // Capture any doc-value fields the hit carries (e.g. a vector_field a transformer injected onto
            // this leg) so a reranker can read them without a second fetch. Empty on the common path.
            Map<String, Object> fields = Collections.emptyMap();
            Map<String, DocumentField> documentFields = hit.getFields();
            if (documentFields != null && documentFields.isEmpty() == false) {
                fields = new HashMap<>(documentFields.size());
                for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                    fields.put(entry.getKey(), entry.getValue().getValues());
                }
            }
            candidates.add(
                new RetrieverCandidate(hit.getIndex(), shardId, hit.getId(), hit.getScore(), position++, hit.getExplanation(), fields)
            );
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
    public Explanation buildExplanation(String index, String id) {
        // A leaf's contribution is exactly the Lucene explanation its sub-search produced for this document.
        // Find the candidate in this leg's resolved output by (index, _id); return its captured explanation.
        if (resolvedResult != null) {
            for (RetrieverCandidate candidate : resolvedResult) {
                if (candidate.index().equals(index) && candidate.id().equals(id)) {
                    Explanation legExplanation = candidate.explanation();
                    if (legExplanation != null) {
                        return legExplanation;
                    }
                    // explain was not enabled on the leg (or Lucene returned none); fall back to the score
                    // so the tree still renders a value rather than a null hole.
                    return Explanation.match(candidate.score(), "standard retriever leg score (no Lucene explanation available)");
                }
            }
        }
        // This leg did not contribute the document to its window.
        return null;
    }

    @Override
    public RetrieverProfile.Node buildProfile() {
        // A leaf reports its sub-search dispatch time and the per-shard query profiles it captured. An empty
        // map is used when the leg returned no profile results (e.g. no matching shards).
        Map<String, ProfileShardResult> shards = legShardProfiles != null ? legShardProfiles : Map.of();
        return RetrieverProfile.leaf(getName(), nodeElapsedNanos, shards);
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
        this.explain = context.isExplain();
        this.profile = context.isProfile();
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
            // The retriever governs only the leg's fetch depth ([size] = rank_window_size). A query's own
            // internal candidate cap (e.g. the knn query's [k], or min_score/max_distance thresholds) is the
            // user's responsibility — set it appropriately (typically k >= rank_window_size) so the leg can
            // supply the window. The retriever does not read or modify it.
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

        // When the request asked for explain, run this leg's sub-search with explain enabled so Lucene
        // returns a per-hit Explanation. The coordinator captures it on each candidate (see
        // extractCandidates) and assembles the user-facing explanation tree after fusion/reshaping.
        if (explain) {
            source.explain(true);
        }

        // When the request asked for profiling, run this leg's sub-search with profiling enabled so its
        // per-shard query profiles can be captured on the response and nested under this leaf in the
        // retriever profile tree.
        if (profile) {
            source.profile(true);
        }

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
