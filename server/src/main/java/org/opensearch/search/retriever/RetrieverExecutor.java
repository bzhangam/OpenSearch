/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives the retriever lifecycle: validate → prepareLeaves → collectLeaves → dispatch → resolveBottomUp.
 * <p>
 * {@link SearchSourceBuilder} delegates retriever orchestration to this class, keeping the builder
 * thin and the orchestration testable in isolation.
 *
 * @opensearch.internal
 */
public class RetrieverExecutor {

    private final RetrieverBuilder root;
    private final String[] indices;
    private final SearchRequest originalRequest;
    private final AggregatorFactories.Builder aggregations;
    private final SuggestBuilder suggest;
    private final boolean trackTotalHits;
    private final Integer trackTotalHitsUpTo;
    private final boolean explain;
    private final boolean profile;

    private List<RankedDoc> fusedResults;
    private SearchResponse globalLegResponse;
    private java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> globalLegShardProfiles;

    public RetrieverExecutor(
        RetrieverBuilder root,
        String[] indices,
        SearchRequest originalRequest,
        AggregatorFactories.Builder aggregations,
        SuggestBuilder suggest,
        boolean trackTotalHits
    ) {
        this(root, indices, originalRequest, aggregations, suggest, trackTotalHits, null, false, false);
    }

    public RetrieverExecutor(
        RetrieverBuilder root,
        String[] indices,
        SearchRequest originalRequest,
        AggregatorFactories.Builder aggregations,
        SuggestBuilder suggest,
        boolean trackTotalHits,
        boolean explain,
        boolean profile
    ) {
        this(root, indices, originalRequest, aggregations, suggest, trackTotalHits, null, explain, profile);
    }

    public RetrieverExecutor(
        RetrieverBuilder root,
        String[] indices,
        SearchRequest originalRequest,
        AggregatorFactories.Builder aggregations,
        SuggestBuilder suggest,
        boolean trackTotalHits,
        Integer trackTotalHitsUpTo,
        boolean explain,
        boolean profile
    ) {
        this.root = root;
        this.indices = indices;
        this.originalRequest = originalRequest;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.trackTotalHits = trackTotalHits;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.explain = explain;
        this.profile = profile;
    }

    /**
     * Execute the full retriever lifecycle asynchronously.
     * On success, fusedResults and globalLegResponse are populated.
     */
    public void execute(Client client, ActionListener<Void> listener) {
        try {
            // Phase 1: Validate entire tree top-down (fail fast)
            RetrieverContext rootContext = RetrieverContext.root();
            root.validate(rootContext);

            // Cluster-safety: bound tree depth (limits serial async rounds and complexity).
            int maxDepth = SearchSourceBuilderRetrieverIntegration.getMaxDepth();
            int depth = treeDepth(root);
            if (depth > maxDepth) {
                listener.onFailure(new IllegalArgumentException(
                    "retriever tree depth (" + depth + ") exceeds the maximum allowed ("
                        + maxDepth + "); reduce nesting or raise [" + SearchSourceBuilderRetrieverIntegration.MAX_DEPTH_SETTING.getKey() + "]"
                ));
                return;
            }

            // Phase 2: Prepare leaves top-down (propagate modifications)
            root.prepareLeaves(rootContext);

            // Phase 3: Collect leaves and build MultiSearch
            List<StandardRetrieverBuilder> leaves = root.collectLeaves();
            if (leaves.isEmpty()) {
                listener.onFailure(new IllegalArgumentException("retriever tree has no leaves"));
                return;
            }

            // Cluster-safety: bound fan-out (each leaf is an independent sub-search).
            int maxLeafCount = SearchSourceBuilderRetrieverIntegration.getMaxLeafCount();
            if (leaves.size() > maxLeafCount) {
                listener.onFailure(new IllegalArgumentException(
                    "retriever request has " + leaves.size() + " leaf retrievers, exceeding the maximum allowed ("
                        + maxLeafCount + "); reduce the number of leaves or raise ["
                        + SearchSourceBuilderRetrieverIntegration.MAX_LEAF_COUNT_SETTING.getKey() + "]"
                ));
                return;
            }

            MultiSearchRequest msr = new MultiSearchRequest();
            for (StandardRetrieverBuilder leaf : leaves) {
                SearchRequest legReq = leaf.toSearchRequest(indices, originalRequest);
                // Propagate explain and profile flags to each leg sub-search
                if (explain && legReq.source() != null) {
                    legReq.source().explain(true);
                }
                if (profile && legReq.source() != null) {
                    legReq.source().profile(true);
                }
                msr.add(legReq);
            }

            boolean hasGlobalLeg = aggregations != null || suggest != null || trackTotalHits;
            if (hasGlobalLeg) {
                msr.add(buildGlobalLeg(root.extractAggregationQuery()));
            }

            // Phase 4: Fire all leaf sub-searches in parallel
            final long dispatchStart = System.nanoTime();
            // Start timing on all leaves
            for (StandardRetrieverBuilder leaf : leaves) {
                leaf.startTiming();
            }
            root.startTiming();

            client.multiSearch(msr, ActionListener.wrap(response -> {
                try {
                    final long dispatchEnd = System.nanoTime();
                    final long dispatchNanos = dispatchEnd - dispatchStart;
                    MultiSearchResponse.Item[] items = response.getResponses();

                    // Check for failures
                    for (int i = 0; i < leaves.size(); i++) {
                        if (items[i].isFailure()) {
                            listener.onFailure(items[i].getFailure());
                            return;
                        }
                    }

                    // Distribute results to leaves (with explanations when explain=true)
                    for (int i = 0; i < leaves.size(); i++) {
                        SearchResponse legResponse = items[i].getResponse();
                        leaves.get(i).setSearchResult(extractRankedDocs(legResponse));
                        leaves.get(i).stopTiming();
                        // Capture per-leg shard profiles when profile=true
                        if (profile && legResponse.getProfileResults() != null && !legResponse.getProfileResults().isEmpty()) {
                            leaves.get(i).setLegShardProfiles(legResponse.getProfileResults());
                        }
                    }

                    // Set dispatch timing on compound/transformer nodes
                    setDispatchTiming(root, dispatchNanos);

                    // Extract global leg response
                    if (hasGlobalLeg) {
                        int globalLegIdx = leaves.size();
                        if (items[globalLegIdx].isFailure()) {
                            listener.onFailure(items[globalLegIdx].getFailure());
                            return;
                        }
                        this.globalLegResponse = items[globalLegIdx].getResponse();
                        // Store on root so SSB can access it during rewrite pass 2
                        root.setGlobalLegResponse(this.globalLegResponse);
                        // Capture global leg shard profiles for the profile response
                        if (profile && this.globalLegResponse.getProfileResults() != null) {
                            this.globalLegShardProfiles = this.globalLegResponse.getProfileResults();
                            root.setGlobalLegShardProfiles(this.globalLegShardProfiles);
                        }
                    }

                    // Phase 5: Iterative async resolution loop
                    iterativeResolve(client, listener, 0);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Iterative resolution loop:
     * 1. resolveBottomUp() — resolves all nodes that can be resolved synchronously
     * 2. Collect nodes that need async dispatch (needsAsyncResolution() == true)
     * 3. If none, resolution is complete
     * 4. If any, dispatch their async requests in parallel, then re-enter this loop
     *
     * Each iteration resolves one "level" of async nodes. For deeply nested trees
     * (e.g., rescore → fusion → rescore → fusion), each iteration resolves the deepest
     * unresolved level, allowing the next level up to resolve on the subsequent pass.
     *
     * Max iterations is bounded by tree depth (typically 2-4 for realistic trees).
     */
    private static final int MAX_ASYNC_RESOLUTION_ROUNDS = 16;

    private void iterativeResolve(Client client, ActionListener<Void> listener, int iteration) {
        if (iteration >= MAX_ASYNC_RESOLUTION_ROUNDS) {
            listener.onFailure(new IllegalStateException(
                "retriever tree resolution did not converge after " + MAX_ASYNC_RESOLUTION_ROUNDS + " rounds"
            ));
            return;
        }

        try {
            // Resolve what can be resolved synchronously
            root.resolveBottomUp();
            this.fusedResults = root.getResolvedResult();

            // Collect nodes that still need async dispatch
            List<RetrieverBuilder> asyncNodes = collectNodesNeedingAsync(root);

            if (asyncNodes.isEmpty()) {
                // All resolved — done
                root.stopTiming();
                listener.onResponse(null);
                return;
            }

            // Dispatch all async nodes in parallel
            MultiSearchRequest asyncMsr = new MultiSearchRequest();
            for (RetrieverBuilder node : asyncNodes) {
                SearchRequest asyncReq = node.buildAsyncSearchRequest(indices, originalRequest);
                // Propagate explain/profile so the extra round returns real explanations/shard profiles.
                if (asyncReq.source() != null) {
                    if (explain) {
                        asyncReq.source().explain(true);
                    }
                    if (profile) {
                        asyncReq.source().profile(true);
                    }
                }
                asyncMsr.add(asyncReq);
            }

            client.multiSearch(asyncMsr, ActionListener.wrap(asyncResponse -> {
                try {
                    MultiSearchResponse.Item[] asyncItems = asyncResponse.getResponses();
                    for (int i = 0; i < asyncNodes.size(); i++) {
                        if (asyncItems[i].isFailure()) {
                            listener.onFailure(asyncItems[i].getFailure());
                            return;
                        }
                        asyncNodes.get(i).setAsyncSearchResult(extractRankedDocs(asyncItems[i].getResponse()));
                    }

                    // Re-enter the loop — next iteration will resolve the level above
                    iterativeResolve(client, listener, iteration + 1);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Walk the tree depth-first and collect all nodes that need async resolution.
     * Returns them in bottom-up order (deepest first) so that parallel dispatch
     * handles independent nodes at the same level.
     */
    private List<RetrieverBuilder> collectNodesNeedingAsync(RetrieverBuilder node) {
        List<RetrieverBuilder> result = new ArrayList<>();
        // Depth-first: children before parent
        for (RetrieverBuilder child : node.getChildRetrievers()) {
            result.addAll(collectNodesNeedingAsync(child));
        }
        if (node.needsAsyncResolution()) {
            result.add(node);
        }
        return result;
    }

    /**
     * Compute the depth of the retriever tree (root = 1). A leaf has depth 1; a compound/transformer
     * is 1 + the deepest child.
     */
    private static int treeDepth(RetrieverBuilder node) {
        int maxChild = 0;
        for (RetrieverBuilder child : node.getChildRetrievers()) {
            maxChild = Math.max(maxChild, treeDepth(child));
        }
        return 1 + maxChild;
    }

    /**
     * Extract ranked docs from a search response, including explanations when explain=true.
     */
    private List<RankedDoc> extractRankedDocs(SearchResponse response) {
        SearchHits hits = response.getHits();
        List<RankedDoc> docs = new ArrayList<>(hits.getHits().length);
        int position = 0;
        for (SearchHit hit : hits.getHits()) {
            ShardId shardId;
            if (hit.getShard() != null) {
                shardId = hit.getShard().getShardId();
            } else {
                shardId = new ShardId(hit.getIndex(), "_na_", 0);
            }
            // Capture explanation when explain is enabled
            Explanation hitExplanation = explain ? hit.getExplanation() : null;
            docs.add(new RankedDoc(hit.getId(), hit.getIndex(), shardId, hit.getScore(), position++, hitExplanation));
        }
        return docs;
    }

    /**
     * Build the global leg for aggs/suggest/track_total_hits.
     * Uses the union of all leaf queries (bool.should) with size=0.
     */
    private SearchRequest buildGlobalLeg(QueryBuilder aggQuery) {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(aggQuery)
            .size(0);
        // Apply the user's track_total_hits setting (true/N threshold)
        if (trackTotalHitsUpTo != null) {
            source.trackTotalHitsUpTo(trackTotalHitsUpTo);
        } else {
            source.trackTotalHits(trackTotalHits);
        }
        if (aggregations != null) {
            for (org.opensearch.search.aggregations.AggregationBuilder agg : aggregations.getAggregatorFactories()) {
                source.aggregation(agg);
            }
            for (org.opensearch.search.aggregations.PipelineAggregationBuilder pipelineAgg : aggregations.getPipelineAggregatorFactories()) {
                source.aggregation(pipelineAgg);
            }
        }
        if (suggest != null) {
            source.suggest(suggest);
        }
        SearchRequest req = new SearchRequest(indices);
        req.source(source);
        if (originalRequest != null) {
            req.preference(originalRequest.preference());
            req.routing(originalRequest.routing());
            if (originalRequest.source() != null) {
                // Same snapshot/consistency and bounding as the scoring legs.
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
        return req;
    }

    /**
     * Get the fused results after execution.
     */
    public List<RankedDoc> getFusedResults() {
        return fusedResults;
    }

    /**
     * Get the global leg response (for aggs/suggest/total hits).
     */
    public SearchResponse getGlobalLegResponse() {
        return globalLegResponse;
    }

    /**
     * Get the global leg's shard profiles (for the profile response).
     */
    public java.util.Map<String, org.opensearch.search.profile.ProfileShardResult> getGlobalLegShardProfiles() {
        return globalLegShardProfiles;
    }

    /**
     * Whether explain was requested on this execution.
     */
    public boolean isExplain() {
        return explain;
    }

    /**
     * Whether profile was requested on this execution.
     */
    public boolean isProfile() {
        return profile;
    }

    /**
     * Set dispatch timing on compound/transformer nodes recursively.
     */
    private void setDispatchTiming(RetrieverBuilder node, long dispatchNanos) {
        if (node instanceof CompoundRetrieverBuilder) {
            ((CompoundRetrieverBuilder) node).setDispatchTimeNanos(dispatchNanos);
        }
        // Children already have their own timing from leaf startTiming/stopTiming
    }
}
