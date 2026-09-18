/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Drives the retriever lifecycle for one request: (optionally) open a framework-managed PIT → validate →
 * cap checks → resolve the tree (per-node, listener-based) while dispatching an optional global leg
 * <b>independently</b> → release the PIT exactly once on every exit path. After {@link #execute}
 * completes, the resolved root can be projected to a {@code RankDocsQuery} via {@link #getResolvedQuery()}
 * and the global-leg response (if any) has been stashed into the {@link RetrieverResolutionContext} for
 * {@code merge()} into the final response.
 * <p>
 * <b>Dispatch model.</b> There is no {@code multiSearch} batch. Each leaf dispatches its own
 * {@code client.search} inside {@link RetrieverBuilder#resolve}; compound/transformer nodes fan out to
 * their children and resolve inline once children complete. The global leg (aggregations /
 * {@code track_total_hits}) is fired as its own search and joined only at the end — it never gates tree
 * resolution.
 * <p>
 * <b>PIT lifecycle (A3c).</b> Three states, in precedence order:
 * <ol>
 *   <li><b>user-supplied {@code pit}</b> — the original request source already carries a
 *       {@code pointInTimeBuilder}; the cascade runs under it and the framework <b>never</b> releases a
 *       PIT it did not open.</li>
 *   <li><b>opt-out</b> ({@code frameworkPit == false}) — no PIT; each round runs on live readers.</li>
 *   <li><b>framework-managed</b> — open a short-lived PIT, set it on the request source (the seam every
 *       leg / the global leg / the final {@code RankDocsQuery} search inherit), run the cascade, and
 *       release it via {@link ActionListener#runAfter} on <b>every</b> exit path (success / failure /
 *       cancellation). {@code CreatePit} failure at the context ceiling <b>fails closed</b> — the error
 *       is surfaced, never degraded to live readers.</li>
 * </ol>
 * The PIT id flows to every round because legs read it from
 * {@code originalRequest.source().pointInTimeBuilder()} (A3b); setting it there is the whole propagation.
 *
 * @opensearch.internal
 */
public class RetrieverExecutor {

    private final RetrieverBuilder root;
    private final String[] indices;
    private final SearchRequest originalRequest;
    private final AggregatorFactories.Builder aggregations;
    private final boolean trackTotalHits;
    private final Integer trackTotalHitsUpTo;
    private final RetrieverResolutionContext context;
    private final boolean frameworkPit;
    private final TimeValue pitKeepAlive;

    public RetrieverExecutor(
        RetrieverBuilder root,
        String[] indices,
        SearchRequest originalRequest,
        AggregatorFactories.Builder aggregations,
        boolean trackTotalHits,
        Integer trackTotalHitsUpTo,
        RetrieverResolutionContext context,
        boolean frameworkPit,
        TimeValue pitKeepAlive
    ) {
        this.root = root;
        this.indices = indices;
        this.originalRequest = originalRequest;
        this.aggregations = aggregations;
        this.trackTotalHits = trackTotalHits;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.context = context;
        this.frameworkPit = frameworkPit;
        this.pitKeepAlive = pitKeepAlive;
    }

    /**
     * Execute the retriever lifecycle. Opens a framework-managed PIT first (state 3) when {@code frameworkPit}
     * is true and the user did not supply their own {@code pit}; then runs the cascade + global leg, and
     * releases the PIT exactly once. {@code listener} fires once both the ranking and the global leg (if
     * any) complete; on any failure it fires once with the first error.
     */
    public void execute(Client client, ActionListener<Void> listener) {
        boolean userSuppliedPit = originalRequest != null
            && originalRequest.source() != null
            && originalRequest.source().pointInTimeBuilder() != null;

        if (frameworkPit && userSuppliedPit == false) {
            // State 3: open a framework-managed PIT. The executor OPENS it and sets it on the request source
            // (the seam every leg / the global leg / the final RankDocsQuery search inherit). Release is
            // split across two mutually-exclusive outcomes so the PIT is freed exactly once:
            //   (a) resolution FAILS here  -> the final search will never run, so THIS method releases the
            //       PIT and clears the id from the context so the transport wrap does not double-release;
            //   (b) resolution SUCCEEDS    -> the PIT must outlive resolution to cover the final search, so
            //       it is released by TransportSearchAction's wrapped response listener (which reads the id
            //       off the context) after the final response, on both its success and failure paths.
            CreatePitRequest createPitRequest = new CreatePitRequest(pitKeepAlive, false, indices);
            client.execute(CreatePitAction.INSTANCE, createPitRequest, ActionListener.wrap(createPitResponse -> {
                final String pitId = createPitResponse.getId();
                context.setFrameworkManagedPitId(pitId);
                if (originalRequest != null && originalRequest.source() != null) {
                    originalRequest.source().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(pitKeepAlive));
                }
                // On resolution failure, release the just-opened PIT (outcome (a)) before propagating: no
                // final search will run to trigger the transport-wrap release, so failing to release here
                // would leak the PIT until keep-alive expiry.
                ActionListener<Void> cascadeListener = ActionListener.wrap(listener::onResponse, cascadeFailure -> {
                    context.setFrameworkManagedPitId(null); // prevent the transport wrap from double-releasing
                    client.execute(
                        DeletePitAction.INSTANCE,
                        new DeletePitRequest(pitId),
                        ActionListener.wrap(r -> listener.onFailure(cascadeFailure), releaseError -> {
                            releaseError.addSuppressed(cascadeFailure);
                            listener.onFailure(cascadeFailure);
                        })
                    );
                });
                runCascade(client, cascadeListener);
            }, listener::onFailure)); // CreatePit failure at the ceiling → fail closed (surface the error).
            return;
        }

        // State 1 (user pit — run under it, never release) or State 2 (opt-out — no pit).
        runCascade(client, listener);
    }

    /** The tree-resolution + global-leg fan-out, joined at a single 2-way (or 1-way) latch. */
    private void runCascade(Client client, ActionListener<Void> listener) {
        try {
            // Phase 1: validate the whole tree top-down (fail fast, before any dispatch).
            root.validate();

            // Cluster-safety: bound tree depth and leaf fan-out.
            int maxDepth = SearchSourceBuilderRetrieverIntegration.getMaxDepth();
            int depth = treeDepth(root);
            if (depth > maxDepth) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "retriever tree depth ("
                            + depth
                            + ") exceeds the maximum allowed ("
                            + maxDepth
                            + "); reduce nesting or raise ["
                            + SearchSourceBuilderRetrieverIntegration.MAX_DEPTH_SETTING.getKey()
                            + "]"
                    )
                );
                return;
            }
            int leafCount = root.collectLeaves().size();
            if (leafCount == 0) {
                listener.onFailure(new IllegalArgumentException("retriever tree has no leaves"));
                return;
            }
            int maxLeafCount = SearchSourceBuilderRetrieverIntegration.getMaxLeafCount();
            if (leafCount > maxLeafCount) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "retriever request has "
                            + leafCount
                            + " leaf retrievers, exceeding the maximum allowed ("
                            + maxLeafCount
                            + "); reduce the number of leaves or raise ["
                            + SearchSourceBuilderRetrieverIntegration.MAX_LEAF_COUNT_SETTING.getKey()
                            + "]"
                    )
                );
                return;
            }
            // Phase 2: prepare leaves top-down.
            root.prepareLeaves();

            // Two independent async flows joined at the end: (1) tree resolution (the ranking) and, when
            // present, (2) the global leg (aggs / track_total_hits). The global leg does NOT gate the tree.
            boolean hasGlobalLeg = aggregations != null || trackTotalHits;
            final CountDown remaining = new CountDown(hasGlobalLeg ? 2 : 1);
            final AtomicReference<Exception> firstFailure = new AtomicReference<>();

            ActionListener<Void> legDone = ActionListener.wrap(v -> {
                if (remaining.countDown()) {
                    Exception failure = firstFailure.get();
                    if (failure != null) {
                        listener.onFailure(failure);
                    } else {
                        listener.onResponse(null);
                    }
                }
            }, e -> {
                firstFailure.compareAndSet(null, e);
                if (remaining.countDown()) {
                    listener.onFailure(firstFailure.get());
                }
            });

            if (hasGlobalLeg) {
                client.search(buildGlobalLeg(root.extractAggregationQuery()), ActionListener.wrap(response -> {
                    context.setGlobalLegResponse(response);
                    legDone.onResponse(null);
                }, legDone::onFailure));
            }

            // Resolve the tree (each leaf dispatches its own search; compounds resolve inline).
            root.resolve(client, indices, originalRequest, legDone);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /** The global leg: the union of leaf queries, size 0, carrying the user's aggs / track_total_hits. */
    private SearchRequest buildGlobalLeg(QueryBuilder aggQuery) {
        SearchSourceBuilder source = new SearchSourceBuilder().query(aggQuery).size(0);
        if (trackTotalHitsUpTo != null) {
            source.trackTotalHitsUpTo(trackTotalHitsUpTo);
        } else {
            source.trackTotalHits(trackTotalHits);
        }
        if (aggregations != null) {
            for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
                source.aggregation(agg);
            }
            for (PipelineAggregationBuilder pipelineAgg : aggregations.getPipelineAggregatorFactories()) {
                source.aggregation(pipelineAgg);
            }
        }
        SearchRequest req = new SearchRequest(indices);
        req.source(source);
        if (originalRequest != null) {
            req.searchType(originalRequest.searchType());
            req.preference(originalRequest.preference());
            req.routing(originalRequest.routing());
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
        return req;
    }

    /** Depth of the retriever tree (root = 1). */
    private static int treeDepth(RetrieverBuilder node) {
        int maxChild = 0;
        for (RetrieverBuilder child : node.getChildRetrievers()) {
            maxChild = Math.max(maxChild, treeDepth(child));
        }
        return 1 + maxChild;
    }

    /** The resolved final query built from the fully-resolved tree root. */
    public QueryBuilder getResolvedQuery() {
        return root.toQueryBuilder();
    }

    /** The global-leg response (aggs / track_total_hits), or null if no global leg was dispatched. */
    public SearchResponse getGlobalLegResponse() {
        return context.getGlobalLegResponse();
    }
}
