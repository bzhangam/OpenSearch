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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for <b>compound</b> retrievers — nodes that fuse the rankings of two or more child
 * retrievers into a single ranking (e.g. {@link RankFusionRetrieverBuilder}). It carries the shared
 * tree-node machinery so a concrete compound only implements its fusion math ({@link #fuse}) and its
 * parse/render/name.
 * <p>
 * <b>Resolution.</b> A compound resolves by fanning out to all children (via the shared
 * {@link #resolveChildren}) and, once every child is resolved, computing its own {@link #resolvedResult}
 * inline in {@link #doResolve()} by calling {@link #fuse(List)} with the children's resolved candidate
 * lists in child order. It is pure in-memory work on a resolution-callback thread — no I/O, no blocking.
 * <p>
 * <b>Final query.</b> {@link #toQueryBuilder()} projects the fused candidate window to {@link RankDoc}s
 * and wraps them in a {@link RankDocsQueryBuilder}, exactly like a leaf — the difference is only how the
 * window was produced (fusion vs a single leg).
 * <p>
 * <b>Aggregation query.</b> {@link #extractAggregationQuery()} returns a {@code bool.should} union of the
 * children's aggregation queries, so the global leg (aggs / {@code track_total_hits}) counts over
 * everything any child leg matched.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public abstract class CompoundRetrieverBuilder extends RetrieverBuilder {

    /** Minimum children a compound must have — fusing fewer than two rankings is meaningless. */
    public static final int MIN_CHILDREN = 2;

    protected final List<RetrieverBuilder> children;

    // Wall time (ns) of the fuse() own-compute, recorded in doResolve; surfaced in the profile breakdown.
    private long fuseElapsedNanos;

    protected CompoundRetrieverBuilder(List<RetrieverBuilder> children) {
        this.children = children == null ? new ArrayList<>() : new ArrayList<>(children);
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return children;
    }

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        List<StandardRetrieverBuilder> leaves = new ArrayList<>();
        for (RetrieverBuilder child : children) {
            leaves.addAll(child.collectLeaves());
        }
        return leaves;
    }

    @Override
    public void validate() {
        if (children.size() < MIN_CHILDREN) {
            throw new IllegalArgumentException(
                "[" + getName() + "] requires at least " + MIN_CHILDREN + " child retrievers in [retrievers], but got " + children.size()
            );
        }
        for (RetrieverBuilder child : children) {
            child.validate();
        }
    }

    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        // Default compound behavior: propagate the ancestor context unchanged. A fusion compound that
        // governs its own window (e.g. RankFusionRetrieverBuilder) overrides this to derive underFusion(...).
        for (RetrieverBuilder child : children) {
            child.prepareLeaves(context);
        }
    }

    @Override
    void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
        // Fan out to children; once all are resolved, fuse them inline on the last child's callback thread.
        // Record wall time from dispatch to fused completion (includes waiting for children); doResolve
        // separately times the fuse() own-compute for the profile breakdown.
        final long startNanos = System.nanoTime();
        resolveChildren(client, indices, original, ActionListener.wrap(v -> {
            doResolve();
            this.nodeElapsedNanos = System.nanoTime() - startNanos;
            whenDone.onResponse(null);
        }, whenDone::onFailure));
    }

    @Override
    void doResolve() {
        List<List<RetrieverCandidate>> childResults = new ArrayList<>(children.size());
        for (RetrieverBuilder child : children) {
            List<RetrieverCandidate> childResult = child.getResolvedResult();
            childResults.add(childResult == null ? List.of() : childResult);
        }
        long fuseStartNanos = System.nanoTime();
        this.resolvedResult = fuse(childResults);
        this.fuseElapsedNanos = System.nanoTime() - fuseStartNanos;
    }

    /**
     * Fuse the children's resolved candidate lists (in child order) into this node's ranking. Pure
     * function of its inputs — deterministic, no I/O. Implemented by each concrete compound (e.g. RRF for
     * {@link RankFusionRetrieverBuilder}).
     *
     * @param childResults each child's resolved candidates, in child order
     * @return the fused, ordered candidate list (already truncated to this compound's window)
     */
    protected abstract List<RetrieverCandidate> fuse(List<List<RetrieverCandidate>> childResults);

    @Override
    public QueryBuilder toQueryBuilder() {
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
        // This node contributed the document only if it survived into the fused window. Find its fused
        // score there, then delegate the formula + per-child detail assembly to the concrete compound.
        if (resolvedResult != null) {
            for (RetrieverCandidate candidate : resolvedResult) {
                if (candidate.index().equals(index) && candidate.id().equals(id)) {
                    return buildFusionExplanation(index, id, candidate.score());
                }
            }
        }
        return null;
    }

    /**
     * Assemble this compound's explanation for a document that survived into its fused window: a root
     * describing the fusion formula (and its configuration) with one detail per child. Concrete compounds
     * use {@link #getChildExplanation(RetrieverBuilder, String, String)} to fetch each child's subtree and
     * describe that child's contribution (e.g. an RRF rank term or a normalized-and-weighted score).
     *
     * @param index      the document's index name
     * @param id         the document's {@code _id}
     * @param fusedScore this node's fused score for the document (matches the value rendered at the root)
     * @return the explanation subtree rooted at this compound for the document
     */
    protected abstract Explanation buildFusionExplanation(String index, String id, float fusedScore);

    /**
     * Fetch a child's explanation subtree for a document, or a neutral "not present in this leg" node when
     * the child did not contribute the document. Never returns {@code null}, so a concrete compound can
     * always nest a child detail (making absent legs explicit in the tree).
     */
    protected Explanation getChildExplanation(RetrieverBuilder child, String index, String id) {
        Explanation childExplanation = child.buildExplanation(index, id);
        if (childExplanation != null) {
            return childExplanation;
        }
        return Explanation.noMatch("not present in this leg");
    }

    @Override
    public RetrieverProfile.Node buildProfile() {
        // A compound reports its wall time (incl. waiting for children) and one child profile node per child.
        // Its breakdown itemizes that wall time so it reconciles exactly:
        // total = fuse + orchestration_overhead + max(child total)
        // - fuse: the fuse() own-compute (pure, no I/O).
        // - orchestration_overhead: everything else the node spends between starting and finishing that is
        // NOT a child's own measured time — child fan-out (request build, dispatch, the leg-concurrency
        // limiter), listener/thread hand-off, the pre-fuse gathering of child results, and parallel
        // scheduling skew. Children run in PARALLEL, so productive child time is max(child), not the sum;
        // orchestration_overhead is therefore nodeElapsed - fuse - max(child), emitted only when positive.
        List<RetrieverProfile.Node> childProfiles = new ArrayList<>(children.size());
        long maxChildNanos = 0L;
        for (RetrieverBuilder child : children) {
            RetrieverProfile.Node childProfile = child.buildProfile();
            childProfiles.add(childProfile);
            maxChildNanos = Math.max(maxChildNanos, childProfile.getTotalTimeInNanos());
        }
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("fuse", fuseElapsedNanos);
        long orchestrationOverhead = nodeElapsedNanos - fuseElapsedNanos - maxChildNanos;
        if (orchestrationOverhead > 0) {
            breakdown.put("orchestration_overhead", orchestrationOverhead);
        }
        return RetrieverProfile.inner(getName(), nodeElapsedNanos, breakdown, childProfiles);
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        // Union of the children's aggregation queries: aggs/total count over everything any leg matched.
        BoolQueryBuilder union = new BoolQueryBuilder();
        for (RetrieverBuilder child : children) {
            union.should(child.extractAggregationQuery());
        }
        return union;
    }

    /**
     * A document's identity across fusion: {@code (index, _id)}. Within one index an {@code _id} lives on a
     * single shard, so shardId is implied; across indices the same {@code _id} is a distinct document.
     */
    protected static String fusionKey(RetrieverCandidate candidate) {
        return candidate.index() + "\u0000" + candidate.id();
    }

    /**
     * Shared fusion tail used by every concrete compound once it has computed a fused score per document.
     * Sorts by descending fused score (stable — ties keep first-seen order, which is deterministic given
     * deterministic child inputs), truncates to {@code windowSize}, applies an optional fused
     * {@code minScore} threshold, and renumbers positions {@code 0..n} in the fused order.
     *
     * @param fusedScore     fused score per fusion key, in first-seen insertion order (use a LinkedHashMap)
     * @param representative one representative candidate per fusion key, to carry identity into the result
     * @param windowSize     truncate the fused ranking to this many documents
     * @param minScore       optional fused-score floor; documents scoring below it are dropped (nullable)
     * @return the fused, ordered, truncated candidate list with positions renumbered
     */
    protected static List<RetrieverCandidate> finalizeFusion(
        Map<String, Double> fusedScore,
        Map<String, RetrieverCandidate> representative,
        int windowSize,
        Float minScore
    ) {
        List<String> orderedKeys = new ArrayList<>(fusedScore.keySet());
        orderedKeys.sort(Comparator.comparingDouble((String k) -> fusedScore.get(k)).reversed());

        List<RetrieverCandidate> fused = new ArrayList<>(Math.min(orderedKeys.size(), windowSize));
        int position = 0;
        for (String key : orderedKeys) {
            if (position >= windowSize) {
                break;
            }
            float score = (float) (double) fusedScore.get(key);
            if (minScore != null && score < minScore) {
                continue;
            }
            fused.add(representative.get(key).withScoreAndPosition(score, position));
            position++;
        }
        return fused;
    }

    /**
     * Write the children as the {@code retrievers} array. Each child's {@link #toXContent} emits a
     * <b>named</b> object ({@code {"<type>": {...}}}), which is only valid inside an enclosing object — so
     * each child must be wrapped in an anonymous object here to produce the {@code [{"standard":{...}}, ...]}
     * shape that {@link RetrieverBuilder#parseInnerRetrieverBuilder} reads back. Writing the child directly
     * into the array would emit a field name with no enclosing object and fail serialization.
     *
     * @param builder    the content builder, positioned to receive a field
     * @param params     xcontent params passed through to each child
     * @param fieldName  the array field name (each concrete compound owns its own constant)
     */
    protected void writeChildrenArray(XContentBuilder builder, ToXContent.Params params, String fieldName) throws IOException {
        builder.startArray(fieldName);
        for (RetrieverBuilder child : children) {
            builder.startObject();
            child.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
    }
}
