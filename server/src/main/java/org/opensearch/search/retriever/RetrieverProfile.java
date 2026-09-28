/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Tree-structured profile for one retriever request, serialized into the response {@code profile} section.
 * <p>
 * A retriever request runs in <b>two sequential coordinator phases</b>, and the profile mirrors that shape
 * so the numbers reconcile top-down:
 * <ol>
 *   <li><b>{@code self_resolve}</b> — resolving the retriever into a concrete {@code RankDocsQuery}. This
 *       phase does two things <b>in parallel</b>, rendered as siblings at the same level:
 *       <ul>
 *         <li><b>{@code retriever}</b> — the retriever node tree (each node's wall-clock
 *             {@code total_time_in_nanos}, its own-compute {@code breakdown} such as {@code fuse}, its
 *             {@code children}, and — for a leaf — its per-shard query profiles under {@code shards});</li>
 *         <li><b>{@code global_leg}</b> — the union search (aggregations / {@code track_total_hits} over the
 *             union of leaf queries), present only when a global leg ran; carries its own
 *             {@code total_time_in_nanos} and per-shard profiles.</li>
 *       </ul>
 *       Because the two run concurrently, the phase's productive time is {@code max(retriever, global_leg)};
 *       {@code self_resolve.total_time_in_nanos} is the phase wall time and its
 *       {@code breakdown.coordinator_overhead} is {@code total - max(retriever, global_leg)} (latch,
 *       dispatch, and coordinator assembly slack).</li>
 *   <li><b>{@code rank_docs_query}</b> — the final {@code RankDocsQuery} fetch search. Carries its per-shard
 *       profiles under {@code shards}, its wall {@code total_time_in_nanos}, and a
 *       {@code breakdown.coordinator_overhead} of {@code total - max(shard query times)} (rewrite, dispatch,
 *       and reduce on top of the pure shard work).</li>
 * </ol>
 * The two phases are <b>sequential</b> (resolution completes before the final search fires), so the
 * top-level <b>{@code total_time_in_nanos}</b> is exactly {@code self_resolve + rank_docs_query} — a true
 * coordinator wall clock in which every sub-number is accounted for.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public class RetrieverProfile implements ToXContentFragment {

    public static final String PROFILE_FIELD = "profile";
    public static final String SELF_RESOLVE_FIELD = "self_resolve";
    public static final String RETRIEVER_FIELD = "retriever";
    public static final String GLOBAL_LEG_FIELD = "global_leg";
    public static final String RANK_DOCS_QUERY_FIELD = "rank_docs_query";
    public static final String TOTAL_TIME_FIELD = "total_time_in_nanos";

    static final String TYPE_FIELD = "type";
    static final String BREAKDOWN_FIELD = "breakdown";
    static final String CHILDREN_FIELD = "children";
    static final String SHARDS_FIELD = "shards";
    static final String ID_FIELD = "id";
    static final String SEARCHES_FIELD = "searches";

    /** Breakdown key for the coordinator time a phase spends beyond its productive (max-of-parallel) work. */
    public static final String COORDINATOR_OVERHEAD = "coordinator_overhead";

    private final Node retriever;
    private final long selfResolveTimeInNanos;
    private final Map<String, ProfileShardResult> globalLegProfile;
    private final long globalLegTimeInNanos;
    private final Map<String, ProfileShardResult> rankDocsQueryProfile;
    private final long rankDocsQueryTimeInNanos;

    private RetrieverProfile(
        Node retriever,
        long selfResolveTimeInNanos,
        Map<String, ProfileShardResult> globalLegProfile,
        long globalLegTimeInNanos,
        Map<String, ProfileShardResult> rankDocsQueryProfile,
        long rankDocsQueryTimeInNanos
    ) {
        this.retriever = retriever;
        this.selfResolveTimeInNanos = selfResolveTimeInNanos;
        this.globalLegProfile = globalLegProfile;
        this.globalLegTimeInNanos = globalLegTimeInNanos;
        this.rankDocsQueryProfile = rankDocsQueryProfile;
        this.rankDocsQueryTimeInNanos = rankDocsQueryTimeInNanos;
    }

    public Node getRetriever() {
        return retriever;
    }

    public long getSelfResolveTimeInNanos() {
        return selfResolveTimeInNanos;
    }

    public Map<String, ProfileShardResult> getGlobalLegProfile() {
        return globalLegProfile;
    }

    public long getGlobalLegTimeInNanos() {
        return globalLegTimeInNanos;
    }

    public Map<String, ProfileShardResult> getRankDocsQueryProfile() {
        return rankDocsQueryProfile;
    }

    public long getRankDocsQueryTimeInNanos() {
        return rankDocsQueryTimeInNanos;
    }

    /** Top-level total: the two sequential phases summed. */
    public long getTotalTimeInNanos() {
        return selfResolveTimeInNanos + rankDocsQueryTimeInNanos;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PROFILE_FIELD);

        // Phase 1: self_resolve — the retriever tree and the (optional) global leg run in parallel here.
        builder.startObject(SELF_RESOLVE_FIELD);
        builder.field(TOTAL_TIME_FIELD, selfResolveTimeInNanos);
        long parallelMax = Math.max(retriever == null ? 0L : retriever.getTotalTimeInNanos(), globalLegTimeInNanos);
        writeOverheadBreakdown(builder, selfResolveTimeInNanos, parallelMax);
        if (retriever != null) {
            builder.field(RETRIEVER_FIELD);
            retriever.toXContent(builder, params);
        }
        if (globalLegProfile != null && globalLegProfile.isEmpty() == false) {
            builder.startObject(GLOBAL_LEG_FIELD);
            builder.field(TOTAL_TIME_FIELD, globalLegTimeInNanos);
            writeShards(builder, params, globalLegProfile);
            builder.endObject();
        }
        builder.endObject();

        // Phase 2: rank_docs_query — the final fetch search, sequential after self_resolve.
        if (rankDocsQueryProfile != null && rankDocsQueryProfile.isEmpty() == false) {
            builder.startObject(RANK_DOCS_QUERY_FIELD);
            builder.field(TOTAL_TIME_FIELD, rankDocsQueryTimeInNanos);
            writeOverheadBreakdown(builder, rankDocsQueryTimeInNanos, maxShardQueryTime(rankDocsQueryProfile));
            writeShards(builder, params, rankDocsQueryProfile);
            builder.endObject();
        }

        builder.field(TOTAL_TIME_FIELD, getTotalTimeInNanos());
        builder.endObject();
        return builder;
    }

    /**
     * Emit a {@code breakdown: { coordinator_overhead: <phaseWall - productive> }} for a phase, where
     * {@code productive} is the phase's productive work (the max across its parallel parts / shards).
     * Skipped when the overhead is not positive (e.g. timings not captured, or clock skew), so the
     * breakdown never shows a misleading zero or negative.
     */
    private static void writeOverheadBreakdown(XContentBuilder builder, long phaseWallNanos, long productiveNanos)
        throws IOException {
        long overhead = phaseWallNanos - productiveNanos;
        if (overhead > 0) {
            builder.startObject(BREAKDOWN_FIELD);
            builder.field(COORDINATOR_OVERHEAD, overhead);
            builder.endObject();
        }
    }

    /** The maximum {@code time_in_nanos} across a per-shard profile map's query profiles (0 when empty). */
    private static long maxShardQueryTime(Map<String, ProfileShardResult> shardResults) {
        long max = 0L;
        for (ProfileShardResult shard : shardResults.values()) {
            for (QueryProfileShardResult queryResult : shard.getQueryProfileResults()) {
                max = Math.max(max, queryResult.getQueryResults().stream().mapToLong(org.opensearch.search.profile.ProfileResult::getTime).max().orElse(0L));
            }
        }
        return max;
    }

    /**
     * Render a per-shard profile map as a {@code "shards":[...]} array, mirroring the standard profile
     * output ({@code SearchProfileShardResults}) so the shape is familiar and reuses the same per-shard
     * {@link QueryProfileShardResult} rendering. Keys are sorted for deterministic output.
     */
    static void writeShards(XContentBuilder builder, Params params, Map<String, ProfileShardResult> shardResults) throws IOException {
        builder.startArray(SHARDS_FIELD);
        for (String key : new TreeSet<>(shardResults.keySet())) {
            ProfileShardResult shard = shardResults.get(key);
            builder.startObject();
            builder.field(ID_FIELD, key);
            builder.startArray(SEARCHES_FIELD);
            for (QueryProfileShardResult queryResult : shard.getQueryProfileResults()) {
                queryResult.toXContent(builder, params);
            }
            builder.endArray();
            shard.getAggregationProfileResults().toXContent(builder, params);
            shard.getFetchProfileResult().toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * One node of the retriever profile tree. {@code type} is the retriever type name; {@code totalTime} is
     * the node's wall-clock time; {@code breakdown} is the node's own coordinator computation (empty for a
     * leaf); {@code children} are the child nodes (empty for a leaf); {@code shards} are the per-shard
     * query profiles for a leaf's sub-search (null for a non-leaf).
     */
    @PublicApi(since = "3.7.0")
    public static class Node implements ToXContentObject {
        private final String type;
        private final long totalTimeInNanos;
        private final Map<String, Long> breakdown;
        private final List<Node> children;
        private final Map<String, ProfileShardResult> shards;

        Node(String type, long totalTimeInNanos, Map<String, Long> breakdown, List<Node> children, Map<String, ProfileShardResult> shards) {
            this.type = type;
            this.totalTimeInNanos = totalTimeInNanos;
            this.breakdown = breakdown == null ? Map.of() : breakdown;
            this.children = children == null ? List.of() : children;
            this.shards = shards;
        }

        public String getType() {
            return type;
        }

        public long getTotalTimeInNanos() {
            return totalTimeInNanos;
        }

        public Map<String, Long> getBreakdown() {
            return breakdown;
        }

        public List<Node> getChildren() {
            return children;
        }

        public Map<String, ProfileShardResult> getShards() {
            return shards;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD, type);
            builder.field(TOTAL_TIME_FIELD, totalTimeInNanos);
            if (breakdown.isEmpty() == false) {
                builder.startObject(BREAKDOWN_FIELD);
                for (Map.Entry<String, Long> entry : breakdown.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }
            if (shards != null && shards.isEmpty() == false) {
                writeShards(builder, params, shards);
            }
            if (children.isEmpty() == false) {
                builder.startArray(CHILDREN_FIELD);
                for (Node child : children) {
                    child.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }
    }

    /** Builder for a leaf node: type + dispatch time + per-shard query profiles. */
    public static Node leaf(String type, long totalTimeInNanos, Map<String, ProfileShardResult> shards) {
        return new Node(type, totalTimeInNanos, Map.of(), List.of(), shards);
    }

    /** Builder for a compound/transformer node: type + wall time + own-compute breakdown + children. */
    public static Node inner(String type, long totalTimeInNanos, Map<String, Long> breakdown, List<Node> children) {
        return new Node(type, totalTimeInNanos, breakdown, children, null);
    }

    /** Top-level {@link RetrieverProfile} builder. */
    @PublicApi(since = "3.7.0")
    public static final class Builder {
        private Node retriever;
        private long selfResolveTimeInNanos;
        private Map<String, ProfileShardResult> globalLegProfile;
        private long globalLegTimeInNanos;
        private Map<String, ProfileShardResult> rankDocsQueryProfile;
        private long rankDocsQueryTimeInNanos;

        public Builder retriever(Node retriever) {
            this.retriever = retriever;
            return this;
        }

        /** Wall time (ns) of the whole self_resolve phase (tree + global leg run in parallel). */
        public Builder selfResolveTimeInNanos(long selfResolveTimeInNanos) {
            this.selfResolveTimeInNanos = selfResolveTimeInNanos;
            return this;
        }

        public Builder globalLegProfile(Map<String, ProfileShardResult> globalLegProfile) {
            this.globalLegProfile = globalLegProfile == null ? null : new LinkedHashMap<>(globalLegProfile);
            return this;
        }

        /** Wall time (ns) of the global leg search (0 when no global leg ran). */
        public Builder globalLegTimeInNanos(long globalLegTimeInNanos) {
            this.globalLegTimeInNanos = globalLegTimeInNanos;
            return this;
        }

        public Builder rankDocsQueryProfile(Map<String, ProfileShardResult> rankDocsQueryProfile) {
            this.rankDocsQueryProfile = rankDocsQueryProfile == null ? null : new LinkedHashMap<>(rankDocsQueryProfile);
            return this;
        }

        /** Wall time (ns) of the final RankDocsQuery fetch phase. */
        public Builder rankDocsQueryTimeInNanos(long rankDocsQueryTimeInNanos) {
            this.rankDocsQueryTimeInNanos = rankDocsQueryTimeInNanos;
            return this;
        }

        public RetrieverProfile build() {
            return new RetrieverProfile(
                retriever,
                selfResolveTimeInNanos,
                globalLegProfile,
                globalLegTimeInNanos,
                rankDocsQueryProfile,
                rankDocsQueryTimeInNanos
            );
        }
    }

    /** Convenience for a single-entry breakdown map (insertion-ordered for stable output). */
    public static Map<String, Long> breakdown(String key, long nanos) {
        Map<String, Long> b = new LinkedHashMap<>();
        b.put(key, nanos);
        return b;
    }
}
