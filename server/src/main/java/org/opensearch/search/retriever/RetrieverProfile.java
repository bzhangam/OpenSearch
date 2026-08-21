/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures profiling information for a retriever node's execution.
 * Tree-structured: compounds have leg profiles, transformers have a child profile.
 * Leaves include per-shard profile data from their sub-search responses.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RetrieverProfile implements ToXContentObject {

    private final String type;
    private final long totalTimeNanos;
    private final Map<String, Long> breakdown;
    private final List<RetrieverProfile> legs;       // for compounds (null for transformers/leaves)
    private final RetrieverProfile child;            // for transformers (null for compounds/leaves)
    private final Map<String, ProfileShardResult> shardProfiles; // for leaves (null for compounds/transformers)

    private RetrieverProfile(Builder builder) {
        this.type = builder.type;
        this.totalTimeNanos = builder.totalTimeNanos;
        this.breakdown = Collections.unmodifiableMap(builder.breakdown);
        this.legs = builder.legs != null ? Collections.unmodifiableList(builder.legs) : null;
        this.child = builder.child;
        this.shardProfiles = builder.shardProfiles;
    }

    public String getType() {
        return type;
    }

    public long getTotalTimeNanos() {
        return totalTimeNanos;
    }

    public Map<String, Long> getBreakdown() {
        return breakdown;
    }

    public List<RetrieverProfile> getLegs() {
        return legs;
    }

    public RetrieverProfile getChild() {
        return child;
    }

    public Map<String, ProfileShardResult> getShardProfiles() {
        return shardProfiles;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.field("total_time_in_nanos", totalTimeNanos);

        if (!breakdown.isEmpty()) {
            builder.startObject("breakdown");
            for (Map.Entry<String, Long> entry : breakdown.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (legs != null && !legs.isEmpty()) {
            builder.startArray("legs");
            for (RetrieverProfile leg : legs) {
                leg.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (child != null) {
            builder.field("child");
            child.toXContent(builder, params);
        }

        if (shardProfiles != null && !shardProfiles.isEmpty()) {
            builder.startArray("shards");
            for (Map.Entry<String, ProfileShardResult> entry : shardProfiles.entrySet()) {
                builder.startObject();
                builder.field("id", entry.getKey());
                builder.startArray("searches");
                for (QueryProfileShardResult result : entry.getValue().getQueryProfileResults()) {
                    result.toXContent(builder, params);
                }
                builder.endArray();
                entry.getValue().getAggregationProfileResults().toXContent(builder, params);
                entry.getValue().getFetchProfileResult().toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    /**
     * The full profile section for a retriever search response.
     * Contains the retriever tree profile, global leg profile, rank_docs_query profile,
     * and overall total time.
     */
    public static class FullRetrieverProfileResult implements ToXContentFragment {
        private final RetrieverProfile retrieverProfile;
        private final Map<String, ProfileShardResult> globalLegShards;
        private final Map<String, ProfileShardResult> rankDocsQueryShards;
        private final long totalTimeNanos;

        public FullRetrieverProfileResult(
            RetrieverProfile retrieverProfile,
            Map<String, ProfileShardResult> globalLegShards,
            Map<String, ProfileShardResult> rankDocsQueryShards,
            long totalTimeNanos
        ) {
            this.retrieverProfile = retrieverProfile;
            this.globalLegShards = globalLegShards;
            this.rankDocsQueryShards = rankDocsQueryShards;
            this.totalTimeNanos = totalTimeNanos;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("profile");
            builder.field("total_time_in_nanos", totalTimeNanos);

            if (retrieverProfile != null) {
                builder.field("retriever");
                retrieverProfile.toXContent(builder, params);
            }

            if (globalLegShards != null && !globalLegShards.isEmpty()) {
                builder.startObject("global_leg");
                writeShards(builder, params, globalLegShards);
                builder.endObject();
            }

            if (rankDocsQueryShards != null && !rankDocsQueryShards.isEmpty()) {
                builder.startObject("rank_docs_query");
                writeShards(builder, params, rankDocsQueryShards);
                builder.endObject();
            }

            builder.endObject();
            return builder;
        }

        private void writeShards(XContentBuilder builder, Params params, Map<String, ProfileShardResult> shards) throws IOException {
            builder.startArray("shards");
            for (Map.Entry<String, ProfileShardResult> entry : shards.entrySet()) {
                builder.startObject();
                builder.field("id", entry.getKey());
                builder.startArray("searches");
                for (QueryProfileShardResult result : entry.getValue().getQueryProfileResults()) {
                    result.toXContent(builder, params);
                }
                builder.endArray();
                entry.getValue().getAggregationProfileResults().toXContent(builder, params);
                entry.getValue().getFetchProfileResult().toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }

        public RetrieverProfile getRetrieverProfile() {
            return retrieverProfile;
        }

        public long getTotalTimeNanos() {
            return totalTimeNanos;
        }
    }

    /**
     * Builder for constructing RetrieverProfile instances.
     */
    public static class Builder {
        private String type;
        private long totalTimeNanos;
        private final Map<String, Long> breakdown = new LinkedHashMap<>();
        private List<RetrieverProfile> legs;
        private RetrieverProfile child;
        private Map<String, ProfileShardResult> shardProfiles;

        public Builder(String type) {
            this.type = type;
        }

        public Builder totalTimeNanos(long nanos) {
            this.totalTimeNanos = nanos;
            return this;
        }

        public Builder addBreakdown(String key, long nanos) {
            this.breakdown.put(key, nanos);
            return this;
        }

        public Builder legs(List<RetrieverProfile> legs) {
            this.legs = legs;
            return this;
        }

        public Builder child(RetrieverProfile child) {
            this.child = child;
            return this;
        }

        public Builder shardProfiles(Map<String, ProfileShardResult> shardProfiles) {
            this.shardProfiles = shardProfiles;
            return this;
        }

        public RetrieverProfile build() {
            return new RetrieverProfile(this);
        }
    }
}
