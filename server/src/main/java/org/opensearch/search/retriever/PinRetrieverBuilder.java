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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A top-level reranker retriever that forces a curated set of documents to the top of a single child
 * retriever's ranking, in the exact order given — the retriever-framework analogue of the {@code pinned}
 * query, but composable over any child retriever (a {@code standard} leg, a {@code rank_fusion} subtree,
 * etc.).
 * <p>
 * <b>Shape.</b> {@code pin} wraps exactly one child ({@code retriever}) and a curated pin list, specified
 * either as {@code ids} (a list of {@code _id}s in the searched index) or {@code docs} (a list of
 * {@code {_id, _index}} for cross-index pinning) — exactly one of the two. It is <b>not</b> a
 * {@link CompoundRetrieverBuilder}: it does not fuse multiple rankings, it reshapes one child's ranking,
 * so it extends {@link RetrieverBuilder} directly as a single-child reranker.
 * <p>
 * <b>Match modes.</b> {@code require_match} (default {@code false}) selects the behavior:
 * <ul>
 *   <li>{@code false} — <b>always pin</b> (default; matches the {@code pinned} query and the merchandising
 *       use case): a pinned document is placed at the top <i>even if the child query did not match it</i>.
 *       Because such a document is absent from the child's resolved window, its {@code (index, shardId)} —
 *       required to scope the final {@code RankDocsQuery} fetch to the right shard — is resolved by a single
 *       bounded {@code ids} sub-search over the not-yet-located pins. A pin that resolves to no document
 *       (deleted / never existed) is silently skipped.</li>
 *   <li>{@code true} — <b>pin only when matched</b>: a pinned document is moved to the top only if it is
 *       already in the child's resolved window; pins the child did not match are dropped. No id sub-search
 *       is needed.</li>
 * </ul>
 * <b>Ordering by score, no injected sort.</b> Pinned documents receive strictly-descending synthetic
 * scores placed above the child's maximum organic score (see {@link #assignPinnedScores}), so ordering by
 * {@code _score} alone reproduces the exact pin order — no position sort is injected. This is deliberately
 * different from rerankers whose display order is decoupled from score (e.g. MMR), which do require the
 * framework's {@code rank_docs_sort}; pinning keeps score and position monotonic, so no position sort is
 * needed. The synthetic-score gap is auto-chosen to guarantee
 * strict separation regardless of the organic score scale; a user-configurable gap is intentionally not
 * exposed in this version.
 * <p>
 * <b>Dedup.</b> A pinned document that also appears organically is emitted once, at its pinned position;
 * it is removed from the organic tail.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.7.0")
public class PinRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "pin";

    static final String IDS_FIELD = "ids";
    static final String DOCS_FIELD = "docs";
    static final String RETRIEVER_FIELD = "retriever";
    static final String REQUIRE_MATCH_FIELD = "require_match";

    static final String DOC_ID_FIELD = "_id";
    static final String DOC_INDEX_FIELD = "_index";

    public static final boolean DEFAULT_REQUIRE_MATCH = false;

    /** One curated pin target: an {@code _id} and an optional {@code _index} (null → the searched index). */
    @PublicApi(since = "3.7.0")
    public static final class PinnedDoc {
        private final String id;
        private final String index; // nullable

        public PinnedDoc(String id, String index) {
            this.id = Objects.requireNonNull(id, "pinned doc _id");
            this.index = index;
        }

        public String id() {
            return id;
        }

        public String index() {
            return index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PinnedDoc other = (PinnedDoc) o;
            return id.equals(other.id) && Objects.equals(index, other.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, index);
        }
    }

    private final List<PinnedDoc> pinnedDocs;
    private final RetrieverBuilder retriever;
    private boolean requireMatch = DEFAULT_REQUIRE_MATCH;

    // Coordinator-only resolution state.
    private boolean explain;
    private boolean profile;
    private long childElapsedNanos;   // child subtree wall time (for profile)
    // Pins resolved to a concrete (index, shardId) by the require_match:false id sub-search, keyed by
    // index + '\u0000' + _id. Empty for require_match:true (pins come from the child window instead).
    private Map<String, ShardId> resolvedPinShards = Map.of();

    public PinRetrieverBuilder(List<PinnedDoc> pinnedDocs, RetrieverBuilder retriever) {
        this.pinnedDocs = pinnedDocs == null ? new ArrayList<>() : new ArrayList<>(pinnedDocs);
        this.retriever = retriever;
    }

    public List<PinnedDoc> getPinnedDocs() {
        return Collections.unmodifiableList(pinnedDocs);
    }

    public RetrieverBuilder getRetriever() {
        return retriever;
    }

    public boolean isRequireMatch() {
        return requireMatch;
    }

    public void setRequireMatch(boolean requireMatch) {
        this.requireMatch = requireMatch;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<RetrieverBuilder> getChildRetrievers() {
        return retriever == null ? Collections.emptyList() : Collections.singletonList(retriever);
    }

    @Override
    public List<StandardRetrieverBuilder> collectLeaves() {
        return retriever == null ? Collections.emptyList() : retriever.collectLeaves();
    }

    @Override
    public void validate() {
        if (retriever == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a [" + RETRIEVER_FIELD + "] child retriever");
        }
        if (pinnedDocs.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires a non-empty [" + IDS_FIELD + "] or [" + DOCS_FIELD + "] pin list");
        }
        retriever.validate();
    }

    @Override
    public void prepareLeaves(LeafPreparationContext context) {
        // pin is a top-level reranker: it must not appear inside a fusion subtree. A fusion node governs
        // its legs' window/size so their fused window is complete and stable; injecting a pin into that
        // contract would change the leg's membership in a way the fusion cannot account for.
        if (context.isFusionGoverned()) {
            throw new IllegalArgumentException(
                "[" + NAME + "] retriever is only allowed at the top level, not inside a [rank_fusion]/[score_fusion] retriever"
            );
        }
        this.explain = context.isExplain();
        this.profile = context.isProfile();
        // The child is not fusion-governed by pin (pin does not impose a window on its child); propagate the
        // root flags unchanged so the child keeps its own sizing and the explain/profile flags flow down.
        retriever.prepareLeaves(LeafPreparationContext.root(context.isExplain(), context.isProfile()));
    }

    @Override
    public QueryBuilder extractAggregationQuery() {
        // Aggregations / track_total_hits are computed over what the child matched; pinned docs that the
        // child did not match are a display-time curation and must not inflate the union count.
        return retriever.extractAggregationQuery();
    }

    @Override
    void resolve(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
        final long startNanos = profile ? System.nanoTime() : 0L;
        // First resolve the child subtree (the organic ranking). Then, on the always-pin path, resolve the
        // shards of any pins the child did not surface via a bounded ids sub-search; finally reorder inline.
        retriever.resolve(client, indices, original, ActionListener.wrap(v -> {
            if (profile) {
                this.childElapsedNanos = System.nanoTime() - startNanos;
            }
            if (requireMatch) {
                // Pin only what the child matched — no extra I/O.
                doResolve();
                if (profile) {
                    this.nodeElapsedNanos = System.nanoTime() - startNanos;
                }
                whenDone.onResponse(null);
                return;
            }
            // Always-pin: locate pins missing from the child window via an ids sub-search so we can build a
            // RankDoc (which needs the shard) for the final fetch.
            resolveMissingPinShards(client, indices, original, ActionListener.wrap(unused -> {
                doResolve();
                if (profile) {
                    this.nodeElapsedNanos = System.nanoTime() - startNanos;
                }
                whenDone.onResponse(null);
            }, whenDone::onFailure));
        }, whenDone::onFailure));
    }

    /**
     * For {@code require_match:false}: find the pins that are NOT already in the child's resolved window and
     * resolve their {@code (index, shardId)} with a single {@code ids} sub-search (size = number of missing
     * pins), so a {@link RankDoc} can be built for the final fetch. Pins that the sub-search does not return
     * (deleted / never existed) stay unresolved and are dropped in {@link #doResolve()}.
     */
    private void resolveMissingPinShards(Client client, String[] indices, SearchRequest original, ActionListener<Void> whenDone) {
        Set<String> childKeys = new LinkedHashSet<>();
        List<RetrieverCandidate> childWindow = retriever.getResolvedResult();
        if (childWindow != null) {
            for (RetrieverCandidate c : childWindow) {
                childKeys.add(key(c.index(), c.id()));
            }
        }
        // Collect the distinct pin ids not already present in the child window.
        List<String> missingIds = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (PinnedDoc doc : pinnedDocs) {
            // A pin with an explicit _index is matched by (index,_id); without one, it targets the searched
            // index, so we still key it by the resolved hit's index below.
            boolean presentInChild = doc.index() != null
                ? childKeys.contains(key(doc.index(), doc.id()))
                : childWindowContainsId(childWindow, doc.id());
            if (presentInChild == false && seen.add(doc.id())) {
                missingIds.add(doc.id());
            }
        }
        if (missingIds.isEmpty()) {
            this.resolvedPinShards = Map.of();
            whenDone.onResponse(null);
            return;
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(new IdsQueryBuilder().addIds(missingIds.toArray(new String[0])))
            .size(missingIds.size())
            .fetchSource(false)
            .trackScores(false);
        SearchRequest req = new SearchRequest(indices);
        req.source(source);
        if (original != null) {
            req.preference(original.preference());
            req.routing(original.routing());
            if (original.source() != null && original.source().pointInTimeBuilder() != null) {
                source.pointInTimeBuilder(original.source().pointInTimeBuilder());
            }
        }
        client.search(req, ActionListener.wrap(response -> {
            Map<String, ShardId> resolved = new LinkedHashMap<>();
            for (SearchHit hit : response.getHits().getHits()) {
                ShardId shardId = hit.getShard() != null ? hit.getShard().getShardId() : null;
                if (shardId != null) {
                    resolved.put(key(hit.getIndex(), hit.getId()), shardId);
                    // Also key by (searchedIndex-agnostic) id so an index-less pin can find it.
                    resolved.putIfAbsent(idOnlyKey(hit.getId()), shardId);
                    resolvedPinIndexById.putIfAbsent(hit.getId(), hit.getIndex());
                }
            }
            this.resolvedPinShards = resolved;
            whenDone.onResponse(null);
        }, whenDone::onFailure));
    }

    // Resolved index name for an index-less pin id, learned from the ids sub-search.
    private final Map<String, String> resolvedPinIndexById = new LinkedHashMap<>();

    private static boolean childWindowContainsId(List<RetrieverCandidate> childWindow, String id) {
        if (childWindow == null) {
            return false;
        }
        for (RetrieverCandidate c : childWindow) {
            if (c.id().equals(id)) {
                return true;
            }
        }
        return false;
    }

    @Override
    void doResolve() {
        List<RetrieverCandidate> childWindow = retriever.getResolvedResult();
        if (childWindow == null) {
            childWindow = List.of();
        }

        // Index the child window by (index,_id) and by id-only, so a pin can be found whether or not it
        // carried an explicit _index.
        Map<String, RetrieverCandidate> childByKey = new LinkedHashMap<>();
        Map<String, RetrieverCandidate> childById = new LinkedHashMap<>();
        for (RetrieverCandidate c : childWindow) {
            childByKey.put(key(c.index(), c.id()), c);
            childById.putIfAbsent(c.id(), c);
        }

        // Resolve each pin (in pin order) to a concrete (index, shardId, _id), dropping unresolvable pins.
        // Dedup pins by identity so a repeated pin does not occupy two slots.
        List<RetrieverCandidate> pinnedResolved = new ArrayList<>();
        Set<String> pinnedKeys = new LinkedHashSet<>();
        for (PinnedDoc doc : pinnedDocs) {
            RetrieverCandidate fromChild = doc.index() != null ? childByKey.get(key(doc.index(), doc.id())) : childById.get(doc.id());
            RetrieverCandidate resolvedPin = null;
            if (fromChild != null) {
                resolvedPin = fromChild; // present in child window (works for both match modes)
            } else if (requireMatch == false) {
                // Always-pin: use the shard resolved by the ids sub-search, if the doc exists.
                String index = doc.index() != null ? doc.index() : resolvedPinIndexById.get(doc.id());
                ShardId shardId = doc.index() != null
                    ? resolvedPinShards.get(key(doc.index(), doc.id()))
                    : resolvedPinShards.get(idOnlyKey(doc.id()));
                if (index != null && shardId != null) {
                    resolvedPin = new RetrieverCandidate(index, shardId, doc.id(), 0f, 0);
                }
            }
            if (resolvedPin == null) {
                continue; // pin not matched (require_match:true) or not found (always-pin): skip
            }
            String k = key(resolvedPin.index(), resolvedPin.id());
            if (pinnedKeys.add(k)) {
                pinnedResolved.add(resolvedPin);
            }
        }

        // Organic tail = child window minus the pinned docs (dedup), preserving child order.
        List<RetrieverCandidate> organic = new ArrayList<>(childWindow.size());
        float organicMax = Float.NEGATIVE_INFINITY;
        for (RetrieverCandidate c : childWindow) {
            if (pinnedKeys.contains(key(c.index(), c.id())) == false) {
                organic.add(c);
                organicMax = Math.max(organicMax, c.score());
            }
        }

        // Assign strictly-descending synthetic scores to the pins, all above the organic max, so ordering
        // by _score reproduces pin order exactly (no injected sort needed).
        List<RetrieverCandidate> scoredPins = assignPinnedScores(pinnedResolved, organicMax);

        // Concatenate: pins (top, in order) then organic (child order), assigning final positions.
        List<RetrieverCandidate> result = new ArrayList<>(scoredPins.size() + organic.size());
        int position = 0;
        for (RetrieverCandidate pin : scoredPins) {
            result.add(pin.withScoreAndPosition(pin.score(), position++));
        }
        for (RetrieverCandidate c : organic) {
            result.add(c.withScoreAndPosition(c.score(), position++));
        }
        this.resolvedResult = result;
    }

    /**
     * Assign strictly-descending synthetic scores to the pinned docs, all strictly above {@code organicMax},
     * so score order reproduces pin order. The gap between adjacent pins is auto-chosen relative to the
     * score magnitude so float precision cannot collapse two adjacent pins even for long pin lists.
     */
    static List<RetrieverCandidate> assignPinnedScores(List<RetrieverCandidate> pins, float organicMax) {
        int n = pins.size();
        if (n == 0) {
            return pins;
        }
        float base = Float.isFinite(organicMax) ? organicMax : 0f;
        // Gap large enough to stay outside float rounding at this magnitude; at least 1.0.
        float gap = Math.max(1.0f, Math.abs(base) * 1e-3f);
        List<RetrieverCandidate> scored = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            // Highest score for the first pin; strictly decreasing but still above base for the last pin.
            float score = base + (n - i) * gap;
            scored.add(pins.get(i).withScoreAndPosition(score, i));
        }
        return scored;
    }

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
        if (resolvedResult == null) {
            return null;
        }
        // Locate the document in this node's resolved output to learn its final rank and whether it was
        // pinned. A pinned doc is described as "pinned"; an organic doc delegates to the child explanation.
        int rank = -1;
        RetrieverCandidate found = null;
        for (int i = 0; i < resolvedResult.size(); i++) {
            RetrieverCandidate c = resolvedResult.get(i);
            if (c.index().equals(index) && c.id().equals(id)) {
                rank = i;
                found = c;
                break;
            }
        }
        if (found == null) {
            return null;
        }
        int pinCount = pinnedCount();
        Explanation childExplanation = retriever.buildExplanation(index, id);
        if (rank < pinCount) {
            // Pinned. Show the pin rank; nest the child explanation if the child also matched this doc.
            String desc = "pin: pinned to rank " + (rank + 1) + " [require_match=" + requireMatch + "]";
            if (childExplanation != null) {
                return Explanation.match(found.score(), desc, childExplanation);
            }
            return Explanation.match(found.score(), desc + " (not matched by child; injected)");
        }
        // Organic: pass the child explanation through unchanged (it reflects the real relevance).
        return childExplanation;
    }

    /** Number of pinned entries at the head of the resolved result (pins were placed first in doResolve). */
    private int pinnedCount() {
        if (resolvedResult == null) {
            return 0;
        }
        // Pins are exactly the leading entries whose (index,_id) is in the pin set; recompute from pins for
        // clarity rather than storing extra state.
        Set<String> pinKeys = new LinkedHashSet<>();
        for (PinnedDoc d : pinnedDocs) {
            pinKeys.add(d.index() != null ? key(d.index(), d.id()) : idOnlyKey(d.id()));
        }
        int count = 0;
        for (RetrieverCandidate c : resolvedResult) {
            if (pinKeys.contains(key(c.index(), c.id())) || pinKeys.contains(idOnlyKey(c.id()))) {
                count++;
            } else {
                break; // pins are contiguous at the head
            }
        }
        return count;
    }

    @Override
    public RetrieverProfile.Node buildProfile() {
        // pin reports its wall time and a breakdown that reconciles to it:
        // total = pin + orchestration_overhead + child total
        // - pin: this node's own reorder/scoring compute is negligible and folded into orchestration; we
        // surface the child-resolution wall so the numbers add up. Here the single child runs sequentially
        // (child resolve, then optional id sub-search, then reorder), so there is no parallel max.
        RetrieverProfile.Node childProfile = retriever.buildProfile();
        long childNanos = childProfile.getTotalTimeInNanos();
        Map<String, Long> breakdown = new LinkedHashMap<>();
        long orchestration = nodeElapsedNanos - childNanos;
        if (orchestration > 0) {
            breakdown.put("orchestration_overhead", orchestration);
        }
        return RetrieverProfile.inner(NAME, nodeElapsedNanos, breakdown, List.of(childProfile));
    }

    private static String key(String index, String id) {
        return index + "\u0000" + id;
    }

    private static String idOnlyKey(String id) {
        return "\u0000" + id;
    }

    // ---- XContent ----

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        boolean allHaveNoIndex = true;
        for (PinnedDoc d : pinnedDocs) {
            if (d.index() != null) {
                allHaveNoIndex = false;
                break;
            }
        }
        if (allHaveNoIndex) {
            builder.startArray(IDS_FIELD);
            for (PinnedDoc d : pinnedDocs) {
                builder.value(d.id());
            }
            builder.endArray();
        } else {
            builder.startArray(DOCS_FIELD);
            for (PinnedDoc d : pinnedDocs) {
                builder.startObject();
                builder.field(DOC_ID_FIELD, d.id());
                if (d.index() != null) {
                    builder.field(DOC_INDEX_FIELD, d.index());
                }
                builder.endObject();
            }
            builder.endArray();
        }
        if (requireMatch != DEFAULT_REQUIRE_MATCH) {
            builder.field(REQUIRE_MATCH_FIELD, requireMatch);
        }
        builder.field(RETRIEVER_FIELD);
        builder.startObject();
        retriever.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static PinRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        List<String> ids = null;
        List<PinnedDoc> docs = null;
        RetrieverBuilder child = null;
        Boolean requireMatch = null;

        String currentField = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentField = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (IDS_FIELD.equals(currentField)) {
                    ids = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        ids.add(parser.text());
                    }
                } else if (DOCS_FIELD.equals(currentField)) {
                    docs = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        docs.add(parsePinnedDoc(parser));
                    }
                } else {
                    throw new IllegalArgumentException("[" + NAME + "] unknown array field [" + currentField + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (RETRIEVER_FIELD.equals(currentField)) {
                    child = parseInnerRetrieverBuilder(parser);
                } else {
                    throw new IllegalArgumentException("[" + NAME + "] unknown object field [" + currentField + "]");
                }
            } else if (token.isValue()) {
                if (REQUIRE_MATCH_FIELD.equals(currentField)) {
                    requireMatch = parser.booleanValue();
                } else {
                    throw new IllegalArgumentException("[" + NAME + "] unknown field [" + currentField + "]");
                }
            }
        }

        if ((ids == null) == (docs == null)) {
            throw new IllegalArgumentException("[" + NAME + "] requires exactly one of [" + IDS_FIELD + "] or [" + DOCS_FIELD + "]");
        }
        List<PinnedDoc> pins = new ArrayList<>();
        if (ids != null) {
            for (String id : ids) {
                pins.add(new PinnedDoc(id, null));
            }
        } else {
            pins = docs;
        }
        PinRetrieverBuilder builder = new PinRetrieverBuilder(pins, child);
        if (requireMatch != null) {
            builder.setRequireMatch(requireMatch);
        }
        return builder;
    }

    private static PinnedDoc parsePinnedDoc(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("[" + NAME + "] each [" + DOCS_FIELD + "] entry must be an object with [_id]");
        }
        String id = null;
        String index = null;
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (token.isValue()) {
                if (DOC_ID_FIELD.equals(field)) {
                    id = parser.text();
                } else if (DOC_INDEX_FIELD.equals(field)) {
                    index = parser.text();
                } else {
                    throw new IllegalArgumentException("[" + NAME + "] unknown [" + DOCS_FIELD + "] entry field [" + field + "]");
                }
            }
        }
        if (id == null) {
            throw new IllegalArgumentException("[" + NAME + "] each [" + DOCS_FIELD + "] entry requires [_id]");
        }
        return new PinnedDoc(id, index);
    }
}
