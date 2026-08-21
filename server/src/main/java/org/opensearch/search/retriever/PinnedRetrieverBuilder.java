/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transformer retriever that forces specific documents to the top of the result set.
 * Pinned docs appear first in the specified order; organic results follow with duplicates removed.
 * <p>
 * Missing pinned doc IDs are skipped gracefully (remaining docs shift up).
 *
 * @opensearch.internal
 */
public class PinnedRetrieverBuilder extends TransformerRetrieverBuilder {

    public static final String NAME = "pinned";

    private List<String> ids;

    public PinnedRetrieverBuilder() {}

    public PinnedRetrieverBuilder(List<String> ids, RetrieverBuilder child) {
        this.ids = ids;
        this.childRetriever = child;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    @Override
    public void validate(RetrieverContext context) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("[pinned] requires [ids] with at least one document ID");
        }
        super.validate(context);
    }

    @Override
    protected List<RankedDoc> reshape(List<RankedDoc> childResult) {
        Set<String> pinnedSet = new LinkedHashSet<>(ids);
        List<RankedDoc> results = new ArrayList<>();

        // Assign pinned docs scores above max organic to guarantee they sort first
        float maxOrganic = childResult.isEmpty() ? 1.0f : childResult.get(0).score();
        float pinnedBase = maxOrganic + ids.size() + 1;

        int position = 0;
        for (String pinnedId : ids) {
            // Find the pinned doc in child results (to get shard/index info)
            RankedDoc found = null;
            for (RankedDoc doc : childResult) {
                if (doc.id().equals(pinnedId)) {
                    found = doc;
                    break;
                }
            }
            if (found != null) {
                results.add(new RankedDoc(found.id(), found.index(), found.shardId(), pinnedBase - position, position));
                position++;
            }
            // If not found, skip gracefully (doc may not exist or not in result window)
        }

        // Add organic results, skip duplicates
        for (RankedDoc doc : childResult) {
            if (!pinnedSet.contains(doc.id())) {
                results.add(new RankedDoc(doc.id(), doc.index(), doc.shardId(), doc.score(), position));
                position++;
            }
        }

        return results;
    }

    @Override
    protected Explanation buildReshapeExplanation(String docId, String docIndex, Explanation childExplanation) {
        // Determine if this doc was pinned or organic
        int pinnedPosition = ids.indexOf(docId);
        if (pinnedPosition >= 0) {
            // Find the doc's organic rank in the child results
            int organicRank = -1;
            List<RankedDoc> childResult = childRetriever.getResolvedResult();
            if (childResult != null) {
                for (int i = 0; i < childResult.size(); i++) {
                    if (childResult.get(i).id().equals(docId) && childResult.get(i).index().equals(docIndex)) {
                        organicRank = i + 1;
                        break;
                    }
                }
            }
            String desc = organicRank > 0
                ? "pinned at position " + (pinnedPosition + 1) + " (specified order), organic rank=" + organicRank
                : "pinned at position " + (pinnedPosition + 1) + " (not in organic results)";
            return Explanation.match(
                resolvedResult != null && pinnedPosition < resolvedResult.size()
                    ? resolvedResult.get(pinnedPosition).score() : 0.0f,
                desc,
                childExplanation != null ? List.of(childExplanation) : List.of()
            );
        } else {
            // Organic doc — explain its position shift
            return Explanation.match(
                findDocScore(docId, docIndex),
                "organic (after " + countPinnedInResults() + " pinned docs removed from organic positions)",
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

    private int countPinnedInResults() {
        if (resolvedResult == null) return 0;
        int count = 0;
        Set<String> pinnedSet = new LinkedHashSet<>(ids);
        for (RankedDoc doc : resolvedResult) {
            if (pinnedSet.contains(doc.id())) count++;
        }
        return count;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("ids");
        for (String id : ids) {
            builder.value(id);
        }
        builder.endArray();
        builder.startObject("retriever");
        childRetriever.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Parse from XContent. Expected: { "ids": [...], "retriever": { "type": {...} } }
     * The child may be any registered retriever type — including nested compound/transformer
     * retrievers, not just "standard"/"rank_fusion"/"score_fusion" — dispatched via the shared registry.
     */
    public static PinnedRetrieverBuilder fromXContent(XContentParser parser) throws IOException {
        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder();

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                switch (fieldName) {
                    case "ids":
                        List<String> idsList = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            idsList.add(parser.text());
                        }
                        builder.setIds(idsList);
                        break;
                    case "retriever":
                        builder.setChildRetriever(RetrieverBuilder.parseInnerRetrieverBuilder(parser));
                        break;
                    default:
                        throw new IllegalArgumentException("[pinned] unknown field [" + fieldName + "]");
                }
            }
        }

        return builder;
    }
}
