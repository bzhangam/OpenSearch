/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Shared cluster/corpus setup and helpers for retriever integration tests. Both the execution/parity IT
 * ({@link RetrieverEngineIT}) and the PIT lifecycle IT ({@code RetrieverPitIT}) extend this so the common
 * corpus and request helpers are defined once (reuse), while each subclass keeps only its own
 * concern-specific tests (decouple).
 * <p>
 * <b>Shared corpus</b> (index {@code products}, {@code number_of_replicas: 0}): six docs a–f; four match
 * {@code title:headphones} — a, b, d, f.
 *
 * <pre>
 *   a | wireless headphones        | acme
 *   b | bluetooth headphones       | acme
 *   c | wired earbuds              | globex
 *   d | noise cancelling headphones| globex
 *   e | usb cable                  | acme
 *   f | headphones stand           | globex
 * </pre>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public abstract class AbstractRetrieverIT extends OpenSearchIntegTestCase {

    protected static final String INDEX = "products";

    /** Create the shared {@code products} corpus with the given shard count (0 replicas), then refresh. */
    protected void createProducts(int shards) {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shards).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("title", "type=text", "brand", "type=keyword")
        );
        index("a", "wireless headphones", "acme");
        index("b", "bluetooth headphones", "acme");
        index("c", "wired earbuds", "globex");
        index("d", "noise cancelling headphones", "globex");
        index("e", "usb cable", "acme");
        index("f", "headphones stand", "globex");
        refresh();
    }

    protected void index(String id, String title, String brand) {
        client().prepareIndex(INDEX).setId(id).setSource("title", title, "brand", brand).get();
    }

    /** Refresh the shared index so writes/deletes are visible to fresh readers. */
    protected void refresh() {
        client().admin().indices().refresh(new RefreshRequest(INDEX)).actionGet();
    }

    protected static List<String> ids(SearchResponse r) {
        List<String> ids = new ArrayList<>();
        for (SearchHit h : r.getHits().getHits()) {
            ids.add(h.getId());
        }
        return ids;
    }

    protected static Map<String, Float> scoreById(SearchResponse r) {
        Map<String, Float> m = new HashMap<>();
        for (SearchHit h : r.getHits().getHits()) {
            m.put(h.getId(), h.getScore());
        }
        return m;
    }

    /** Run a retriever {@code _search} with an explicit search type (so parity comparisons are deterministic). */
    protected SearchResponse retrieverSearch(String retrieverBody, int size, SearchType searchType) throws IOException {
        String body = "{\"retriever\":" + retrieverBody + ",\"size\":" + size + "}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        return client().prepareSearch(INDEX).setSearchType(searchType).setSource(source).get();
    }
}
