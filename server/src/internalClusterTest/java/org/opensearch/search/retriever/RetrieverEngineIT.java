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
import org.opensearch.common.settings.Settings;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * A3a integration test for the retriever engine skeleton on a real, multi-shard cluster.
 * <p>
 * A3a is <b>accept-don't-execute</b>: there is no executor yet (that is A3b). The one thing here that is
 * genuinely <b>cluster-level</b> (not parse-time) is that a {@code retriever} request survives the real
 * transport {@code _search} path end-to-end — the SearchModule-wired global parser dispatches the
 * {@code retriever} field on a live node and the request completes across shards without error.
 * <p>
 * The blocked-combo rejections are <b>parse-time</b> (they throw during {@code SearchSourceBuilder}
 * XContent parsing, entirely in-JVM on the coordinator with no cluster interaction), so they are covered
 * by {@link SearchSourceBuilderRetrieverIntegrationTests} as unit tests — standing up a cluster to assert
 * a parse exception would be a unit test in an IT costume. Result/parity ITs arrive with the executor
 * (A3b).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RetrieverEngineIT extends OpenSearchIntegTestCase {

    public void testTopLevelStandardRetrieverTraversesSearchPath() throws Exception {
        assertAcked(
            prepareCreate("products").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );
        for (String id : new String[] { "a", "b", "c", "d", "e", "f" }) {
            client().prepareIndex("products").setId(id).setSource("title", "doc " + id).get();
        }
        client().admin().indices().refresh(new RefreshRequest("products")).actionGet();

        // A top-level `standard` retriever (a valid entry point — NOT blocked) parses via the live node's
        // SearchModule-wired registry and the search traverses the real coordinator↔shard transport path.
        // A3a does not execute the retriever, so this completes as an ordinary multi-shard search.
        String body = "{\"retriever\":{\"standard\":{\"query\":{\"match_all\":{}}}},\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        assertNotNull("retriever parsed via the registry", source.retriever());
        assertEquals("standard", source.retriever().getName());

        SearchResponse response = client().prepareSearch("products").setSource(source).get();
        assertEquals(0, response.getFailedShards());
    }
}
