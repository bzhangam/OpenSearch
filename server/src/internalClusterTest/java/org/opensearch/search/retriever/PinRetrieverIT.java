/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Integration tests for the {@code pin} retriever on a real multi-shard cluster. Pin forces a curated set
 * of documents to the top of a child retriever's ranking, in the exact order given. {@code require_match}
 * (default {@code false}) selects always-pin (inject non-matching docs) vs pin-only-when-matched.
 * <p>
 * Corpus (from {@link AbstractRetrieverIT}): a,b,d,f match {@code title:headphones}; a,b,e are
 * {@code brand:acme}; c is neither.
 */
public class PinRetrieverIT extends AbstractRetrieverIT {

    private static final String HEADPHONES = "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}";

    private static String pinByIds(List<String> ids, String childBody, Boolean requireMatch) {
        StringBuilder sb = new StringBuilder("{\"pin\":{\"ids\":[");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(ids.get(i)).append('"');
        }
        sb.append("]");
        if (requireMatch != null) {
            sb.append(",\"require_match\":").append(requireMatch);
        }
        sb.append(",\"retriever\":").append(childBody).append("}}");
        return sb.toString();
    }

    private SearchResponse search(String retrieverBody, int size, int from, boolean trackTotalHits, String aggs) throws IOException {
        String body = "{\"retriever\":"
            + retrieverBody
            + ",\"size\":"
            + size
            + ",\"from\":"
            + from
            + (trackTotalHits ? ",\"track_total_hits\":true" : "")
            + (aggs != null ? ",\"aggs\":" + aggs : "")
            + "}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        return client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(source).get();
    }

    private static String renderToJson(SearchResponse r) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        r.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    /** PIN-IT-1: pin by ids on a standard child — pinned docs on top, in order, organic follows. */
    public void testPinByIdsOnStandard() throws Exception {
        createProducts(3);
        // Pin d then a on top of a headphones search (which matches a,b,d,f).
        SearchResponse r = search(pinByIds(List.of("d", "a"), HEADPHONES, null), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals("d pinned first", "d", ids.get(0));
        assertEquals("a pinned second", "a", ids.get(1));
        // Organic headphones matches (b, f) follow; d and a are not duplicated.
        assertEquals("d appears once", 1, ids.stream().filter("d"::equals).count());
        assertEquals("a appears once", 1, ids.stream().filter("a"::equals).count());
        assertTrue("organic b present after pins", ids.contains("b"));
        assertTrue("organic f present after pins", ids.contains("f"));
    }

    /** PIN-IT-3: a pinned doc that also matches organically appears exactly once, at its pinned position. */
    public void testPinDedupsFromOrganicTail() throws Exception {
        createProducts(3);
        // 'a' matches headphones organically; pinning it must not duplicate it.
        SearchResponse r = search(pinByIds(List.of("a"), HEADPHONES, null), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals("a pinned first", "a", ids.get(0));
        assertEquals("a appears exactly once", 1, ids.stream().filter("a"::equals).count());
    }

    /** PIN-IT-4: a pinned id that does not exist is silently skipped (default always-pin). */
    public void testPinMissingIdSkipped() throws Exception {
        createProducts(3);
        SearchResponse r = search(pinByIds(List.of("zzz", "a"), HEADPHONES, null), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals("existing pin a is first", "a", ids.get(0));
        assertFalse("nonexistent id not present", ids.contains("zzz"));
    }

    /** PIN-IT-5: pin on top of a rank_fusion child — pins on top, fusion order (minus pins) follows. */
    public void testPinOnTopOfRankFusion() throws Exception {
        createProducts(3);
        String rankFusion = "{\"rank_fusion\":{\"retrievers\":["
            + "{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}},"
            + "{\"standard\":{\"query\":{\"term\":{\"brand\":\"acme\"}}}}]}}";
        SearchResponse r = search(pinByIds(List.of("f", "e"), rankFusion, null), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertEquals("f pinned first", "f", ids.get(0));
        assertEquals("e pinned second", "e", ids.get(1));
        assertEquals("f once", 1, ids.stream().filter("f"::equals).count());
        assertEquals("e once", 1, ids.stream().filter("e"::equals).count());
    }

    /** PIN-IT-6: explain — pinned hits described as pinned; organic hits pass through child explanation. */
    public void testPinExplain() throws Exception {
        createProducts(3);
        String body = "{\"retriever\":" + pinByIds(List.of("d"), HEADPHONES, null) + ",\"explain\":true,\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        String json = renderToJson(r);
        assertTrue("pinned explanation present", json.contains("pinned to rank 1"));
        // Every hit carries an _explanation, and the top hit is the pinned doc d.
        assertEquals("d", r.getHits().getHits()[0].getId());
        for (SearchHit h : r.getHits().getHits()) {
            assertNotNull("every hit has an explanation", h.getExplanation());
        }
    }

    /** PIN-IT-7: profile — a pin node with a child subtree, reconciling total = orchestration + child. */
    public void testPinProfile() throws Exception {
        createProducts(3);
        String body = "{\"retriever\":" + pinByIds(List.of("d", "a"), HEADPHONES, null) + ",\"profile\":true,\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        SearchResponse r = client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(source).get();
        assertEquals(0, r.getFailedShards());
        String json = renderToJson(r);
        assertTrue("profile present", json.contains("\"profile\":{"));
        assertTrue("self_resolve present", json.contains("\"self_resolve\":{"));
        assertTrue("pin node present", json.contains("\"type\":\"pin\""));
        assertTrue("child standard node present", json.contains("\"type\":\"standard\""));
        assertTrue("rank_docs_query present", json.contains("\"rank_docs_query\":{"));
    }

    /** PIN-IT-8: size/from paging over the reordered window (pins occupy the head). */
    public void testPinRespectsSizeAndFrom() throws Exception {
        createProducts(3);
        // Pin d,a,f. size=2 -> only the first two pins.
        SearchResponse first = search(pinByIds(List.of("d", "a", "f"), HEADPHONES, null), 2, 0, false, null);
        assertEquals(0, first.getFailedShards());
        assertEquals(List.of("d", "a"), ids(first));
        // from=2, size=5 -> skip the first two pins; pin #3 (f) then organic.
        SearchResponse page2 = search(pinByIds(List.of("d", "a", "f"), HEADPHONES, null), 5, 2, false, null);
        assertEquals(0, page2.getFailedShards());
        assertEquals("third pin f leads page 2", "f", ids(page2).get(0));
    }

    /** PIN-IT-9: track_total_hits reflects the child match count, not inflated by pinned non-matches. */
    public void testPinTrackTotalHits() throws Exception {
        createProducts(3);
        // Pin c (does NOT match headphones). Child headphones matches a,b,d,f = 4. Total must stay 4.
        SearchResponse r = search(pinByIds(List.of("c"), HEADPHONES, null), 10, 0, true, null);
        assertEquals(0, r.getFailedShards());
        assertEquals("union total is the child match count (4), pin c does not inflate it", 4L, r.getHits().getTotalHits().value());
        assertEquals("c is pinned on top", "c", ids(r).get(0));
    }

    /** PIN-IT-11: always-pin (default) injects a non-matching doc at the top. */
    public void testAlwaysPinInjectsNonMatchingDoc() throws Exception {
        createProducts(3);
        // c does not match headphones; default require_match:false must still pin it on top.
        SearchResponse r = search(pinByIds(List.of("c"), HEADPHONES, null), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        assertEquals("non-matching c injected at top", "c", ids(r).get(0));
        assertTrue("organic headphones matches still present", ids(r).contains("a"));
    }

    /** PIN-IT-12: require_match:true drops a non-matching pin. */
    public void testRequireMatchDropsNonMatchingPin() throws Exception {
        createProducts(3);
        SearchResponse r = search(pinByIds(List.of("c", "a"), HEADPHONES, true), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        List<String> ids = ids(r);
        assertFalse("non-matching c dropped under require_match:true", ids.contains("c"));
        assertEquals("matching pin a is on top", "a", ids.get(0));
    }

    /** PIN-IT-13': pinned scores are strictly above every organic score and strictly descending. */
    public void testPinScoresStrictlyAboveOrganic() throws Exception {
        createProducts(3);
        SearchResponse r = search(pinByIds(List.of("d", "a"), HEADPHONES, null), 10, 0, false, null);
        assertEquals(0, r.getFailedShards());
        SearchHit[] hits = r.getHits().getHits();
        float pin0 = hits[0].getScore();
        float pin1 = hits[1].getScore();
        assertTrue("pin0 strictly > pin1", pin0 > pin1);
        // Every organic hit (index >= 2) scores strictly below the last pin.
        for (int i = 2; i < hits.length; i++) {
            assertTrue("pin1 strictly above organic hit " + hits[i].getId(), pin1 > hits[i].getScore());
        }
    }

    /** PIN-IT-14: pin is rejected inside a rank_fusion (top-level only). */
    public void testPinRejectedInsideRankFusion() throws Exception {
        createProducts(3);
        String nested = "{\"rank_fusion\":{\"retrievers\":["
            + pinByIds(List.of("a"), HEADPHONES, null)
            + ",{\"standard\":{\"query\":{\"match\":{\"title\":\"headphones\"}}}}]}}";
        String body = "{\"retriever\":" + nested + ",\"size\":10}";
        SearchSourceBuilder source = SearchSourceBuilder.fromXContent(createParser(jsonXContent, body));
        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(source).get()
        );
        assertTrue(unwrapMessage(e), unwrapMessage(e).contains("only allowed at the top level"));
    }

    /** PIN-IT-10: validation — neither ids nor docs, or an empty child, is rejected. */
    public void testPinValidationRejectsMissingIdsAndDocs() throws Exception {
        createProducts(3);
        String body = "{\"retriever\":{\"pin\":{\"retriever\":" + HEADPHONES + "}},\"size\":10}";
        // The "exactly one of" error is raised during retriever parsing (SearchSourceBuilder.fromXContent),
        // before the search is dispatched.
        Exception e = expectThrows(Exception.class, () -> SearchSourceBuilder.fromXContent(createParser(jsonXContent, body)));
        assertTrue(unwrapMessage(e), unwrapMessage(e).contains("exactly one of"));
    }

    private static String unwrapMessage(Throwable t) {
        StringBuilder sb = new StringBuilder();
        while (t != null) {
            if (t.getMessage() != null) {
                sb.append(t.getMessage()).append(" | ");
            }
            t = t.getCause();
        }
        return sb.toString();
    }
}
