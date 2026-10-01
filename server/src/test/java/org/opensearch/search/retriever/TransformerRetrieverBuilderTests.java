/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the {@link TransformerRetrieverBuilder} base contract that is independent of any concrete
 * reranker — specifically the {@link TransformerRetrieverBuilder#prepareLeaves(LeafPreparationContext)}
 * fusion-placement rule and the {@link TransformerRetrieverBuilder#preservesFusionWindow()} opt-in that a
 * window-preserving reranker (e.g. {@code diversify}) uses to run per-leg inside a fusion subtree.
 * <p>
 * Two minimal concrete subclasses are defined locally: {@link TopLevelOnlyTransformer} (default behavior,
 * rejects under fusion) and {@link WindowPreservingTransformer} (opts in). They implement only the abstract
 * reshape/explain/XContent hooks; all window/placement plumbing under test lives in the base.
 */
public class TransformerRetrieverBuilderTests extends OpenSearchTestCase {

    /** A reranker that keeps the base default: top-level only. */
    private static class TopLevelOnlyTransformer extends TransformerRetrieverBuilder {
        TopLevelOnlyTransformer(RetrieverBuilder child) {
            super(child);
        }

        @Override
        protected List<RetrieverCandidate> reshape(List<RetrieverCandidate> childWindow) {
            return childWindow;
        }

        @Override
        public String getName() {
            return "top_level_only_test";
        }

        @Override
        public Explanation buildExplanation(String index, String id) {
            return Explanation.match(1.0f, "test");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    /** A reranker that preserves the candidate count, so it may run under fusion. */
    private static class WindowPreservingTransformer extends TopLevelOnlyTransformer {
        WindowPreservingTransformer(RetrieverBuilder child) {
            super(child);
        }

        @Override
        protected boolean preservesFusionWindow() {
            return true;
        }

        @Override
        public String getName() {
            return "window_preserving_test";
        }
    }

    /**
     * A trivial child retriever that records the {@link LeafPreparationContext} it was handed, so the test can
     * assert exactly what the base propagated downward.
     */
    private static class RecordingChild extends RetrieverBuilder {
        final AtomicReference<LeafPreparationContext> received = new AtomicReference<>();

        @Override
        public List<StandardRetrieverBuilder> collectLeaves() {
            return List.of();
        }

        @Override
        public List<RetrieverBuilder> getChildRetrievers() {
            return List.of();
        }

        @Override
        public void validate() {}

        @Override
        public void prepareLeaves(LeafPreparationContext context) {
            received.set(context);
        }

        @Override
        public String getName() {
            return "recording_child_test";
        }

        @Override
        void resolve(
            org.opensearch.transport.client.Client client,
            String[] indices,
            org.opensearch.action.search.SearchRequest original,
            org.opensearch.core.action.ActionListener<Void> whenDone
        ) {
            whenDone.onResponse(null);
        }

        @Override
        void doResolve() {}

        @Override
        public org.opensearch.index.query.QueryBuilder toQueryBuilder() {
            return new MatchAllQueryBuilder();
        }

        @Override
        public Explanation buildExplanation(String index, String id) {
            return Explanation.match(1.0f, "child");
        }

        @Override
        public RetrieverProfile.Node buildProfile() {
            return null; // not exercised by prepareLeaves tests
        }

        @Override
        public org.opensearch.index.query.QueryBuilder extractAggregationQuery() {
            return new MatchAllQueryBuilder();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    // ---- Default (top-level-only) behavior ----

    public void testDefaultRejectsInsideFusion() {
        RecordingChild child = new RecordingChild();
        TopLevelOnlyTransformer transformer = new TopLevelOnlyTransformer(child);
        LeafPreparationContext fusionCtx = LeafPreparationContext.root(false, false).underFusion(50);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> transformer.prepareLeaves(fusionCtx));
        assertTrue(e.getMessage(), e.getMessage().contains("only allowed at the top level"));
        assertTrue(e.getMessage(), e.getMessage().contains("top_level_only_test"));
        // Child must not have been prepared when the node is rejected.
        assertNull(child.received.get());
    }

    public void testDefaultTopLevelPropagatesRootFlagsAndNoWindow() {
        RecordingChild child = new RecordingChild();
        TopLevelOnlyTransformer transformer = new TopLevelOnlyTransformer(child);
        // explain=true, profile=false at the root; no fusion ancestor.
        transformer.prepareLeaves(LeafPreparationContext.root(true, false));

        assertEquals(LeafPreparationContext.NO_WINDOW, transformer.effectiveWindow);
        LeafPreparationContext childCtx = child.received.get();
        assertNotNull(childCtx);
        assertFalse("child must not be fusion-governed at top level", childCtx.isFusionGoverned());
        assertEquals(LeafPreparationContext.NO_WINDOW, childCtx.getInheritedWindow());
        assertTrue("explain flag propagates", childCtx.isExplain());
        assertFalse("profile flag propagates", childCtx.isProfile());
    }

    // ---- Window-preserving opt-in behavior ----

    public void testPreservingAllowedInsideFusionCapturesWindow() {
        RecordingChild child = new RecordingChild();
        WindowPreservingTransformer transformer = new WindowPreservingTransformer(child);
        transformer.prepareLeaves(LeafPreparationContext.root(false, true).underFusion(50));

        // The node's output size is pinned to the inherited fusion window.
        assertEquals(50, transformer.effectiveWindow);
    }

    public void testPreservingPropagatesFusionContextUnchangedToChild() {
        RecordingChild child = new RecordingChild();
        WindowPreservingTransformer transformer = new WindowPreservingTransformer(child);
        transformer.prepareLeaves(LeafPreparationContext.root(true, true).underFusion(123));

        LeafPreparationContext childCtx = child.received.get();
        assertNotNull(childCtx);
        // Child leg must stay fusion-governed with the SAME window, so it still fetches rank_window_size.
        assertTrue("child stays fusion-governed", childCtx.isFusionGoverned());
        assertEquals("child inherits the same window", 123, childCtx.getInheritedWindow());
        assertTrue("explain propagates", childCtx.isExplain());
        assertTrue("profile propagates", childCtx.isProfile());
    }

    public void testPreservingTopLevelBehavesLikeDefault() {
        RecordingChild child = new RecordingChild();
        WindowPreservingTransformer transformer = new WindowPreservingTransformer(child);
        // Not fusion-governed: even a window-preserving reranker behaves normally at the top level.
        transformer.prepareLeaves(LeafPreparationContext.root(false, false));

        assertEquals(LeafPreparationContext.NO_WINDOW, transformer.effectiveWindow);
        LeafPreparationContext childCtx = child.received.get();
        assertNotNull(childCtx);
        assertFalse(childCtx.isFusionGoverned());
        assertEquals(LeafPreparationContext.NO_WINDOW, childCtx.getInheritedWindow());
    }
}
