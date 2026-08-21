/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.plugins.SearchPlugin;

import java.util.List;

/**
 * Helper that constructs the {@link RetrieverParser} from plugins at node startup.
 * <p>
 * This is called from {@code SearchModule} constructor:
 * <pre>
 * // In SearchModule constructor, after existing plugin registration:
 * this.retrieverParser = RetrieverModuleRegistration.buildRetrieverParser(plugins);
 * </pre>
 * <p>
 * The built-in {@code standard} retriever is always registered. Plugins add their own
 * types via the {@link RetrieverPlugin} SPI.
 *
 * @opensearch.internal
 */
public final class RetrieverModuleRegistration {

    private RetrieverModuleRegistration() {}

    /**
     * Build the RetrieverParser by collecting specs from all plugins that implement RetrieverPlugin.
     * Always registers the built-in "standard" retriever.
     * <p>
     * Takes {@code List<SearchPlugin>} — the type {@code SearchModule} actually holds — rather than
     * {@code List<Plugin>}. {@link RetrieverPlugin} is a standalone marker interface (like
     * {@code SearchPipelinePlugin}), not a {@code Plugin} subtype, and the only check this method
     * ever does is {@code instanceof RetrieverPlugin}; requiring {@code Plugin} bought nothing and
     * forced the call site to blindly cast every {@code SearchPlugin} to {@code Plugin} — which
     * fails for the many tests (e.g. {@code SearchModuleTests}) that register lightweight
     * {@code SearchPlugin}-only test doubles that don't also extend {@code Plugin}.
     *
     * @param plugins all loaded search plugins
     * @return the fully configured RetrieverParser
     */
    public static RetrieverParser buildRetrieverParser(List<? extends SearchPlugin> plugins) {
        RetrieverParser.Builder builder = RetrieverParser.builder();

        // Register built-in retriever types
        builder.register(StandardRetrieverBuilder.NAME, parser -> {
            try {
                return StandardRetrieverBuilder.fromXContent(parser);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse [standard] retriever", e);
            }
        });

        builder.register(RankFusionRetrieverBuilder.NAME, parser -> {
            try {
                return RankFusionRetrieverBuilder.fromXContent(parser);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse [rank_fusion] retriever", e);
            }
        });

        builder.register(ScoreFusionRetrieverBuilder.NAME, parser -> {
            try {
                return ScoreFusionRetrieverBuilder.fromXContent(parser);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse [score_fusion] retriever", e);
            }
        });

        builder.register(PinnedRetrieverBuilder.NAME, parser -> {
            try {
                return PinnedRetrieverBuilder.fromXContent(parser);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse [pinned] retriever", e);
            }
        });

        builder.register(RescoreRetrieverBuilder.NAME, parser -> {
            try {
                return RescoreRetrieverBuilder.fromXContent(parser);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse [rescore] retriever", e);
            }
        });

        // Register plugin-provided retriever types
        for (SearchPlugin plugin : plugins) {
            if (plugin instanceof RetrieverPlugin) {
                RetrieverPlugin retrieverPlugin = (RetrieverPlugin) plugin;
                for (RetrieverPlugin.RetrieverSpec<?> spec : retrieverPlugin.getRetrievers()) {
                    builder.register(spec.getName(), spec.getParser());
                }
            }
        }

        return builder.build();
    }
}
