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
 * this.retrieverParser = RetrieverModuleRegistration.buildRetrieverParser(plugins);
 * </pre>
 * <p>
 * The built-in {@code standard} retriever is always registered. Additional core compound/transformer
 * types are added here (append-only) as they land; plugins add their own types via the
 * {@link RetrieverPlugin} SPI.
 *
 * @opensearch.internal
 */
public final class RetrieverModuleRegistration {

    private RetrieverModuleRegistration() {}

    /**
     * Build the RetrieverParser by collecting specs from all plugins that implement RetrieverPlugin.
     * Always registers the built-in {@code standard} retriever.
     * <p>
     * Takes {@code List<? extends SearchPlugin>} — the type {@code SearchModule} actually holds — rather
     * than {@code List<Plugin>}. {@link RetrieverPlugin} is a standalone marker interface (like
     * {@code SearchPipelinePlugin}), not a {@code Plugin} subtype, and the only check this method ever
     * does is {@code instanceof RetrieverPlugin}; requiring {@code Plugin} bought nothing and forced the
     * call site to blindly cast every {@code SearchPlugin} to {@code Plugin} — which fails for the many
     * tests (e.g. {@code SearchModuleTests}) that register lightweight {@code SearchPlugin}-only test
     * doubles that don't also extend {@code Plugin}.
     *
     * @param plugins all loaded search plugins
     * @return the fully configured RetrieverParser
     */
    public static RetrieverParser buildRetrieverParser(List<? extends SearchPlugin> plugins) {
        RetrieverParser.Builder builder = RetrieverParser.builder();

        // Built-in retriever types. Only `standard` exists today; core compound/transformer types
        // (rank_fusion, score_fusion, pinned, rescore, ...) are appended here as each one lands.
        builder.register(StandardRetrieverBuilder.NAME, parser -> {
            try {
                return StandardRetrieverBuilder.fromXContent(parser);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse [" + StandardRetrieverBuilder.NAME + "] retriever", e);
            }
        });

        // Plugin-provided retriever types (RetrieverPlugin SPI).
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
