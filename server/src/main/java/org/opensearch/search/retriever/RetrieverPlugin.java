/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Extension point for plugins to register custom retriever types.
 * <p>
 * Plugins implement this interface and return their retriever specifications from
 * {@link #getRetrievers()}. The framework collects all specs at node startup and builds
 * the {@link RetrieverParser} registry.
 * <p>
 * Analogous to {@code SearchPipelinePlugin} — a one-time extension point that enables
 * all future retriever types without per-type core changes.
 * <p>
 * Example plugin:
 * <pre>
 * public class MyRetrieverPlugin extends Plugin implements RetrieverPlugin {
 *     &#64;Override
 *     public List&lt;RetrieverSpec&lt;?&gt;&gt; getRetrievers() {
 *         return List.of(
 *             new RetrieverSpec&lt;&gt;("my_fusion", MyFusionRetrieverBuilder::fromXContent)
 *         );
 *     }
 * }
 * </pre>
 *
 * @opensearch.api
 */
public interface RetrieverPlugin {

    /**
     * Returns the retriever type specifications provided by this plugin.
     */
    default List<RetrieverSpec<?>> getRetrievers() {
        return Collections.emptyList();
    }

    /**
     * A specification for a retriever type: name + parser function.
     *
     * @param <T> the specific RetrieverBuilder subclass
     */
    class RetrieverSpec<T extends RetrieverBuilder> {
        private final String name;
        private final Function<XContentParser, T> parser;

        /**
         * Create a retriever spec.
         *
         * @param name   the unique type name used in search requests (e.g., "standard", "rrf")
         * @param parser function that parses XContent into the retriever builder
         */
        public RetrieverSpec(String name, Function<XContentParser, T> parser) {
            this.name = Objects.requireNonNull(name, "retriever type name must not be null");
            this.parser = Objects.requireNonNull(parser, "retriever parser must not be null");
        }

        public String getName() {
            return name;
        }

        @SuppressWarnings("unchecked")
        public Function<XContentParser, RetrieverBuilder> getParser() {
            return (Function<XContentParser, RetrieverBuilder>) (Function<?, ?>) parser;
        }
    }
}
