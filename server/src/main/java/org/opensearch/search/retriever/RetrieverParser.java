/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Registry-based parser that dispatches to the correct {@link RetrieverBuilder} by type name.
 * <p>
 * This is the parsing entry point used by {@code SearchSourceBuilder} when it encounters
 * a {@code "retriever"} field in the search request. It reads the type name (the first field
 * key inside the retriever object), looks up the corresponding parser from the registry,
 * and delegates.
 * <p>
 * Example JSON structure:
 * <pre>
 * {
 *   "retriever": {
 *     "standard": {        ← type name looked up in registry
 *       "query": { ... }   ← passed to StandardRetrieverBuilder.fromXContent()
 *     }
 *   }
 * }
 * </pre>
 *
 * @opensearch.internal
 */
public class RetrieverParser {

    private final Map<String, Function<XContentParser, RetrieverBuilder>> parsers;

    public RetrieverParser(Map<String, Function<XContentParser, RetrieverBuilder>> parsers) {
        this.parsers = Collections.unmodifiableMap(new HashMap<>(parsers));
    }

    /**
     * Parse a retriever from the current XContent position.
     * The parser must be positioned at the START_OBJECT of the retriever field value.
     *
     * @param parser positioned at the START_OBJECT after "retriever":
     * @return the parsed RetrieverBuilder
     * @throws IOException on parsing errors
     */
    public RetrieverBuilder parse(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("[retriever] must be an object");
        }

        // Advance to the field name (the retriever type)
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("[retriever] must contain exactly one retriever type");
        }

        String typeName = parser.currentName();
        Function<XContentParser, RetrieverBuilder> parserFunc = parsers.get(typeName);
        if (parserFunc == null) {
            throw new IllegalArgumentException("unknown retriever type [" + typeName + "]");
        }

        // Advance past the field name to the START_OBJECT of the retriever body
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("[" + typeName + "] retriever must be an object");
        }

        RetrieverBuilder builder = parserFunc.apply(parser);

        // Advance past the outer END_OBJECT
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new IllegalArgumentException("[retriever] must contain exactly one retriever type, found trailing content");
        }

        return builder;
    }

    /**
     * Create a builder for constructing a RetrieverParser with registered types.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing a RetrieverParser.
     */
    public static class Builder {
        private final Map<String, Function<XContentParser, RetrieverBuilder>> parsers = new HashMap<>();

        public Builder register(String name, Function<XContentParser, RetrieverBuilder> parser) {
            Objects.requireNonNull(name, "retriever type name must not be null");
            Objects.requireNonNull(parser, "retriever parser must not be null");
            if (parsers.containsKey(name)) {
                throw new IllegalArgumentException("retriever type [" + name + "] is already registered");
            }
            parsers.put(name, parser);
            return this;
        }

        public RetrieverParser build() {
            return new RetrieverParser(parsers);
        }
    }

    /**
     * Checks if a retriever type is registered.
     */
    public boolean hasRetriever(String name) {
        return parsers.containsKey(name);
    }

    /**
     * Returns all registered retriever type names.
     */
    public Iterable<String> getRegisteredTypes() {
        return parsers.keySet();
    }
}
