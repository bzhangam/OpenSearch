/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Internal query + sort family that replays a coordinator-resolved retriever ranking window onto an
 * ordinary {@code _search} by seeking each document's {@code _id} on its own shard. Purely internal to
 * the retriever framework; not authorable in a {@code _search} body.
 */
package org.opensearch.search.retriever;
