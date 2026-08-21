/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Retriever framework for tree-structured, composable result-set orchestration.
 * <p>
 * A retriever is a composable stage that produces a ranked result set — by retrieving
 * candidates, combining multiple result sets, or reshaping an existing one. The tree
 * structure determines execution order. Retrievers are declared inline within a single
 * search request.
 *
 * @opensearch.internal
 */
package org.opensearch.search.retriever;
