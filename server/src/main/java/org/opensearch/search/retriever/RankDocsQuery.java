/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Internal-only Lucene {@link Query} that matches a fixed set of documents by their _id field
 * values and assigns each a pre-computed score from the retriever framework.
 * <p>
 * This query creates a single {@link Weight} that performs sorted sequential seeks through
 * the _id term dictionary — NOT N separate TermQuery executions.
 * <p>
 * Not registered in the named query registry. Only the retriever framework constructs it.
 *
 * @opensearch.internal
 */
public final class RankDocsQuery extends Query {

    private final String[] docIds;
    private final float[] scores;
    private final BytesRef[] encodedIds;

    /**
     * Construct a RankDocsQuery from pre-computed document IDs and scores.
     *
     * @param docIds the _id string values to match (will be sorted internally for seeking efficiency)
     * @param scores parallel array of pre-computed scores
     */
    public RankDocsQuery(String[] docIds, float[] scores) {
        if (docIds.length != scores.length) {
            throw new IllegalArgumentException("docIds and scores arrays must have the same length");
        }
        // Sort both arrays by encoded _id bytes for sequential seeking
        Integer[] indices = new Integer[docIds.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        BytesRef[] encoded = new BytesRef[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            encoded[i] = Uid.encodeId(docIds[i]);
        }
        Arrays.sort(indices, Comparator.comparing(i -> encoded[i]));

        this.docIds = new String[docIds.length];
        this.scores = new float[docIds.length];
        this.encodedIds = new BytesRef[docIds.length];
        for (int i = 0; i < indices.length; i++) {
            this.docIds[i] = docIds[indices[i]];
            this.scores[i] = scores[indices[i]];
            this.encodedIds[i] = encoded[indices[i]];
        }
    }

    public String[] getDocIds() {
        return docIds;
    }

    public float[] getScores() {
        return scores;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                ScorerSupplier supplier = scorerSupplier(context);
                if (supplier != null) {
                    Scorer scorer = supplier.get(1L);
                    if (scorer.iterator().advance(doc) == doc) {
                        return Explanation.match(
                            scorer.score(),
                            "RankDocsQuery: pre-computed retriever score"
                        );
                    }
                }
                return Explanation.noMatch("RankDocsQuery: doc not in retriever results");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                Terms terms = context.reader().terms(IdFieldMapper.NAME);
                if (terms == null) {
                    return null;
                }
                TermsEnum termsEnum = terms.iterator();

                List<Integer> matchedLuceneDocIds = new ArrayList<>();
                List<Float> matchedScores = new ArrayList<>();

                for (int i = 0; i < encodedIds.length; i++) {
                    if (termsEnum.seekExact(encodedIds[i])) {
                        PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
                        int luceneDocId = postings.nextDoc();
                        if (luceneDocId != DocIdSetIterator.NO_MORE_DOCS) {
                            matchedLuceneDocIds.add(luceneDocId);
                            matchedScores.add(scores[i] * boost);
                        }
                    }
                }

                if (matchedLuceneDocIds.isEmpty()) {
                    return null;
                }

                // Sort by Lucene doc ID for correct iteration order
                Integer[] sortIdx = new Integer[matchedLuceneDocIds.size()];
                for (int i = 0; i < sortIdx.length; i++) {
                    sortIdx[i] = i;
                }
                Arrays.sort(sortIdx, Comparator.comparingInt(matchedLuceneDocIds::get));

                int[] sortedDocIds = new int[sortIdx.length];
                float[] sortedScores = new float[sortIdx.length];
                for (int i = 0; i < sortIdx.length; i++) {
                    sortedDocIds[i] = matchedLuceneDocIds.get(sortIdx[i]);
                    sortedScores[i] = matchedScores.get(sortIdx[i]);
                }

                Scorer scorer = new RankDocsScorer(sortedDocIds, sortedScores);
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) {
                        return scorer;
                    }

                    @Override
                    public long cost() {
                        return sortedDocIds.length;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "RankDocsQuery{docCount=" + docIds.length + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankDocsQuery that = (RankDocsQuery) o;
        return Arrays.equals(docIds, that.docIds) && Arrays.equals(scores, that.scores);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(classHash());
        result = 31 * result + Arrays.hashCode(docIds);
        result = 31 * result + Arrays.hashCode(scores);
        return result;
    }

    /**
     * Scorer that iterates matched documents in Lucene doc ID order with pre-computed scores.
     */
    private static class RankDocsScorer extends Scorer {
        private final int[] sortedLuceneDocIds;
        private final float[] docScores;
        private int currentIdx = -1;

        RankDocsScorer(int[] sortedLuceneDocIds, float[] docScores) {
            super();
            this.sortedLuceneDocIds = sortedLuceneDocIds;
            this.docScores = docScores;
        }

        @Override
        public float score() {
            return docScores[currentIdx];
        }

        @Override
        public int docID() {
            if (currentIdx < 0) return -1;
            if (currentIdx >= sortedLuceneDocIds.length) return DocIdSetIterator.NO_MORE_DOCS;
            return sortedLuceneDocIds[currentIdx];
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                @Override
                public int docID() {
                    return RankDocsScorer.this.docID();
                }

                @Override
                public int nextDoc() {
                    currentIdx++;
                    return docID();
                }

                @Override
                public int advance(int target) {
                    int idx = Arrays.binarySearch(sortedLuceneDocIds, currentIdx + 1, sortedLuceneDocIds.length, target);
                    currentIdx = idx >= 0 ? idx : -(idx + 1);
                    return docID();
                }

                @Override
                public long cost() {
                    return sortedLuceneDocIds.length;
                }
            };
        }

        @Override
        public float getMaxScore(int upTo) {
            // Upper bound on the score of any doc with docID <= upTo among the docs not yet consumed.
            // Scores are arbitrary pre-computed values (no monotonicity), so we take the max over the
            // remaining docs whose docID is within range. This is a valid (if loose) upper bound for
            // TOP_SCORES; it never under-estimates, so correctness of max-score skipping is preserved.
            float max = 0f;
            int start = Math.max(currentIdx, 0);
            for (int i = start; i < sortedLuceneDocIds.length; i++) {
                if (sortedLuceneDocIds[i] <= upTo) {
                    max = Math.max(max, docScores[i]);
                }
            }
            return max;
        }
    }
}
