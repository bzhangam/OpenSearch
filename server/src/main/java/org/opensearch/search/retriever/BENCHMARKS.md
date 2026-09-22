# Retriever Framework — Latency Benchmarks

Latency comparison of the `rank_fusion` **retriever** vs the classic **`hybrid` query** (neural-search
`score-ranker-processor` with the `rrf` technique). Both use Reciprocal Rank Fusion with the same
`rank_constant` (60), so they return identical rankings — this isolates the **execution-model** cost, not
the fusion algorithm.

## Execution models

- **Classic `hybrid` query**: sub-queries execute **sequentially within each shard's single search task**,
  then results are combined. One coordinator round. Shard-level cost ≈ `Σ(sub-query cost)`.
- **`rank_fusion` retriever**: each leg is dispatched as an **independent, concurrent sub-search**; the
  coordinator fuses in memory, then runs a final `RankDocsQuery` fetch. Two coordinator rounds (parallel
  legs overlap into one wall-time round + the final fetch). Wall-time ≈ `max(leg cost) + final fetch`.

The retriever trades **one extra round-trip** (final fetch) and **per-leg coordinator overhead** for
**parallel leg execution**. Whether that is a net win depends on the legs.

## Results

Single-node dev cluster. `hybrid` uses a `score-ranker-processor` (`rrf`, `rank_constant` 60);
`rank_fusion` uses `rank_constant` 60. Latency is `took` p50.

| Scenario | Corpus | Legs | classic hybrid | rank_fusion | Winner |
|---|---|---|---|---|---|
| Cheap legs | 20 docs, 3-dim | `knn` + `match` (both ~ms) | **2 ms** | 4 ms | hybrid |
| One dominant leg | 20k docs, 128-dim | `knn` k=500 (~3 ms) + expensive `script_score` (~199 ms) | **207 ms** | 213 ms | ~tie |
| Two balanced expensive legs | 20k docs, 128-dim | two `script_score` legs (~119 ms each) | 335 ms | **273 ms** | **rank_fusion** |

Reference: one `script_score` leg alone ≈ 119 ms.

## Interpretation

The crossover is governed by:

```
parallelism savings              extra fixed cost
(Σ leg_cost − max leg_cost)  vs  (final-fetch round + per-leg coordinator overhead)
```

- **Cheap legs** (row 1): `Σ ≈ max ≈ 0`, no parallelism to exploit, so the extra round-trip dominates and
  **hybrid wins** (2 ms vs 4 ms).
- **One dominant leg** (row 2): the cheap leg hides under the expensive one, so `max ≈ Σ`; both paths are
  dominated by the single 199 ms leg and are **~tied**.
- **Two balanced expensive legs** (row 3): hybrid pays `Σ` (119+119 ≈ 335 ms, confirming sequential
  per-shard sub-query execution); the retriever runs the legs concurrently and pays roughly `max` + the
  extra round-trip (≈ 273 ms), a **~18% win** despite the extra round-trip. The gap widens with more
  balanced expensive legs and more spare search-thread capacity.

**Takeaway:** prefer `rank_fusion` when a query has **two or more comparably-expensive legs** and the
cluster has spare search-thread capacity — parallel leg execution converts `Σ(leg costs)` into
`max(leg costs)`. Prefer the classic `hybrid` query for **cheap legs** or a **single dominant leg**, where
the retriever's extra round-trip has nothing to hide behind. The retriever additionally offers arbitrary
composition (nested fusion, per-leg sort/filter, cross-index legs) that a single `hybrid` query cannot
express.

## Optimizations applied

- **Leg fetch trimming**: leg sub-searches run with `_source` disabled (`fetchSource(false)`) — a leg only
  needs `(index, shardId, _id, score)` to compute ranks; the payload is loaded once by the final fetch.
  Savings scale with `document size × rank_window_size × num_legs` (negligible on tiny docs). Stored fields
  are kept because the leg still needs `_id`.

## Leg candidate depth

The retriever sets each leg's `size` to `rank_window_size` (the fused window depth). A query's own internal
candidate cap — the `knn` query's `k`, or `min_score` / `max_distance` thresholds — is **not** modified by
the retriever; it is the user's responsibility. For a `knn`/`neural` leg, set `k` (or a threshold)
appropriately for the desired depth, typically `k >= rank_window_size`, so the leg can supply the full
window. A leg that returns fewer than `rank_window_size` candidates (small `k`, a selective filter, or a
small index) simply contributes fewer docs to fusion — which is valid, so the retriever does not enforce it.

## Future optimization

- **`_msearch` batching** of legs would cut per-leg coordinator overhead (one multi-search request instead
  of N), lowering the fixed cost and pushing the crossover toward cheaper legs.

## Reproduction

Requires a local cluster with core + k-NN + neural-search (see the `retriever-local-cluster` skill). Create
an `rrf` pipeline, index a corpus, and compare:
```
PUT /_search/pipeline/rrf-pipeline {"phase_results_processors":[{"score-ranker-processor":{"combination":{"technique":"rrf","rank_constant":60}}}]}
# hybrid:     POST /idx/_search?search_pipeline=rrf-pipeline {"query":{"hybrid":{"queries":[<legA>,<legB>]}},"size":10}
# rank_fusion: POST /idx/_search {"retriever":{"rank_fusion":{"retrievers":[{"standard":{"query":<legA>}},{"standard":{"query":<legB>}}],"rank_window_size":200,"rank_constant":60}},"size":10}
```
Use two comparably-expensive `script_score` legs to observe the crossover.
