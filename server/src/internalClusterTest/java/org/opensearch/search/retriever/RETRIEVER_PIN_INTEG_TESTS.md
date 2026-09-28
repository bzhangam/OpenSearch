# Retriever `pin` — Reference and Integration Test Results

The `pin` retriever forces a curated set of documents to the top of a single child
retriever's ranking, in the exact order given — the retriever-framework analogue of the
`pinned` query, composable over any child (`standard`, `rank_fusion`, `score_fusion`, …).

Source: `PinRetrieverBuilder.java`, `PinRetrieverBuilderTests.java` (unit),
`PinRetrieverIT.java` (integration). Shared corpus and helpers: `AbstractRetrieverIT.java`.

> **On the example outputs.** Response bodies below were **captured from a real run** of these
> exact requests against a local multi-shard cluster (OpenSearch `3.7.0-SNAPSHOT`,
> `DFS_QUERY_THEN_FETCH`) over the shared corpus. Field names, tree shape, description strings,
> ordering, and the score/timing relationships shown are the actual captured values. Only the
> per-run-variable bits vary between runs: absolute `total_time_in_nanos` / `breakdown` timings,
> node/shard UUIDs, and `pit_id`.

---

## Shared corpus

Index `products` (`number_of_replicas: 0`), six docs a–f:

| `_id` | `title`                      | `brand`  |
|-------|------------------------------|----------|
| a     | wireless headphones          | acme     |
| b     | bluetooth headphones         | acme     |
| c     | wired earbuds                | globex   |
| d     | noise cancelling headphones  | globex   |
| e     | usb cable                    | acme     |
| f     | headphones stand             | globex   |

`title:headphones` matches **a, b, d, f**. `brand:acme` matches **a, b, e**. `c` matches neither.

---

## Request syntax (UX)

**Pin by id (index-less pins target the searched index):**
```json
{
  "retriever": {
    "pin": {
      "ids": ["d", "a"],
      "retriever": { "standard": { "query": { "match": { "title": "headphones" } } } }
    }
  },
  "size": 10
}
```

**Pin by `{_id, _index}` (cross-index) with the match mode set explicitly:**
```json
{
  "retriever": {
    "pin": {
      "docs": [ { "_id": "d", "_index": "products" }, { "_id": "a", "_index": "products" } ],
      "require_match": true,
      "retriever": { "rank_fusion": { "retrievers": [ /* … */ ] } }
    }
  },
  "size": 10
}
```

**Fields:**

| Field | Type | Required | Default | Meaning |
|-------|------|----------|---------|---------|
| `ids` | string[] | one of `ids`/`docs` | — | pinned `_id`s in the searched index (index-less pins) |
| `docs` | object[] `{_id,_index}` | one of `ids`/`docs` | — | pinned docs for cross-index pinning |
| `retriever` | retriever | yes | — | the child whose ranking is reranked |
| `require_match` | boolean | no | `false` | `false` = always pin (inject even if the child didn't match); `true` = pin only docs the child matched |

**Semantics:**
- Pinned docs occupy the head of the result, **in array order**; the organic child results
  (minus any pinned dupes) follow.
- **Dedup:** a pinned doc that also matched organically appears once, at its pinned position.
- **Ordering by score, no injected sort.** Pinned docs get strictly-descending synthetic scores
  placed above the child's maximum organic score, so ordering by `_score` alone reproduces the
  pin order. The gap is auto-chosen for strict float separation; a configurable gap is
  intentionally not exposed in this version. (Rerankers whose display order is genuinely decoupled
  from score — e.g. MMR — would instead use the framework's `rank_docs_sort`; pinning does not.)
- **Always-pin routing.** Under `require_match:false`, a pinned doc the child did not match has no
  `(index, shardId)` to scope the final fetch, so the pin retriever resolves it with one bounded
  `ids` sub-search over the not-yet-located pins. A pin that resolves to no document (deleted /
  never existed) is silently skipped.
- **Top-level only.** `pin` is rejected inside a `rank_fusion`/`score_fusion` (a fusion governs its
  legs' window; injecting a pin into that contract is disallowed).
- **Aggregations / `track_total_hits`** are computed over what the child matched; pinned
  non-matches do not inflate the union count.

---

# Integration test scenarios (`PinRetrieverIT`)

All run on `createProducts(3)` (3 shards), `DFS_QUERY_THEN_FETCH`.

## PIN-IT-1 — pin by ids on a `standard` child

**Test:** `testPinByIdsOnStandard`
**Input:**
```json
{"retriever":{"pin":{"ids":["d","a"],
  "retriever":{"standard":{"query":{"match":{"title":"headphones"}}}}}},"size":10,"from":0}
```
**Output (captured — hit ids and scores):**
```
ids = [d, a, f, b]
d score = 2.2073584   (pinned #1)
a score = 1.2073584   (pinned #2)
f score = 0.20735832  (organic)
b score = 0.20735832  (organic)
```
`d`, `a` are pinned on top in order; the organic headphones matches `f`, `b` follow; `d`/`a`
are not duplicated. Pinned scores are strictly descending and strictly above the organic max.

**Asserts:** `d` first, `a` second; each pinned id appears exactly once; organic `b`, `f` present.

---

## PIN-IT-3 — dedup a pin that also matches organically

**Test:** `testPinDedupsFromOrganicTail`
**Input:** `pin ids ["a"]` on `match title:headphones` (a matches organically).
**Output:** `a` is first and appears **exactly once** (removed from the organic tail).

---

## PIN-IT-4 — a nonexistent pinned id is silently skipped

**Test:** `testPinMissingIdSkipped`
**Input:** `pin ids ["zzz","a"]` (default always-pin).
**Output:** `a` is first; `zzz` does not appear; 0 failed shards.

---

## PIN-IT-5 — pin on top of a `rank_fusion` child

**Test:** `testPinOnTopOfRankFusion`
**Input:** `pin ids ["f","e"]` on a two-leg `rank_fusion` (`headphones` + `brand:acme`).
**Output:** `f`, `e` on top in order; the fused organic order follows with `f`/`e` deduped.

---

## PIN-IT-6 — `explain`: pinned hits described as pinned; organic pass through

**Test:** `testPinExplain`
**Input:**
```json
{"retriever":{"pin":{"ids":["d"],
  "retriever":{"standard":{"query":{"match":{"title":"headphones"}}}}}},"explain":true,"size":10}
```
**Output — top hit `d` (captured):**
```json
{
  "_id": "d",
  "_score": 1.2073584,
  "_explanation": {
    "value": 1.2073584,
    "description": "pin: pinned to rank 1 [require_match=false]",
    "details": [
      {
        "value": 0.17352948,
        "description": "weight(title:headphones in 2) [PerFieldSimilarity], result of:",
        "details": [ "... real BM25 idf/tf subtree from the child ..." ]
      }
    ]
  }
}
```
A pinned doc is described as `pin: pinned to rank N [require_match=…]` and **nests the child's
real explanation** when the child also matched it (for an injected non-matching pin the node
instead reads `… (not matched by child; injected)`). Organic hits pass the child explanation
through unchanged. Every hit carries an `_explanation`.

---

## PIN-IT-7 — `profile`: a `pin` node that reconciles

**Test:** `testPinProfile`
**Input:**
```json
{"retriever":{"pin":{"ids":["d","a"],
  "retriever":{"standard":{"query":{"match":{"title":"headphones"}}}}}},"profile":true,"size":10}
```
**Output — profile skeleton (captured; one shard shown):**
```json
{
  "profile": {
    "self_resolve": {
      "total_time_in_nanos": 51942355,
      "breakdown": { "coordinator_overhead": 2077409 },
      "retriever": {
        "type": "pin",
        "total_time_in_nanos": 49864946,
        "breakdown": { "orchestration_overhead": 75798 },
        "children": [
          {
            "type": "standard",
            "total_time_in_nanos": 49789148,
            "shards": [
              { "id": "[dsEPRCfrSyq68SJIvsCjYw][products][1]",
                "searches": [ { "query": [ { "type": "TermQuery", "description": "title:headphones", "...": "..." } ] } ],
                "aggregations": [], "fetch": [ "..." ] }
            ]
          }
        ]
      }
    },
    "rank_docs_query": { "total_time_in_nanos": "...", "breakdown": { "coordinator_overhead": "..." }, "shards": [ "..." ] },
    "total_time_in_nanos": "..."
  }
}
```
The `pin` node wraps the `standard` child subtree and reconciles like any compound:
`self_resolve.total (51,942,355) = pin.total (49,864,946) + coordinator_overhead (2,077,409)`,
and `pin.total = orchestration_overhead (75,798) + child standard.total (49,789,148)`.

**Asserts:** `profile`, `self_resolve`, `type:pin`, child `type:standard`, and `rank_docs_query`
all present.

---

## PIN-IT-8 — `size` / `from` paging over the reordered window

**Test:** `testPinRespectsSizeAndFrom`
**Input:** `pin ids ["d","a","f"]`.
**Output:** `size:2, from:0` → `[d, a]` (first two pins only). `size:5, from:2` → `f` (third pin)
leads, then organic. Pins occupy the head; standard paging applies to the reordered window.

---

## PIN-IT-9 — `track_total_hits` reflects the child match count

**Test:** `testPinTrackTotalHits`
**Input:** `pin ids ["c"]` (c does not match headphones), `track_total_hits:true`.
**Output:** total hits = **4** (the headphones matches a,b,d,f); pinning `c` does not inflate it.
`c` is still pinned on top of the returned page.

---

## PIN-IT-11 — always-pin (default) injects a non-matching doc

**Test:** `testAlwaysPinInjectsNonMatchingDoc`
**Input:** `pin ids ["c"]` on `headphones` (default `require_match:false`).
**Output:** `c` (which does not match headphones) is injected at the top via the id sub-search;
organic headphones matches still present below.

---

## PIN-IT-12 — `require_match:true` drops a non-matching pin

**Test:** `testRequireMatchDropsNonMatchingPin`
**Input:** `pin ids ["c","a"], require_match:true`.
**Output:** `c` is dropped (child didn't match it); `a` (a real match) is pinned on top.

---

## PIN-IT-13' — pinned scores strictly above organic, strictly descending

**Test:** `testPinScoresStrictlyAboveOrganic`
**Input:** `pin ids ["d","a"]` on `headphones`.
**Output:** `score(d) > score(a)`, and both strictly greater than every organic hit's score —
so ordering by `_score` alone reproduces the pin order (no injected sort needed).

---

## PIN-IT-14 — `pin` rejected inside a `rank_fusion`

**Test:** `testPinRejectedInsideRankFusion`
**Input:** a `rank_fusion` whose first leg is a `pin`.
**Output:** request fails with `"[pin] retriever is only allowed at the top level, not inside a
[rank_fusion]/[score_fusion] retriever"` (raised during leaf preparation).

---

## PIN-IT-10 — validation: exactly one of `ids`/`docs`

**Test:** `testPinValidationRejectsMissingIdsAndDocs`
**Input:** a `pin` with neither `ids` nor `docs`.
**Output:** parse fails with `"[pin] requires exactly one of [ids] or [docs]"`.

---

# Unit tests (`PinRetrieverBuilderTests`)

Pin the exact arithmetic / parsing that the IT asserts structurally:

- **XContent:** parse `ids` form, parse `docs` form + `require_match`, `toXContent` for both forms,
  render-then-reparse round-trip.
- **Validation:** missing child, empty pins, both `ids`+`docs`, neither, and rejection inside a
  fusion-governed context.
- **`assignPinnedScores`:** strictly-descending, strictly-above-max scores (and the no-organic /
  `-inf` max fallback).
- **`require_match:true` reorder + dedup**, and dropping a non-matching pin.
- **`buildExplanation`** pinned-vs-organic shape, **`buildProfile`** node shape + reconciliation
  (`pin total == orchestration_overhead + child total`).

---

# Running these tests

From `/local/home/bzhangam/retriever-framework/OpenSearch`:

```bash
export JAVA_HOME=/local/apollo/package/local_1/AL2_x86_64/JDK21/JDK21-4711.0-0/jdk-21

# Unit
./gradlew :server:test --tests "org.opensearch.search.retriever.PinRetrieverBuilderTests"

# Integration (all pin scenarios)
./gradlew :server:internalClusterTest --tests "org.opensearch.search.retriever.PinRetrieverIT"

# A single scenario
./gradlew :server:internalClusterTest \
  --tests "org.opensearch.search.retriever.PinRetrieverIT.testAlwaysPinInjectsNonMatchingDoc"
```
