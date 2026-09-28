# Retriever `explain` and `profile` — Integration Test Reference

This document catalogs the integration tests that exercise `explain: true` and
`profile: true` against the retriever framework (`standard`, `rank_fusion`,
`score_fusion`) on a real multi-shard cluster. For each scenario it gives the
purpose, the exact `_search` request (input), the captured response (output),
and what the test asserts.

Source tests:
- `RetrieverExplainIT.java` — 6 scenarios (EX-IT-1 … EX-IT-6)
- `RetrieverProfileIT.java` — 5 scenarios (PR-IT-1 … PR-IT-5)
- `AbstractRetrieverIT.java` — shared corpus and request helpers

> **On the example outputs.** The tests assert on *structure* and a few *invariants*
> (e.g. root explanation value == hit `_score`; present-leg contributions sum to the
> root; specific description substrings). They do **not** pin exact BM25/RRF/score-fusion
> floats or timings. The response bodies below were **captured from a real run** of these
> exact requests against a local single-node cluster (OpenSearch `3.7.0-SNAPSHOT`,
> `search_type` as noted per scenario) over the shared corpus, using this workspace's
> retriever build. Field names, tree shape, description strings, and the BM25/RRF/score-fusion
> values shown are the actual captured values. Only the per-run-variable bits vary between runs:
> `total_time_in_nanos` and per-`breakdown` timings, node/shard UUIDs in `id`, the internal
> `pit_id`, and the relative order of ties (several docs share an identical score). Exact
> arithmetic is pinned in the unit tests (`RetrieverExplainTests`, `RetrieverProfileTests`,
> and the per-type builder tests).
>
> **Capture setup.** Single node, so leg/`rank_docs_query` shard profiles show one node UUID;
> `createProducts(3)` still yields 3 primary shards. The corpus is created with 3 shards / 0
> replicas and the six docs a–f below. BM25 term stats for `title:headphones` on this corpus:
> `n=4` (docs containing the term), `N=6` (docs with the field), `avgdl≈2.1667` — these drive
> the `idf`/`tf` leaves shown in EX-IT-1/EX-IT-2.

---

## Shared corpus

All scenarios call `createProducts(shards)` from `AbstractRetrieverIT`, which creates
the index `products` (`number_of_replicas: 0`) with mapping `title: text`,
`brand: keyword`, indexes six documents, and refreshes:

| `_id` | `title`                      | `brand`  |
|-------|------------------------------|----------|
| a     | wireless headphones          | acme     |
| b     | bluetooth headphones         | acme     |
| c     | wired earbuds                | globex   |
| d     | noise cancelling headphones  | globex   |
| e     | usb cable                    | acme     |
| f     | headphones stand             | globex   |

Docs matching `title:headphones` → **a, b, d, f**. Docs matching `brand:acme` → **a, b, e**.
So under the two-leg fusion `[match title:headphones] + [term brand:acme]`:
- **a, b** are in *both* legs
- **d, f** are in the `title` leg only
- **e** is in the `brand` leg only

The shard count (`createProducts(3)` in most tests) and, in EX-IT-5,
`ensureAtLeastNumDataNodes(3)`, put the assertions on a genuine multi-shard /
multi-node topology.

Two retriever bodies are reused across scenarios:

**`RANK_FUSION_TWO_LEG`**
```json
{"rank_fusion":{"retrievers":[
  {"standard":{"query":{"match":{"title":"headphones"}}}},
  {"standard":{"query":{"term":{"brand":"acme"}}}}
]}}
```

**`SCORE_FUSION_TWO_LEG`**
```json
{"score_fusion":{"retrievers":[
  {"standard":{"query":{"match":{"title":"headphones"}}}},
  {"standard":{"query":{"term":{"brand":"acme"}}}}],
  "normalization":{"technique":"min_max"},
  "combination":{"technique":"arithmetic_mean"}}}
```

---

# Part 1 — `explain: true` (`RetrieverExplainIT`)

When `explain: true` is set, each hit's `_explanation` is the coordinator-assembled
tree. A `standard` leg passes the Lucene BM25 explanation through unchanged; a compound
node (`rank_fusion` / `score_fusion`) describes its fusion at the root and nests one
detail per leg, each of which in turn nests the leaf explanation. The root explanation
value always equals the hit's `_score`.

Request template (helper `explainSearch`):
```json
{"retriever": <retrieverBody>, "explain": true, "size": <size>}
```

---

## EX-IT-1 — `standard` leg passes through the Lucene BM25 tree

**Test:** `testStandardExplainPassesThroughLuceneTree`
**Purpose:** A bare `standard` retriever must surface the real Lucene BM25 explanation,
and its value must equal the hit score.

**Input** (`size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"standard": {"query": {"match": {"title": "headphones"}}}},
  "explain": true,
  "size": 10
}
```

**Output** (hit `a`; captured — hits `a`, `f`, `b` tie at `0.20735832`, `d` at `0.17352948`):
```json
{
  "_id": "a",
  "_score": 0.20735832,
  "_explanation": {
    "value": 0.20735832,
    "description": "weight(title:headphones in 0) [PerFieldSimilarity], result of:",
    "details": [
      {
        "value": 0.20735832,
        "description": "score(freq=1.0), computed as boost * idf * tf from:",
        "details": [
          {
            "value": 0.44183275,
            "description": "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
            "details": [
              { "value": 4, "description": "n, number of documents containing term", "details": [] },
              { "value": 6, "description": "N, total number of documents with field", "details": [] }
            ]
          },
          {
            "value": 0.46931404,
            "description": "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
            "details": [
              { "value": 1.0, "description": "freq, occurrences of term within document", "details": [] },
              { "value": 1.2, "description": "k1, term saturation parameter", "details": [] },
              { "value": 0.75, "description": "b, length normalization parameter", "details": [] },
              { "value": 2.0, "description": "dl, length of field", "details": [] },
              { "value": 2.1666667, "description": "avgdl, average length of field", "details": [] }
            ]
          }
        ]
      }
    ]
  }
}
```

**Asserts:** 0 failed shards; ≥1 hit; every hit has an `_explanation`; root
`value == _score` (±1e-4); description contains `weight(title:headphones`.

---

## EX-IT-2 — `rank_fusion` explain shows per-leg RRF contributions; absent legs marked

**Test:** `testRankFusionExplainShowsPerLegContributions`
**Purpose:** The fusion root lists one detail per leg; present-leg contributions sum to
the root; a leg the doc is missing from is rendered as a no-match "not present" node.

**Input** (`RANK_FUSION_TWO_LEG`, `size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"rank_fusion": {"retrievers": [
    {"standard": {"query": {"match": {"title": "headphones"}}}},
    {"standard": {"query": {"term":  {"brand": "acme"}}}}
  ]}},
  "explain": true,
  "size": 10
}
```

**Output — doc `a` (in both legs)** (captured; `rank_constant=60`, `_score=0.032786883`):
```json
{
  "_id": "a",
  "_score": 0.032786883,
  "_explanation": {
    "value": 0.032786883,
    "description": "rank_fusion [rank_constant=60]",
    "details": [
      {
        "value": 0.016393442,
        "description": "leg 0: 1/(60+1) = 0.01639344262295082 [rank 1]",
        "details": [
          {
            "value": 0.20735832,
            "description": "weight(title:headphones in 0) [PerFieldSimilarity], result of:",
            "details": [ "... BM25 idf/tf subtree (see EX-IT-1) ..." ]
          }
        ]
      },
      {
        "value": 0.016393442,
        "description": "leg 1: 1/(60+1) = 0.01639344262295082 [rank 1]",
        "details": [
          { "value": 1.0, "description": "ConstantScore(brand:acme)", "details": [] }
        ]
      }
    ]
  }
}
```

**Output — doc `d` (title leg only)** (captured; `_score=0.015625`):
```json
{
  "_id": "d",
  "_score": 0.015625,
  "_explanation": {
    "value": 0.015625,
    "description": "rank_fusion [rank_constant=60]",
    "details": [
      {
        "value": 0.015625,
        "description": "leg 0: 1/(60+4) = 0.015625 [rank 4]",
        "details": [
          {
            "value": 0.17352948,
            "description": "weight(title:headphones in 2) [PerFieldSimilarity], result of:",
            "details": [ "... BM25 idf/tf subtree ..." ]
          }
        ]
      },
      { "value": 0.0, "description": "leg 1: not present", "details": [] }
    ]
  }
}
```

**Asserts:** doc `a` root description contains `rank_fusion`; root `value == _score`;
exactly 2 leg details; both legs present for `a` and their values sum to the root
(±1e-4). Doc `d` has exactly one present leg and the other detail description contains
`not present`.

---

## EX-IT-3 — `score_fusion` explain shows normalization + per-leg weight

**Test:** `testScoreFusionExplainShowsNormalizationDetail`
**Purpose:** The `score_fusion` root names the normalization + combination techniques,
and each present leg detail carries its normalized score and weight.

**Input** (`SCORE_FUSION_TWO_LEG`, `size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"score_fusion": {"retrievers": [
    {"standard": {"query": {"match": {"title": "headphones"}}}},
    {"standard": {"query": {"term":  {"brand": "acme"}}}}],
    "normalization": {"technique": "min_max"},
    "combination":   {"technique": "arithmetic_mean"}}},
  "explain": true,
  "size": 10
}
```

**Output — doc `a`** (captured; `_score=1.0`):
```json
{
  "_id": "a",
  "_score": 1.0,
  "_explanation": {
    "value": 1.0,
    "description": "score_fusion(min_max, arithmetic_mean) [normalized by total leg weight]",
    "details": [
      {
        "value": 1.0,
        "description": "leg 0: norm=1.0 (raw=0.20735832, min=0.17352948, max=0.20735832), weight=1.0",
        "details": [
          {
            "value": 0.20735832,
            "description": "weight(title:headphones in 0) [PerFieldSimilarity], result of:",
            "details": [ "... BM25 idf/tf subtree ..." ]
          }
        ]
      },
      {
        "value": 1.0,
        "description": "leg 1: norm=1.0 (raw=1.0, min=1.0, max=1.0), weight=1.0",
        "details": [
          { "value": 1.0, "description": "ConstantScore(brand:acme)", "details": [] }
        ]
      }
    ]
  }
}
```

**Asserts:** root description contains `score_fusion`, `min_max`, and `arithmetic_mean`;
root `value == _score`; 2 leg details; at least one present leg description contains both
`norm=` and `weight=`.

---

## EX-IT-4 — nested fusion explains bottom-up (depth ≥ 3)

**Test:** `testNestedFusionExplainRecurses`
**Purpose:** A `rank_fusion` whose first leg is itself a `rank_fusion` must nest an inner
`rank_fusion` node inside the outer leg detail.

**Input** (`size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"rank_fusion": {"retrievers": [
    {"rank_fusion": {"retrievers": [
      {"standard": {"query": {"match": {"title": "headphones"}}}},
      {"standard": {"query": {"term":  {"brand": "acme"}}}}
    ]}},
    {"standard": {"query": {"match": {"title": "earbuds"}}}}
  ]}},
  "explain": true,
  "size": 10
}
```

**Output — doc `a` (reachable via the inner fusion)** (captured; `_score=0.016393442`, leaves collapsed):
```json
{
  "_id": "a",
  "_score": 0.016393442,
  "_explanation": {
    "value": 0.016393442,
    "description": "rank_fusion [rank_constant=60]",
    "details": [
      {
        "value": 0.016393442,
        "description": "leg 0: 1/(60+1) = 0.01639344262295082 [rank 1]",
        "details": [
          {
            "value": 0.032786883,
            "description": "rank_fusion [rank_constant=60]",
            "details": [
              {
                "value": 0.016393442,
                "description": "leg 0: 1/(60+1) = 0.01639344262295082 [rank 1]",
                "details": [ { "description": "weight(title:headphones in 0) [PerFieldSimilarity], result of:" } ]
              },
              {
                "value": 0.016393442,
                "description": "leg 1: 1/(60+1) = 0.01639344262295082 [rank 1]",
                "details": [ { "description": "ConstantScore(brand:acme)" } ]
              }
            ]
          }
        ]
      },
      { "value": 0.0, "description": "leg 1: not present", "details": [] }
    ]
  }
}
```

**Asserts:** doc `a` root description contains `rank_fusion`; walking outer-leg →
its nested child finds at least one node whose description contains `rank_fusion`
(the inner fusion).

---

## EX-IT-5 — explain is stable across a multi-node, multi-shard cluster

**Test:** `testExplainStableMultiNode`
**Purpose:** With ≥3 data nodes, coordinator-assembled explanations remain correct
(root value == score) for every hit.

**Setup:** `ensureAtLeastNumDataNodes(3)` then `createProducts(3)`.

**Input:** `RANK_FUSION_TWO_LEG`, `explain:true`, `size:10`, `DFS_QUERY_THEN_FETCH`
(same body as EX-IT-2).

**Output:** same `rank_fusion` explanation tree as EX-IT-2 (root `value == _score`,
two leg details, present legs summing to the root). The capture in this document was taken
on a **single node** (`./gradlew run` / the local distro), so the multi-node topology of
this specific test isn't reproduced here; the coordinator-assembled tree is identical in
shape and the invariant holds regardless of node count.

**Asserts:** 0 failed shards; ≥1 hit; every hit has an `_explanation` with
`value == _score` (±1e-4).

---

## EX-IT-6 — `explain: true` is honored (regression gate)

**Test:** `testExplainNoLongerIgnored`
**Purpose:** Regression gate — under `QUERY_THEN_FETCH`, `explain:true` must produce an
`_explanation` on **every** hit (it was previously dropped).

**Input:** `RANK_FUSION_TWO_LEG`, `explain:true`, `size:10`, **`QUERY_THEN_FETCH`**.

**Output:** every hit carries a non-null `_explanation` (same `rank_fusion [rank_constant=60]`
tree as EX-IT-2). Captured under `QUERY_THEN_FETCH`, hits ordered `a, b, e, d, f` with
scores `0.032266, 0.032266, 0.016129, 0.016129, 0.015625` — scores differ slightly from
the DFS run (EX-IT-2) because non-DFS uses per-shard term stats.

**Asserts:** 0 failed shards; ≥1 hit; `h.getExplanation()` is non-null for every hit.

---

# Part 2 — `profile: true` (`RetrieverProfile`)

When `profile: true` is set, the response `profile` section is the retriever profile
rendered by `RetrieverProfile`. It mirrors the **two sequential coordinator phases** of a
retriever request:

- **`self_resolve`** — resolving the retriever into a concrete `RankDocsQuery`. Two things
  run **in parallel** here, rendered as siblings at the same level:
  - **`retriever`** — a node tree mirroring the retriever tree. Each node has a `type` and
    `total_time_in_nanos`. A **compound** node (`rank_fusion` / `score_fusion`) carries a
    `breakdown` that reconciles to its total —
    `total = fuse + orchestration_overhead + max(child total)` — where `fuse` is the fusion
    own-compute and `orchestration_overhead` is the child fan-out / listener hand-off /
    pre-fuse gathering / parallel-scheduling skew (children run in parallel, so productive
    child time is the `max`, not the sum; `orchestration_overhead` is omitted when not
    positive). It also carries a `children` array. A **leaf** (`standard`) carries its
    per-shard query profiles under a `shards` array (each shard → `id` + a `searches` array
    of Lucene query profiles + `fetch`).
  - **`global_leg`** — the union search (aggregations / `track_total_hits` over the union of
    leaf queries), present **only** when a global leg ran; carries its own
    `total_time_in_nanos` and per-shard profiles.

  Because the two run concurrently, the phase's productive time is
  `max(retriever, global_leg)`. `self_resolve.total_time_in_nanos` is the phase wall time,
  and `self_resolve.breakdown.coordinator_overhead` is `total − max(retriever, global_leg)`
  (latch, dispatch, and coordinator-assembly slack). The `breakdown` is omitted when that
  overhead is not positive.
- **`rank_docs_query`** — the final `RankDocsQuery` fetch search, run **sequentially after**
  `self_resolve`. Carries its per-shard profiles under `shards`, its wall
  `total_time_in_nanos`, and a `breakdown.coordinator_overhead` of
  `total − max(shard query times)` (rewrite, dispatch, and reduce on top of the pure shard
  work).
- **`total_time_in_nanos`** — the top-level total, exactly `self_resolve + rank_docs_query`
  (the two phases are sequential, so this is a true coordinator wall clock in which every
  sub-number is accounted for).

Request template (helper `profileSearchJson`):
```json
{"retriever": <retrieverBody>, "profile": true, "size": <size>
 /* optional: , "track_total_hits": true */ }
```

Representative `profile` section, **captured from PR-IT-1** (3-shard cluster; only the first
shard of each leg shown for brevity — the real response has all 3 shards per leg; timings &
UUIDs vary per run):
```json
{
  "profile": {
    "self_resolve": {
      "total_time_in_nanos": 12559473,
      "breakdown": { "coordinator_overhead": 27999 },
      "retriever": {
        "type": "rank_fusion",
        "total_time_in_nanos": 12531474,
        "breakdown": { "fuse": 71194, "orchestration_overhead": 735133 },
        "children": [
          {
            "type": "standard",
            "total_time_in_nanos": 11725147,
            "shards": [
              {
                "id": "[00tUaJKHR2uo4wNtnjupBg][products][0]",
                "searches": [
                  { "query": [ { "type": "TermQuery", "description": "title:headphones", "time_in_nanos": 305151,
                        "breakdown": { "create_weight": 153088, "build_scorer": 131232, "next_doc": 11518, "score": 9313 } } ],
                    "rewrite_time": 9844,
                    "collector": [ { "name": "TopScoreDocCollector", "reason": "search_top_hits", "time_in_nanos": 34409 } ] }
                ],
                "aggregations": [],
                "fetch": [
                  { "type": "fetch", "description": "fetch", "time_in_nanos": 132172,
                    "breakdown": { "create_stored_fields_visitor": 12070, "build_sub_phase_processors": 23942,
                      "get_next_reader": 1844, "load_stored_fields": 94316, "load_source": 0 } }
                ]
              }
            ]
          },
          {
            "type": "standard",
            "total_time_in_nanos": 10721457,
            "shards": [
              {
                "id": "[00tUaJKHR2uo4wNtnjupBg][products][0]",
                "searches": [
                  { "query": [ { "type": "ConstantScoreQuery", "description": "ConstantScore(brand:acme)", "time_in_nanos": 181036,
                        "breakdown": { "create_weight": 35443, "build_scorer": 137791, "next_doc": 7040, "score": 762 },
                        "children": [ { "type": "TermQuery", "description": "brand:acme", "time_in_nanos": 134162,
                            "breakdown": { "create_weight": 8046, "build_scorer": 121635, "next_doc": 4481 } } ] } ],
                    "rewrite_time": 12892,
                    "collector": [ { "name": "TopScoreDocCollector", "reason": "search_top_hits", "time_in_nanos": 15144 } ] }
                ],
                "aggregations": [],
                "fetch": [ { "type": "fetch", "description": "fetch", "time_in_nanos": 77783,
                    "breakdown": { "create_stored_fields_visitor": 26149, "build_sub_phase_processors": 12864, "load_stored_fields": 37527 } } ]
              }
            ]
          }
        ]
      }
    },
    "rank_docs_query": {
      "total_time_in_nanos": 7423579,
      "breakdown": { "coordinator_overhead": 7257572 },
      "shards": [
        {
          "id": "[00tUaJKHR2uo4wNtnjupBg][products][0]",
          "searches": [
            { "query": [ { "type": "RankDocsQuery", "description": "RankDocsQuery(index=products, shardId=0, docs=2)",
                  "time_in_nanos": 166007,
                  "breakdown": { "create_weight": 1828, "build_scorer": 158500, "next_doc": 4007, "score": 1672 } } ],
              "rewrite_time": 6526,
              "collector": [ { "name": "TopScoreDocCollector", "reason": "search_top_hits", "time_in_nanos": 20146 } ] }
          ],
          "aggregations": [],
          "fetch": [
            { "type": "fetch", "description": "fetch", "time_in_nanos": 141186,
              "breakdown": { "create_stored_fields_visitor": 8920, "build_sub_phase_processors": 20012,
                "get_next_reader": 1383, "load_stored_fields": 100776, "load_source": 2646 },
              "children": [
                { "type": "FetchSourcePhase", "description": "FetchSourcePhase", "time_in_nanos": 7449,
                  "breakdown": { "set_next_reader": 1677, "process": 5772 } }
              ] }
          ]
        }
      ]
    },
    "total_time_in_nanos": 19983052
  }
}
```

> **Reconciliation (from the capture above).** Every level's numbers add up:
> - **compound node:** `rank_fusion.total (12,531,474) = fuse (71,194) + orchestration_overhead (735,133) + max(child total) (11,725,147)`. Children run in parallel, so productive child time is the `max`, not the sum; `orchestration_overhead` catches fan-out / listener hand-off / pre-fuse gathering / scheduling skew.
> - **self_resolve phase:** `self_resolve.total (12,559,473) = max(retriever, global_leg) (12,531,474) + coordinator_overhead (27,999)`.
> - **top level:** `total (19,983,052) = self_resolve (12,559,473) + rank_docs_query (7,423,579)` — an exact sum, because the two phases are sequential.
>
> The `rank_docs_query` leaf `fetch` is fully populated (`fetch` + a `FetchSourcePhase` child) —
> the final `RankDocsQuery` search runs a real fetch phase; earlier drafts of this doc trimmed it
> to `[]`, which was a documentation artifact, not the actual output.

---

## PR-IT-1 — `rank_fusion` profile tree

**Test:** `testRankFusionProfileTree`
**Purpose:** `profile:true` returns a `rank_fusion` root node with `standard` leaf
children, a `fuse` breakdown, per-node timing, per-leg shard query profiles that show the
**real leg query** (not just the RankDocsQuery replay), and a `rank_docs_query` section.

**Input** (`RANK_FUSION_TWO_LEG`, `size:10`, `DFS_QUERY_THEN_FETCH`, no track_total_hits):
```json
{
  "retriever": {"rank_fusion": {"retrievers": [
    {"standard": {"query": {"match": {"title": "headphones"}}}},
    {"standard": {"query": {"term":  {"brand": "acme"}}}}
  ]}},
  "profile": true,
  "size": 10
}
```

**Output:** the `profile` skeleton above.

**Asserts (substrings on the rendered JSON):** `"profile":{`; `"self_resolve":{`;
`"retriever":{`; root `"type":"rank_fusion"`; child `"type":"standard"`;
`"breakdown":{"fuse":`; `"total_time_in_nanos":`; a leaf `"searches":[`; the leg term
`headphones` appears in a shard query description; `"children":[`; `"rank_docs_query":{`.

---

## PR-IT-2 — aggregations add a `global_leg` section

**Test:** `testProfileIncludesGlobalLegWithAggs`
**Purpose:** When aggregations run, the union/global search is profiled under `global_leg`.

**Input** (`RANK_FUSION_TWO_LEG` + a terms agg, `size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"rank_fusion": {"retrievers": [
    {"standard": {"query": {"match": {"title": "headphones"}}}},
    {"standard": {"query": {"term":  {"brand": "acme"}}}}
  ]}},
  "profile": true,
  "size": 10,
  "aggs": {"brands": {"terms": {"field": "brand"}}}
}
```

**Output:** the profile from PR-IT-1 **plus** a `global_leg` section nested inside
`self_resolve` alongside `retriever` (captured; the global leg's `bool.should` union has been
rewritten by Lucene to a scoreless `ConstantScore` because it runs `size:0`):
```json
{
  "profile": {
    "self_resolve": {
      "total_time_in_nanos": 126017931,
      "breakdown": { "coordinator_overhead": 369005 },
      "retriever": { "type": "rank_fusion", "...": "..." },
      "global_leg": {
        "total_time_in_nanos": 125648926,
        "shards": [ { "id": "[TT0kPhylQduW4YsMB8SKWA][products][1]",
          "searches": [ { "query": [ { "type": "ConstantScoreQuery", "description": "ConstantScore(title:headphones brand:acme)",
                "children": [ { "type": "BooleanQuery", "description": "title:headphones brand:acme",
                    "children": [ { "type": "TermQuery", "description": "title:headphones" },
                                  { "type": "TermQuery", "description": "brand:acme" } ] } ] } ],
              "collector": [ { "name": "QueryCollectorManager", "reason": "search_multi" } ] } ],
          "aggregations": [ { "type": "GlobalOrdinalsStringTermsAggregator", "description": "brands" } ],
          "fetch": [ ] } ]
      }
    },
    "rank_docs_query": { "...": "..." },
    "total_time_in_nanos": 133441000
  }
}
```

> The `global_leg` query is `ConstantScore(title:headphones brand:acme)` wrapping a
> `BooleanQuery` with one `TermQuery` child per leg — i.e. the `bool.should` union of the
> legs (`CompoundRetrieverBuilder.extractAggregationQuery()`), after Lucene's `size:0` rewrite
> constant-scores it (scoring is unnecessary for a count/aggregate). The global-leg `fetch` is
> `[]` because it runs `size:0`. This is the correct, near-optimal union query — no change needed.

**Asserts:** rendered JSON contains `"global_leg":{` and `"retriever":{`.

---

## PR-IT-3 — `track_total_hits` triggers the `global_leg` (no aggs)

**Test:** `testProfileIncludesGlobalLegWithTrackTotalHits`
**Purpose:** `track_total_hits:true` alone forces the global leg to run, so `global_leg`
appears even without aggregations.

**Input** (`RANK_FUSION_TWO_LEG`, `size:10`, `DFS_QUERY_THEN_FETCH`, `track_total_hits:true`):
```json
{
  "retriever": {"rank_fusion": {"retrievers": [
    {"standard": {"query": {"match": {"title": "headphones"}}}},
    {"standard": {"query": {"term":  {"brand": "acme"}}}}
  ]}},
  "profile": true,
  "size": 10,
  "track_total_hits": true
}
```

**Output:** the PR-IT-1 profile plus a `global_leg` nested under `self_resolve` (as in
PR-IT-2, but with `aggregations: []` and a `TotalHitCountCollector` instead of the aggregation
collector). Captured `global_leg` query is `ConstantScore(title:headphones brand:acme)` over
the two-leg `bool.should` union, `global_leg.total_time_in_nanos ≈ 153973486`,
`aggregations: []`, `fetch: []` (size:0):
```json
{
  "profile": {
    "self_resolve": {
      "total_time_in_nanos": 154200000,
      "breakdown": { "coordinator_overhead": 226514 },
      "retriever": { "type": "rank_fusion", "...": "..." },
      "global_leg": {
        "total_time_in_nanos": 153973486,
        "shards": [ { "id": "[zoOrPdRqQUm2ePm4P7g2WA][products][0]",
          "searches": [ { "query": [ { "type": "ConstantScoreQuery", "description": "ConstantScore(title:headphones brand:acme)" } ],
              "collector": [ { "name": "TotalHitCountCollector", "reason": "search_count" } ] } ],
          "aggregations": [], "fetch": [] } ]
      }
    },
    "rank_docs_query": { "...": "..." },
    "total_time_in_nanos": "..."
  }
}
```

**Asserts:** rendered JSON contains `"global_leg":{`.

---

## PR-IT-4 — nested fusion produces a nested profile tree

**Test:** `testNestedProfileTree`
**Purpose:** A `rank_fusion` nested inside a `rank_fusion` yields two `rank_fusion` nodes
in the tree (children within children).

**Input** (`size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"rank_fusion": {"retrievers": [
    {"rank_fusion": {"retrievers": [
      {"standard": {"query": {"match": {"title": "headphones"}}}},
      {"standard": {"query": {"term":  {"brand": "acme"}}}}
    ]}},
    {"standard": {"query": {"match": {"title": "earbuds"}}}}
  ]}},
  "profile": true,
  "size": 10
}
```

**Output** (captured; nested `children`, shard profiles omitted for brevity; the retriever
tree now sits under `self_resolve`):
```json
{
  "profile": {
    "self_resolve": {
      "total_time_in_nanos": 9740000,
      "breakdown": { "coordinator_overhead": 32625 },
      "retriever": {
        "type": "rank_fusion",
        "total_time_in_nanos": 9707375,
        "breakdown": { "fuse": 33891 },
        "children": [
          {
            "type": "rank_fusion",
            "total_time_in_nanos": 8911571,
            "breakdown": { "fuse": 40748 },
            "children": [
              { "type": "standard", "total_time_in_nanos": 5907434, "shards": [ "... 3 shards ..." ] },
              { "type": "standard", "total_time_in_nanos": 7946711, "shards": [ "... 3 shards ..." ] }
            ]
          },
          { "type": "standard", "total_time_in_nanos": 7995488, "shards": [ "... 3 shards ..." ] }
        ]
      }
    },
    "rank_docs_query": { "...": "..." },
    "total_time_in_nanos": "..."
  }
}
```

**Asserts:** `"type":"rank_fusion"` occurs at least twice in the rendered JSON (outer +
inner).

---

## PR-IT-5 — `score_fusion` profile tree

**Test:** `testScoreFusionProfileTree`
**Purpose:** A `score_fusion` retriever produces a profile tree whose root node type is
`score_fusion`, with per-node timing.

**Input** (`SCORE_FUSION_TWO_LEG`, `size:10`, `DFS_QUERY_THEN_FETCH`):
```json
{
  "retriever": {"score_fusion": {"retrievers": [
    {"standard": {"query": {"match": {"title": "headphones"}}}},
    {"standard": {"query": {"term":  {"brand": "acme"}}}}],
    "normalization": {"technique": "min_max"},
    "combination":   {"technique": "arithmetic_mean"}}},
  "profile": true,
  "size": 10
}
```

**Output:** the PR-IT-1 skeleton with the root node `"type":"score_fusion"` (its
`breakdown` reflects normalization/combination compute) and `standard` leaf children.
Captured root, under `self_resolve` (shard profiles omitted):
```json
{
  "profile": {
    "self_resolve": {
      "total_time_in_nanos": 7721000,
      "breakdown": { "coordinator_overhead": 18460 },
      "retriever": {
        "type": "score_fusion",
        "total_time_in_nanos": 7702540,
        "breakdown": { "fuse": 69106 },
        "children": [
          { "type": "standard", "total_time_in_nanos": "...", "shards": [ "... 3 shards ..." ] },
          { "type": "standard", "total_time_in_nanos": "...", "shards": [ "... 3 shards ..." ] }
        ]
      }
    },
    "rank_docs_query": { "...": "..." },
    "total_time_in_nanos": "..."
  }
}
```

**Asserts:** rendered JSON contains `"type":"score_fusion"` and `"total_time_in_nanos":`.

---

# Running these tests

From `/local/home/bzhangam/retriever-framework/OpenSearch`:

```bash
export JAVA_HOME=/local/apollo/package/local_1/AL2_x86_64/JDK21/JDK21-4711.0-0/jdk-21

# All explain integ tests
./gradlew :server:internalClusterTest --tests "org.opensearch.search.retriever.RetrieverExplainIT"

# All profile integ tests
./gradlew :server:internalClusterTest --tests "org.opensearch.search.retriever.RetrieverProfileIT"

# A single scenario
./gradlew :server:internalClusterTest \
  --tests "org.opensearch.search.retriever.RetrieverProfileIT.testRankFusionProfileTree"
```

Companion unit tests that pin the exact arithmetic / rendering:
`RetrieverExplainTests`, `RetrieverProfileTests`, `RankFusionRetrieverBuilderTests`,
`ScoreFusionRetrieverBuilderTests`.
