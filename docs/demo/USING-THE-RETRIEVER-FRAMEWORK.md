# Using the OpenSearch retriever framework (demo distribution)

A ready-to-run OpenSearch **3.7.0-SNAPSHOT** distribution that bundles the **retriever framework** —
built from the forks of core + k-NN + neural-search on `retriever-framework-3.7`, plus native Faiss +
Lucene k-NN, neural-search, and ml-commons. This guide shows how to download it, run it, and use **every
retriever** in the framework.

- **Release page:** https://github.com/bzhangam/OpenSearch/releases/tag/retriever-framework-3.7-demo
- **Direct download (Linux x64, ~772 MB):**
  `https://github.com/bzhangam/OpenSearch/releases/download/retriever-framework-3.7-demo/opensearch-3.7.0-SNAPSHOT-linux-x64.tar.gz`
- **Platform:** Linux x86_64. **Java:** bundled (ships its own JDK). Marked **pre-release** (SNAPSHOT).

---

## What is the retriever framework?

A **retriever** is a tree-structured way to describe *how* to fetch and rank results, placed under a
top-level `"retriever"` key in `_search` (instead of, or in addition to, a plain `"query"`). Retrievers
**compose**: a retriever can wrap a child retriever, so you can express "search, then fuse, then
re-rank" declaratively in one request — no search pipeline required.

```
_search
└── retriever
    └── <some retriever>
        └── (optionally) child retriever(s)
```

- **Leaf** retrievers produce results from a query (`standard`).
- **Fusion** retrievers combine several child retrievers (`rank_fusion`, `score_fusion`).
- **Re-ranker / transformer** retrievers wrap a single child and reshape its ranking (`pin`,
  `diversify`).

Everything below (`explain`, `profile`, `from`/`size` pagination) works on **any** retriever tree.

### Retrievers in this distribution

| Retriever | Kind | What it does | Owner |
|-----------|------|--------------|-------|
| [`standard`](#standard--run-any-query) | leaf | Wraps any OpenSearch `query` (`match`, `knn`, `neural`, …) as a retriever. The building block every other retriever wraps. | core |
| [`rank_fusion`](#rank_fusion--hybrid-search-by-rank-rrf) | fusion | Combines multiple legs by **rank** using Reciprocal Rank Fusion (RRF). The classic hybrid (keyword + vector). | core |
| [`score_fusion`](#score_fusion--hybrid-search-by-normalized-score) | fusion | Combines legs by **normalized score** with a weighted technique (e.g. min-max + weighted mean). | core |
| [`pin`](#pin--force-curated-documents-to-the-top) | re-ranker | Forces chosen document ids to the top of a child ranking (editorial curation / merchandising). | core |
| [`diversify`](#diversify--relevance--diversity-mmr) | re-ranker | Re-ranks a child for **relevance + diversity** using MMR, so results aren't near-duplicates. | k-NN |

> There is no separate `knn` or `neural` retriever — you run those as **queries inside a `standard`
> retriever** (see below). The build also ships neural-search + ml-commons, so `neural` queries work.

---

## Part 1 — run the distribution

### Option A — single node (quickest)

```bash
# 1. download + extract
curl -L -o opensearch-rf.tar.gz \
  https://github.com/bzhangam/OpenSearch/releases/download/retriever-framework-3.7-demo/opensearch-3.7.0-SNAPSHOT-linux-x64.tar.gz
tar xzf opensearch-rf.tar.gz
cd opensearch-3.7.0

# 2. demo-friendly config: security off, single node
printf '\nplugins.security.disabled: true\ndiscovery.type: single-node\n' >> config/opensearch.yml

# 3. (k-NN native libs) let the dynamic linker find the Faiss .so files
export LD_LIBRARY_PATH="$PWD/plugins/opensearch-knn/lib:$LD_LIBRARY_PATH"

# 4. start
./bin/opensearch
```

In another shell:
```bash
curl -s localhost:9200                 # -> "number":"3.7.0-SNAPSHOT"
curl -s localhost:9200/_cat/plugins?v  # -> opensearch-knn, opensearch-neural-search, opensearch-ml, ...
```

> **If the node dies the moment you index a vector** with
> `UnsatisfiedLinkError: ... libopensearchknn_*.so: cannot open shared object file`, you skipped step 3.
> The Faiss libraries depend on sibling `.so`s in `plugins/opensearch-knn/lib/`, and the OS dynamic
> linker resolves those via `LD_LIBRARY_PATH` (not `-Djava.library.path`). Export it **before** starting
> the node. (The pure-Java **Lucene** engine needs none of this — a safe fallback: use
> `"engine":"lucene"` in your mapping.)

### Option B — local 3-node cluster

Extract three copies (`node1/ node2/ node3/`) and give each a config like below (only the two ports and
`node.name` change per node). Start each with the `LD_LIBRARY_PATH` export pointing at *that* node's
`plugins/opensearch-knn/lib`.

`nodeN/opensearch-3.7.0/config/opensearch.yml`:
```yaml
cluster.name: retriever-demo
node.name: node-1                       # node-2 / node-3 on the others
network.host: 127.0.0.1
http.port: 9200                         # 9201 / 9202
transport.port: 9300                    # 9301 / 9302
discovery.seed_hosts: ["127.0.0.1:9300","127.0.0.1:9301","127.0.0.1:9302"]
cluster.initial_cluster_manager_nodes: ["node-1","node-2","node-3"]
plugins.security.disabled: true
```
Start each node:
```bash
LD_LIBRARY_PATH="$PWD/nodeN/opensearch-3.7.0/plugins/opensearch-knn/lib:$LD_LIBRARY_PATH" \
  nodeN/opensearch-3.7.0/bin/opensearch &
```
Verify: `curl -s localhost:9200/_cat/health?v` → `green`, `node.total 3`.

> **Disk watermark on a near-full host:** if index creation is blocked
> (`cluster create-index blocked (api)`), relax the watermark for a throwaway demo:
> ```bash
> curl -s -X PUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' -d '{
>   "persistent":{"cluster.routing.allocation.disk.threshold_enabled":false,
>   "cluster.routing.allocation.disk.watermark.flood_stage":"99%","cluster.blocks.create_index":false}}'
> ```

---

## Part 2 — a sample index

A tiny index used by every example below. One text field (`title`) and one 2-D vector field
(`embedding`) so both keyword and vector retrievers work.

> **Use the nested `settings.index` form** — the flat `"index.knn": true` form is silently ignored and
> your field won't actually be a `knn_vector`.

```bash
curl -s -X PUT localhost:9200/products -H 'Content-Type: application/json' -d '{
  "settings": { "index": { "knn": true, "number_of_shards": 3, "number_of_replicas": 0 } },
  "mappings": { "properties": {
    "embedding": { "type": "knn_vector", "dimension": 2,
      "method": { "name": "hnsw", "space_type": "l2", "engine": "faiss" } },
    "title": { "type": "text" } } }
}'

curl -s -X POST 'localhost:9200/products/_bulk?refresh=true' -H 'Content-Type: application/json' -d '
{"index":{"_id":"a"}}
{"embedding":[1.00,0.00],"title":"wireless noise cancelling headphones"}
{"index":{"_id":"b"}}
{"embedding":[0.98,0.02],"title":"wireless noise-cancelling headphone"}
{"index":{"_id":"c"}}
{"embedding":[0.00,1.00],"title":"stainless steel water bottle"}
'
curl -s localhost:9200/products/_mapping | grep -o '"embedding":{"type":"knn_vector"'   # confirm vector field
```

Geometry to keep in mind: the query vector `[1,0]` makes `a` the top match, `b` a **near-duplicate** of
`a`, and `c` **orthogonal** (diverse).

---

## Part 3 — the retrievers

### `standard` — run any query

The leaf retriever. It wraps any OpenSearch `query`. Use it on its own, or as the child of a fusion /
re-ranker retriever.

```bash
# standard over a keyword query
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 3,
  "retriever": { "standard": { "query": { "match": { "title": "wireless" } } } } }'

# standard over a vector (knn) query  -> [a, b, c]  (near-duplicate b is #2)
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 3,
  "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } } }'
```

| Param | Required | Meaning |
|-------|----------|---------|
| `query` | yes | any OpenSearch query DSL (`match`, `term`, `knn`, `neural`, `bool`, …) |

### `rank_fusion` — hybrid search by rank (RRF)

Combines several legs using Reciprocal Rank Fusion: each doc's score is `Σ 1/(rank_constant + rank)`
across the legs. Best when the legs' raw scores aren't comparable (BM25 vs vector distance).

```bash
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 5,
  "retriever": { "rank_fusion": {
    "rank_window_size": 100,
    "rank_constant": 60,
    "retrievers": [
      { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } },
      { "standard": { "query": { "match": { "title": "wireless" } } } }
    ] } } }'
```

| Param | Default | Meaning |
|-------|---------|---------|
| `retrievers` | — | the legs to fuse (any retriever subtrees) |
| `rank_window_size` | `100` | how many candidates each leg contributes |
| `rank_constant` | `60` | RRF constant (higher = flatter weighting across ranks) |

### `score_fusion` — hybrid search by normalized score

Normalizes each leg's scores to a common scale, then combines them (optionally weighted). Use when you
want the actual score magnitudes to matter and to weight legs.

```bash
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 5,
  "retriever": { "score_fusion": {
    "normalization": "min_max",
    "combination": { "technique": "arithmetic_mean", "parameters": { "weights": [1.0, 2.0] } },
    "rank_window_size": 100,
    "retrievers": [
      { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } },
      { "standard": { "query": { "match": { "title": "wireless" } } } }
    ] } } }'
```

| Param | Meaning |
|-------|---------|
| `normalization` | how to scale each leg's scores, e.g. `min_max` |
| `combination` | `{ "technique": "arithmetic_mean", "parameters": { "weights": [...] } }` — one weight per leg |
| `rank_window_size` | candidates per leg |

> **Gotcha:** `weights` is **not** a top-level field — it nests under `combination.parameters`. Putting
> it at the top gives `"[score_fusion] unknown field [weights]"`.

`rank_fusion` vs `score_fusion`: the former combines **ranks** (robust, scale-free), the latter combines
**normalized scores** (lets you weight legs and keep score magnitude).

### `pin` — force curated documents to the top

Pins chosen document ids to the top in the order given, then lets the child retriever fill the rest.
Editorial curation / merchandising.

```bash
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 5,
  "retriever": { "pin": {
    "ids": ["c"],
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } }
  } } }'
# -> c pinned to #1, then the organic knn ranking (a, b, ...)
```

| Param | Meaning |
|-------|---------|
| `ids` *(or `docs`)* | ids to pin to the top, in order. `docs` form is `[{"_id":"x","_index":"i"}]` for cross-index pins |
| `retriever` | the child whose ranking fills the non-pinned positions |
| `require_match` | if true, only pin ids that the child actually matched |

### `diversify` — relevance + diversity (MMR)

Re-ranks a child's top `window_size` candidates for **relevance *and* diversity** using Maximal Marginal
Relevance, so a result set isn't dominated by near-duplicates (the RAG "five near-identical chunks"
problem). Owned by the k-NN plugin because it reads `knn_vector` vectors.

```bash
# plain knn over 'products' is [a, b, c] — near-duplicate b sits at #2.
# diversify (lambda=0.1 = high diversity) -> [a, c, b] — diverse c promoted over near-dup b.
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 3,
  "retriever": { "diversify": {
    "vector_field": "embedding",
    "lambda": 0.1,
    "window_size": 100,
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } }
  } } }'
```

| Param | Required | Default | Meaning |
|-------|----------|---------|---------|
| `retriever` | yes | — | the child to diversify (any subtree; usually `standard` over `knn`/`neural`) |
| `vector_field` | yes | — | the `knn_vector` field whose vectors drive diversity |
| `lambda` | no | `0.5` | relevance ↔ diversity dial in `[0,1]`. **`1.0` = pure relevance** (no-op), **`0.0` = max diversity**. `~0.3` is a good RAG-dedup default |
| `window_size` | no | `100` | how many top candidates to re-rank |
| `space_type` / `vector_data_type` | no | from mapping | override similarity space / `float`\|`byte` if needed |

Notes:
- Add `"_source": false` and it still works — `diversify` reads vectors via a `docvalue_fields`
  ride-along, not from `_source`.
- Point `vector_field` at a **directly-mapped `knn_vector`**. The newer `semantic` field type (nested
  vector path) is **not** supported as `vector_field` in v1.

### Composition — a re-ranker inside a fusion

Retrievers nest. For example, diversify *one leg* of a hybrid search before fusion:

```bash
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 5,
  "retriever": { "rank_fusion": {
    "rank_window_size": 100,
    "retrievers": [
      { "diversify": { "vector_field": "embedding", "lambda": 0.2, "window_size": 100,
          "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } } } },
      { "standard": { "query": { "match": { "title": "wireless" } } } }
    ] } } }'
```

---

## Part 4 — cross-cutting features (work on any retriever)

### `explain`
Add `"explain": true` to see how each hit was produced. For `diversify` each hit reads
`diversify: selected by MMR [lambda=..., diversity=..., max_similarity_to_selected=..., mmr_score=...]`;
for `rank_fusion` it shows each leg's `1/(rank_constant+rank)` contribution.

### `profile`
Add `"profile": true` for a tree of per-retriever timings that reconcile parent↔child (coordinator
overhead, each retriever node, and per-shard query timings down to slice level). Use it to see where
time goes in a composed tree.

### Pagination
Use `"from"` / `"size"` to page over any retriever result; paging is stable across pages.

---

## Part 5 — neural (model-backed) search

The build includes neural-search + ml-commons, so a `standard` leg can run a `neural` query (embeddings
produced by an ML model), and you can wrap that in any retriever (e.g. `diversify`). Register + deploy a
text-embedding model via `_plugins/_ml`, create an index with a `text_embedding` ingest pipeline
(`text` → a `knn_vector` field), then:

```json
{ "retriever": { "diversify": { "vector_field": "passage_knn", "lambda": 0.2, "window_size": 100,
  "retriever": { "standard": { "query": { "neural": { "passage_knn": {
    "query_text": "how do I reset my password", "model_id": "<model_id>", "k": 100 } } } } } } } }
```

---

## Full worked examples (request → response → verdict)

Every retriever above was tested on a local 3-node cluster with exact request JSON, the actual response,
and a plain-English verdict — including a RAG near-duplicate dedup demo and hybrid-search examples — in
**[`docs/demo/diversify-live-demo.md`](./diversify-live-demo.md)** (Part A = correctness tests,
Part B = matrix, Part C = the RAG demo, Part D = `rank_fusion` / `score_fusion` / `pin` / profile /
pagination / neural).

## Troubleshooting quick reference

| Symptom | Cause | Fix |
|---------|-------|-----|
| Node exits on first vector index; `UnsatisfiedLinkError ... .so cannot open shared object` | k-NN native libs not on linker path | `export LD_LIBRARY_PATH=<home>/plugins/opensearch-knn/lib:$LD_LIBRARY_PATH` before start |
| `Field 'embedding' is not knn_vector type` | used the flat `"index.knn":true` settings form | use nested `"settings":{"index":{"knn":true,...}}`; verify mapping |
| `cluster create-index blocked (api)` / shards won't allocate | disk above watermark | relax disk watermark settings (see Option B) |
| `[score_fusion] unknown field [weights]` | `weights` at top level | nest under `combination.parameters.weights` |
| `MMR query extension cannot support non knn_vector field` | `diversify.vector_field` points at a non-vector field | point it at the `knn_vector` field |
