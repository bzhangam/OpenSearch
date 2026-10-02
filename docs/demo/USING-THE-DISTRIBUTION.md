# Using the diversify-retriever demo distribution

A ready-to-run OpenSearch **3.7.0-SNAPSHOT** distribution bundling the retriever-framework forks
(core + k-NN + neural-search @ `retriever-framework-3.7`) is published as a GitHub release asset on the
personal fork. It includes the full retriever framework — `standard`, `rank_fusion`, `score_fusion`,
`pin`, and the new **`diversify`** (MMR) retriever — plus native Faiss + Lucene k-NN, neural-search, and
ml-commons.

- **Release page:** https://github.com/bzhangam/OpenSearch/releases/tag/retriever-framework-3.7-demo
- **Direct download (Linux x64, ~772 MB):**
  `https://github.com/bzhangam/OpenSearch/releases/download/retriever-framework-3.7-demo/opensearch-3.7.0-SNAPSHOT-linux-x64.tar.gz`
- **Platform:** Linux x86_64. **Java:** bundled (ships its own JDK). Marked **pre-release** (SNAPSHOT).

---

## Option 1 — single node (quickest)

```bash
# 1. download + extract
curl -L -o opensearch-diversify.tar.gz \
  https://github.com/bzhangam/OpenSearch/releases/download/retriever-framework-3.7-demo/opensearch-3.7.0-SNAPSHOT-linux-x64.tar.gz
tar xzf opensearch-diversify.tar.gz
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

---

## Option 2 — local 3-node cluster

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

## Create a vector index + data

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
```
Confirm the field is really a vector before searching:
```bash
curl -s localhost:9200/products/_mapping | grep -o '"embedding":{"type":"knn_vector"'
```

---

## Use the `diversify` retriever

**The problem it solves:** a plain k-NN search returns near-duplicates at the top (here `a` and `b`),
pushing diverse results down. `diversify` re-ranks for relevance **and** diversity using MMR.

```bash
# plain k-NN (baseline): returns [a, b, c] — the near-duplicate b sits second
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 3, "_source": false,
  "query": { "knn": { "embedding": { "vector": [1.0, 0.0], "k": 3 } } } }'

# diversify (lambda=0.1 = high diversity): returns [a, c, b] — diverse c promoted over near-dup b
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 3,
  "retriever": { "diversify": {
    "vector_field": "embedding",
    "lambda": 0.1,
    "window_size": 100,
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } }
  } } }'
```

### Parameters

| Param | Required | Default | Meaning |
|-------|----------|---------|---------|
| `retriever` | yes | — | the child retriever to diversify (any subtree; usually `standard` over `knn`/`neural`) |
| `vector_field` | yes | — | the `knn_vector` field whose vectors drive diversity |
| `lambda` | no | `0.5` | relevance ↔ diversity dial in `[0,1]`. **`1.0` = pure relevance** (no-op), **`0.0` = max diversity** |
| `window_size` | no | `100` | how many top candidates to re-rank |
| `space_type` / `vector_data_type` | no | from mapping | override the similarity space / `float`\|`byte` if needed |

**Tuning tip:** start at `lambda=0.3` for RAG dedup (one best answer + varied follow-ups). Raise toward
`1.0` for more relevance, lower toward `0.0` for more spread.

### Add `explain` to see the MMR math
```bash
curl -s localhost:9200/products/_search -H 'Content-Type: application/json' -d '{
  "size": 3, "explain": true,
  "retriever": { "diversify": { "vector_field": "embedding", "lambda": 0.1, "window_size": 100,
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } } } } }'
# each hit explains: diversify: selected by MMR [lambda=..., diversity=..., max_similarity_to_selected=..., mmr_score=...]
```

### Works with `_source` disabled (efficient vector pull)
Add `"_source": false` — diversify still works because it reads vectors via a `docvalue_fields`
ride-along, not from `_source`.

---

## Use the other retrievers

**Hybrid search with `rank_fusion`** (keyword + vector, RRF):
```json
{ "retriever": { "rank_fusion": { "rank_window_size": 100, "retrievers": [
  { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } },
  { "standard": { "query": { "match": { "title": "wireless" } } } }
] } } }
```

**`score_fusion`** (normalized, weighted — note `weights` nests under `combination.parameters`):
```json
{ "retriever": { "score_fusion": {
  "normalization": "min_max",
  "combination": { "technique": "arithmetic_mean", "parameters": { "weights": [1.0, 2.0] } },
  "rank_window_size": 100,
  "retrievers": [
    { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } },
    { "standard": { "query": { "match": { "title": "wireless" } } } } ] } } }
```

**`pin`** (force curated docs to the top):
```json
{ "retriever": { "pin": {
  "ids": ["c"],
  "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } } } } }
```

**Diversify one leg of a hybrid search** (per-leg, before fusion):
```json
{ "retriever": { "rank_fusion": { "rank_window_size": 100, "retrievers": [
  { "diversify": { "vector_field": "embedding", "lambda": 0.2, "window_size": 100,
      "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 100 } } } } } } },
  { "standard": { "query": { "match": { "title": "wireless" } } } }
] } } }
```

**Profiling & pagination:** add `"profile": true` to any retriever search for a tree of per-retriever
timings; use `"from"` / `"size"` to page over the result.

---

## Neural (model-backed) search

The build includes neural-search + ml-commons, so you can diversify a `neural` leg (embeddings produced
by an ML model). Register + deploy a text-embedding model via `_plugins/_ml`, create an index with a
`text_embedding` ingest pipeline (`text` → a `knn_vector` field), then:
```json
{ "retriever": { "diversify": { "vector_field": "passage_knn", "lambda": 0.2, "window_size": 100,
  "retriever": { "standard": { "query": { "neural": { "passage_knn": {
    "query_text": "how do I reset my password", "model_id": "<model_id>", "k": 100 } } } } } } } }
```
Point `vector_field` at a **directly-mapped `knn_vector`** field. (The newer `semantic` field type, which
stores its vector at a nested path, is **not** supported as `vector_field` in v1.)

---

## Full worked examples

Every tested scenario — with exact request JSON, the actual response, and a plain-English verdict,
including the RAG near-duplicate dedup demo — is in **`docs/demo/diversify-live-demo.md`** (Parts A–D).

## Troubleshooting quick reference

| Symptom | Cause | Fix |
|---------|-------|-----|
| Node exits on first vector index; `UnsatisfiedLinkError ... .so cannot open shared object` | k-NN native libs not on linker path | `export LD_LIBRARY_PATH=<home>/plugins/opensearch-knn/lib:$LD_LIBRARY_PATH` before start |
| `Field 'embedding' is not knn_vector type` | used the flat `"index.knn":true` settings form | use nested `"settings":{"index":{"knn":true,...}}`; verify mapping |
| `cluster create-index blocked (api)` / shards won't allocate | disk above watermark | relax disk watermark settings (see Option 2) |
| `[score_fusion] unknown field [weights]` | `weights` at top level | nest under `combination.parameters.weights` |
| `MMR query extension cannot support non knn_vector field` | `vector_field` points at a non-vector field | point it at the `knn_vector` field |
