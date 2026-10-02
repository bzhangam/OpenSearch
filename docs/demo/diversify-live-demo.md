# `diversify` retriever — live demo on a local 3-node cluster

**Date:** 2026-10-02 · **Build:** OpenSearch `3.7.0-SNAPSHOT` distribution built from our forks
(core + k-NN + neural-search on `retriever-framework-3.7`) · **Engine:** native **Faiss** (and
**Lucene** for one cross-check).

This document is written to be read start-to-finish by someone who has **not** seen the code. For every
test it gives: **what we are proving**, the **exact request** you can paste into `_search`, the **actual
response** the live cluster returned, and a **plain-English verdict**. The star of the show is the
"RAG near-duplicate" demo in [Part C](#part-c--the-live-demo-use-case-rag-near-duplicate-dedup).

> **One-line summary.** The `diversify` retriever re-ranks vector-search results for *relevance +
> diversity* using MMR (Maximal Marginal Relevance), so a search no longer returns five near-identical
> hits. A single `lambda` knob dials from pure relevance (1.0) to pure diversity (0.0).

---

## What `diversify` is (30-second version)

A normal k-NN (vector) search returns the documents whose embeddings are **closest** to the query. If
your corpus contains near-duplicates (the same answer phrased five ways), the top results are those
five near-duplicates — relevant, but redundant. For RAG this is actively harmful: the five
restatements fill the LLM's context window and crowd out other useful information.

`diversify` wraps the vector search and re-ranks its top `window_size` candidates with MMR:

```
score(candidate) = (1 - diversity) * relevance(candidate)  -  diversity * max_similarity_to_already_picked
                   where  diversity = 1 - lambda
```

- `lambda = 1.0` → diversity 0 → **pure relevance** (behaves like plain k-NN).
- `lambda = 0.0` → diversity 1 → **maximum diversity** (spread the results out).
- Vectors are pulled efficiently via a `docvalue_fields` "ride-along" on the search it already does —
  **no second query, and it works even with `_source` disabled**.

The type is registered by the k-NN plugin and composes with the retriever framework, so it can sit at
the **top level** (diversify the final result) or **inside a `rank_fusion`** (diversify one leg of a
hybrid search before fusion).

---

## Environment (how this was run)

- **Distribution:** built with `opensearch-build` in the `opensearchstaging/ci-runner:...-v1` Docker
  image (JDK 21, arch `x64`), bundling our three forks. The built tarball
  (`opensearch-3.7.0-SNAPSHOT-linux-x64.tar.gz`, ~772 MB) contains the modified core
  (`RetrieverPlugin`, `TransformerRetrieverBuilder`, `RankFusionRetrieverBuilder`) and the modified
  k-NN plugin (`DiversifyRetrieverBuilder`, `MMRSelector`). A copy is in `artifacts/`.
- **Cluster:** 3 nodes (`node-1/2/3`), one shared cluster `retriever-demo`, security disabled, each a
  `cluster_manager,data,ingest` node, heap 1 GB each. Cluster health **green**, all 3 nodes carry
  `opensearch-knn`, `opensearch-neural-search`, `opensearch-ml`, `opensearch-job-scheduler`,
  `opensearch-custom-codecs`.
- All requests below were issued against `http://localhost:9200`. Every request/response pair is saved
  alongside the run (`results/*.req.json`, `results/*.resp.json`).

### Three environment gotchas worth knowing (so you can reproduce the cluster)

1. **k-NN native libraries need `LD_LIBRARY_PATH`.** The Faiss `.so` files live in
   `plugins/opensearch-knn/lib/`. Setting `-Djava.library.path` is **not** enough — the Faiss library
   depends on *other* `.so`s in the same dir (`libopensearchknn_util.so`), and those inter-library
   dependencies are resolved by the OS dynamic linker, which reads `LD_LIBRARY_PATH`. Launch each node
   with `LD_LIBRARY_PATH=<node>/plugins/opensearch-knn/lib:$LD_LIBRARY_PATH` or the node crashes
   (`UnsatisfiedLinkError ... cannot open shared object file`) the moment it indexes a vector. This was
   the single most important fix to get Faiss running locally. (Lucene HNSW is pure-Java and needs none
   of this — a handy fallback if you can't get native libs loaded.)
2. **Disk watermark on a near-full host.** If the host disk is above ~90–95%, OpenSearch sets a
   cluster-wide `cluster.blocks.create_index` block and refuses to allocate shards. For a throwaway
   demo cluster, relax it:
   `PUT _cluster/settings {"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false,
   "cluster.routing.allocation.disk.watermark.flood_stage":"99%","cluster.blocks.create_index":false}}`.
3. **Index settings must use the nested form.** Use
   `"settings":{"index":{"knn":true,"number_of_shards":3,"number_of_replicas":0}}`. The flat form
   (`"settings":{"index.knn":true,...}`) was silently ignored, producing a non-`knn_vector` field and
   the error *"Field 'embedding' is not knn_vector type."* Always confirm the mapping shows
   `"embedding":{"type":"knn_vector"...}` before testing.

---

# Part A — the canonical correctness tests (small corpus)

**Index `products_faiss`** — 3 shards, 0 replicas, a 2-D `knn_vector` field `embedding` + a `title`
text field. Three documents, engineered so the geometry is obvious:

| id | embedding | meaning |
|----|-----------|---------|
| `a` | `[1.00, 0.00]` | top match for the query |
| `b` | `[0.98, 0.02]` | **near-duplicate of `a`** |
| `c` | `[0.00, 1.00]` | **orthogonal** (diverse) |

The query vector is `[1.0, 0.0]`, so plain k-NN ranks them **`[a, b, c]`** — the near-duplicate `b`
sits second and the diverse `c` is pushed to the bottom. That is the behavior `diversify` changes.

> **Baseline (plain k-NN, no diversify)** — for reference:
> ```json
> POST /products_faiss/_search
> {"size":3,"_source":false,"query":{"knn":{"embedding":{"vector":[1.0,0.0],"k":3}}}}
> ```
> → order **`[a, b, c]`**.

### Test A1 — top-level diversify, high diversity (`lambda=0.1`), Faiss

**Proves:** diversify promotes the diverse document ahead of the near-duplicate.

Request:
```json
POST /products_faiss/_search
{
  "retriever": {
    "diversify": {
      "vector_field": "embedding",
      "lambda": 0.1,
      "window_size": 10,
      "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0, 0.0], "k": 10 } } } } }
    }
  },
  "size": 3
}
```
Response order: **`[a, c, b]`**

**Verdict:** ✅ The near-duplicate `b` is demoted below the orthogonal `c`. With high diversity, after
picking `a` the algorithm penalizes `b` (almost identical to `a`) and prefers the dissimilar `c`.

### Test A2 — pure relevance (`lambda=1.0`)

**Proves:** at `lambda=1.0`, diversify is a no-op (identical to plain k-NN).

Request: same as A1 but `"lambda": 1.0`.
Response order: **`[a, b, c]`**

**Verdict:** ✅ Identical to the plain-k-NN baseline. `lambda=1.0` = diversity 0 = relevance only.

### Test A3 — works with `_source` disabled (the efficient vector pull)

**Proves:** diversify gets its vectors via a `docvalue_fields` ride-along, not from `_source`, so it
works even when the client disables `_source`.

Request:
```json
POST /products_faiss/_search
{
  "_source": false,
  "retriever": {
    "diversify": {
      "vector_field": "embedding", "lambda": 0.1, "window_size": 10,
      "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0, 0.0], "k": 10 } } } } }
    }
  },
  "size": 3
}
```
Response order: **`[a, c, b]`**, and **no hit carries a `_source`** field.

**Verdict:** ✅ Correct diversified order *and* no `_source` in the response — the vectors rode along on
the k-NN leg's own fetch. This is the efficiency win over the old pipeline-MMR approach (which forced a
full `_source` fetch + parse).

### Test A4 — explain output

**Proves:** when `explain:true`, each hit carries a human-readable MMR explanation.

Request: same as A1 plus `"explain": true`.
Response order: **`[a, c, b]`**. The first hit's explanation reads:
```
diversify: selected by MMR [lambda=0.1000, diversity=0.9000, max_similarity_to_selected=0.0000, mmr_score=0.1000]
```

**Verdict:** ✅ The explanation shows the MMR math. The first-picked document has
`max_similarity_to_selected = 0.0` (nothing selected yet to be similar to) — exactly right.

### Test A5 — validation: `vector_field` must be a `knn_vector`

**Proves:** pointing `vector_field` at a non-vector field fails cleanly, not silently.

Request: same as A1 but `"vector_field": "title"` (a `text` field).
Response:
```json
{ "error": { "type": "illegal_argument_exception",
  "reason": "MMR query extension cannot support non knn_vector field [products_faiss:title]." } }
```

**Verdict:** ✅ Clear, actionable error.

### Test A6 — per-leg under fusion (`rank_fusion[ diversify(knn), bm25 ]`)

**Proves:** diversify is **accepted and runs inside a `rank_fusion`** (not only at the top level) — the
"window-preserving" placement — and returns a complete fused window.

Request:
```json
POST /products_faiss/_search
{
  "retriever": {
    "rank_fusion": {
      "retrievers": [
        { "diversify": { "vector_field": "embedding", "lambda": 0.1, "window_size": 10,
            "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 10 } } } } } } },
        { "standard": { "query": { "match": { "title": "wireless" } } } }
      ],
      "rank_window_size": 10
    }
  },
  "size": 3
}
```
Response order: **`[a, b, c]`** (accepted, executed, full window, `a` first).

**Verdict:** ✅ The per-leg placement works end-to-end — the request is accepted (older behavior would
reject a reranker inside fusion) and produces a complete fused result. On this tiny 3-document corpus,
reciprocal-rank fusion blends the diversified vector leg with the BM25 leg and the two near-identical
docs `b`/`c` end up with near-equal fused scores, so the *visible* reordering is masked at this scale.
Part C shows the per-leg effect clearly on a larger corpus.

### Test A7 — same thing on the Lucene engine

**Proves:** diversify is engine-agnostic (not Faiss-specific).

Setup: an identical index `products_lucene` but `"engine":"lucene"`; same 3 docs. Request identical to
A1.
Response order: **`[a, c, b]`**

**Verdict:** ✅ Same diversified result on Lucene HNSW. Useful as a native-free fallback.

---

# Part B — the test matrix at a glance

| # | Scenario | Engine | Request `lambda` | Result | Verdict |
|---|----------|--------|------|--------|---------|
| A1 | top-level diversify | Faiss | 0.1 | `[a, c, b]` | ✅ diverse `c` promoted over near-dup `b` |
| A2 | pure relevance | Faiss | 1.0 | `[a, b, c]` | ✅ no-op, == plain k-NN |
| A3 | `_source:false` ride-along | Faiss | 0.1 | `[a, c, b]`, no `_source` | ✅ vectors via docvalue, not source |
| A4 | explain | Faiss | 0.1 | `[a, c, b]` + MMR explanation | ✅ MMR math shown, first maxSim=0 |
| A5 | validation (non-vector field) | Faiss | — | `illegal_argument_exception` | ✅ clean error |
| A6 | per-leg under `rank_fusion` | Faiss | 0.1 | `[a, b, c]`, accepted | ✅ runs inside fusion, full window |
| A7 | top-level diversify | Lucene | 0.1 | `[a, c, b]` | ✅ engine-agnostic |

All seven ran against the live 3-node cluster; all three node JVMs stayed up throughout.

---

# Part C — the live-demo use case (RAG near-duplicate dedup)

This is the scenario to show in a live demo. It makes the value obvious in one screen.

### The setup

A help-center knowledge base. The index `rag_demo` (3 shards, 0 replicas, **4-D** `knn_vector`
`embedding`, plus `title` + `passage` text) holds **10 passages**:

- **Five near-duplicates** about resetting a password (`reset1`…`reset5`) — all clustered tightly near
  the vector `[1,0,0,0]`. They say the same thing five ways.
- **Five distinct-but-related** passages, each on its own axis: `twofa` (two-factor auth), `lockout`
  (account lockout), `changepw` (change password while signed in), `phishing` (phishing safety),
  `manager` (password managers).

The user's question is **"how do I reset my password?"** → query vector `[1, 0, 0, 0]`.

### C1 — BASELINE: plain k-NN (what RAG retrieves **without** diversify)

Request:
```json
POST /rag_demo/_search
{ "size": 5, "_source": ["title"],
  "query": { "knn": { "embedding": { "vector": [1.0, 0.0, 0.0, 0.0], "k": 10 } } } }
```
Actual response (id · score · title):
```
1. reset1   1.0000  Reset your password
2. reset2   0.9998  How to reset a forgotten password
3. reset4   0.9994  Cannot sign in - reset password
4. reset3   0.9992  Password reset steps
5. reset5   0.9992  Forgot password help
```

**Verdict:** 🔴 This is the RAG problem in one screen. All five results are the **same answer restated**.
Notice the scores (`1.0000, 0.9998, 0.9994, …`) are nearly identical — that is *why* the near-duplicates
dominate. Feed this to an LLM and five of its context slots are wasted on one point; the user never sees
2FA, lockout, or phishing guidance.

### C2 — DIVERSIFY (`lambda=0.3`) — what RAG retrieves **with** diversify

Request:
```json
POST /rag_demo/_search
{ "size": 5, "_source": ["title"],
  "retriever": { "diversify": {
    "vector_field": "embedding", "lambda": 0.3, "window_size": 10,
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 10 } } } } }
  } } }
```
Actual response (id · score · title):
```
1. reset1    10.0  Reset your password
2. phishing   9.0  Avoid password phishing scams
3. lockout    8.0  Account locked after failed logins
4. twofa      7.0  Set up two-factor authentication
5. changepw   6.0  Change your password while signed in
```

**Verdict:** 🟢 One best "reset" answer, then four **genuinely different and still relevant** topics.
The four redundant reset restatements are gone. An LLM given this context can write a far more complete
answer. (The scores `10,9,8,…` are synthetic ranks the retriever assigns to carry its chosen order
through the response — the framework sorts hits by `_score`.)

### Side-by-side

| rank | C1 baseline (plain k-NN) | C2 diversify (λ=0.3) |
|------|--------------------------|----------------------|
| 1 | Reset your password | Reset your password |
| 2 | How to reset a forgotten password *(dup)* | **Avoid password phishing scams** |
| 3 | Cannot sign in - reset password *(dup)* | **Account locked after failed logins** |
| 4 | Password reset steps *(dup)* | **Set up two-factor authentication** |
| 5 | Forgot password help *(dup)* | **Change your password while signed in** |

### C3 — the `lambda` knob (relevance ↔ diversity dial)

Same query, `size=5`, sweeping `lambda`:

| `lambda` | result (ids) | reading |
|----------|--------------|---------|
| `1.0` | `reset1, reset2, reset4, reset3, reset5` | pure relevance = baseline (all resets) |
| `0.7` | `reset1, reset2, reset4, reset3, reset5` | still relevance-dominated |
| `0.5` | `reset1, reset2, reset4, reset5, reset3` | diversity starts to re-order |
| `0.3` | `reset1, phishing, lockout, twofa, changepw` | balanced — 1 reset + 4 distinct topics |
| `0.0` | `reset1, phishing, lockout, twofa, changepw` | max diversity |

**Verdict:** 🟢 One number smoothly tunes the result from "all near-duplicates" to "maximally varied."
`0.3` is a good default for RAG dedup.

### C4 — per-leg diversify inside hybrid search (now clearly visible)

Hybrid retrieval: fuse a vector leg with a BM25 keyword leg. We diversify **only the vector leg**,
before fusion.

Request (diversified vector leg):
```json
POST /rag_demo/_search
{ "size": 5, "_source": ["title"],
  "retriever": { "rank_fusion": { "rank_window_size": 10, "retrievers": [
    { "diversify": { "vector_field": "embedding", "lambda": 0.2, "window_size": 10,
        "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 10 } } } } } } },
    { "standard": { "query": { "match": { "passage": "password" } } } }
  ] } } }
```

Result **with diversify on the vector leg** vs. the **same fusion using a plain k-NN leg**:

| rank | fusion + **plain** k-NN leg | fusion + **diversify** k-NN leg (λ=0.2) |
|------|-----------------------------|------------------------------------------|
| 1 | Reset your password | Reset your password |
| 2 | How to reset a forgotten password *(dup)* | **Use a password manager** |
| 3 | Cannot sign in - reset password *(dup)* | **Avoid password phishing scams** |
| 4 | Password reset steps *(dup)* | **Change your password while signed in** |
| 5 | Use a password manager | How to reset a forgotten password |

**Verdict:** 🟢 Diversifying the vector leg *before* fusion visibly changes the hybrid result — the
near-duplicate resets are pushed down and distinct topics surface, while the keyword leg still
contributes. This is the per-leg placement (Test A6) shown with a visible effect.

---

## Reproduce it yourself

1. **Build the distribution** (from the three forks on `retriever-framework-3.7`):
   ```bash
   # in the ci-runner Docker image, JDK 21:
   ./build.sh manifests/3.7.0/opensearch-3.7.0.yml \
     --component OpenSearch common-utils job-scheduler opensearch-remote-metadata-sdk \
                 security custom-codecs k-NN ml-commons neural-search \
     -a x64 -d tar -s
   ./assemble.sh tar/builds/opensearch/manifest.yml
   # → tar/dist/opensearch/opensearch-3.7.0-SNAPSHOT-linux-x64.tar.gz
   ```
2. **Start 3 nodes** from the tarball. Shared `cluster.name`, unique `node.name`/ports,
   `discovery.seed_hosts` listing all three transport ports, `plugins.security.disabled: true`, and —
   critically — launch each node with
   `LD_LIBRARY_PATH=<node>/plugins/opensearch-knn/lib:$LD_LIBRARY_PATH` so Faiss loads. (See the three
   gotchas above.)
3. **Create a `knn_vector` index** using the nested `settings.index` form, index your corpus, and run
   the requests in Parts A and C. Every exact request body is saved under `results/*.req.json` and the
   responses under `results/*.resp.json`.

## Known limitations (v1)

- `vector_field` must resolve to a **`knn_vector`** field in every target index (else a validation
  error — Test A5).
- The newer **`semantic` field type** (which stores its vector at a nested path) is **not** supported as
  `vector_field` in v1 — point `diversify` at a directly-mapped `knn_vector` field instead.
- Under `rank_fusion`, diversify preserves the leg's window size (it reorders within the window rather
  than shrinking it), so the fused window stays complete; the visible effect depends on the fusion math
  and corpus (Tests A6 vs C4).

---

# Part D — the rest of the retriever framework (same live cluster)

The distribution we built contains the **whole retriever framework**, not just `diversify`. These
tests exercise the other registered retrievers and features on the same 3-node cluster, using the same
`rag_demo` corpus from Part C (10 passages: 5 near-duplicate "reset password" + 5 distinct topics).
Every request/response is saved under `results/D*.req.json` / `results/D*.resp.json`.

## D1 — `rank_fusion` standalone (hybrid BM25 + vector)

**Proves:** `rank_fusion` blends a keyword leg and a vector leg with Reciprocal Rank Fusion (RRF).

Request:
```json
POST /rag_demo/_search
{ "size": 5, "_source": ["title"],
  "retriever": { "rank_fusion": { "rank_window_size": 10, "retrievers": [
    { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 10 } } } } },
    { "standard": { "query": { "match": { "passage": "two-factor security" } } } }
  ] } } }
```
Response order: **`[twofa, changepw, reset1, reset2, reset4]`**

**Verdict:** ✅ The BM25 leg (keywords "two-factor security") lifts `twofa` and `changepw` to the top;
the vector leg contributes the password-reset cluster. RRF fuses the two rankings — a hybrid result
neither leg would produce alone.

## D2 — `score_fusion` (score normalization + weighted combination)

**Proves:** `score_fusion` normalizes each leg's scores and combines them (here a weighted mean), so you
rank on blended *scores* rather than ranks.

> **API gotcha (important):** `weights` is **not** a top-level field. It lives inside `combination`:
> `"combination": { "technique": "arithmetic_mean", "parameters": { "weights": [...] } }`. Putting
> `weights` at the top level fails with *"[score_fusion] unknown field [weights]"*.

Request (keyword leg weighted 2×):
```json
POST /rag_demo/_search
{ "size": 5, "_source": ["title"],
  "retriever": { "score_fusion": {
    "normalization": "min_max",
    "combination": { "technique": "arithmetic_mean", "parameters": { "weights": [1.0, 2.0] } },
    "rank_window_size": 10,
    "retrievers": [
      { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 10 } } } } },
      { "standard": { "query": { "match": { "passage": "two-factor security" } } } }
    ] } } }
```
Response (id · score):
```
twofa   0.7483   reset1  0.3333   reset2  0.3332   reset4  0.3329   reset3  0.3328
```

**Verdict:** ✅ `twofa` wins with a high **normalized** blended score (strong keyword match × 2 weight),
and the reset cluster follows at ~0.333. Contrast with D1: `rank_fusion` combines *ranks* (RRF), while
`score_fusion` combines *normalized scores* — note the real fractional scores here vs. rank-derived
ones.

## D3 — `pin` (force curated documents to the top)

**Proves:** `pin` places chosen document ids at the top in the given order, then lets the child
retriever fill the rest — the merchandising / editorial-curation use case.

Request (pin `phishing` and `manager` above an organic k-NN ranking):
```json
POST /rag_demo/_search
{ "size": 5, "_source": ["title"],
  "retriever": { "pin": {
    "ids": ["phishing", "manager"],
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 10 } } } } }
  } } }
```
Response order: **`[phishing, manager, reset1, reset2, reset4]`**
(Plain k-NN for the same query is `[reset1, reset2, reset4, reset3, reset5]`.)

**Verdict:** ✅ `phishing` and `manager` are pinned to positions 1–2 in the requested order; the organic
k-NN results fill 3–5. Pinned docs did not have to be top k-NN matches.

## D4a — retriever `profile` (tree-structured timings)

**Proves:** `profile:true` returns a tree of per-retriever timings that reconcile parent↔child.

Request: a `profile:true` diversify query (same shape as C2). The response's `profile` section:
```json
"profile": {
  "self_resolve": {
    "total_time_in_nanos": 37091203,
    "breakdown": { "coordinator_overhead": 1099749 },
    "retriever": {
      "type": "diversify",
      "total_time_in_nanos": 35991454,
      "breakdown": { "orchestration_overhead": 461257 },
      "children": [
        { "type": "standard", "total_time_in_nanos": 35530197,
          "shards": [ { "id": "[...][rag_demo][1]",
            "searches": [ { "query": [ { "type": "KNNQuery", "time_in_nanos": 4043412,
              "max_slice_time_in_nanos": 2684503, ... } ] } ] } ] } ] } } }
```

**Verdict:** ✅ The profile shows the full tree: the `diversify` node's `orchestration_overhead`, its
`standard` child, and the child's per-shard `KNNQuery` timings (down to slice level). This is how you'd
diagnose where time goes in a composed retriever.

## D4b — pagination (`from` / `size` over a diversified result)

**Proves:** `from`/`size` page stably over the retriever result.

Two requests over the same diversify query (λ=0.3, whose full order is
`[reset1, phishing, lockout, twofa, changepw]`):
- `{"from":0,"size":2, ...}` → **`[reset1, phishing]`**
- `{"from":2,"size":2, ...}` → **`[lockout, twofa]`**

**Verdict:** ✅ The two pages stitch together into the stable full order with no overlap or gap —
pagination over the diversified window is consistent.

## D5 / D6 — `neural` leg under `diversify` (model-backed, end-to-end)

**Proves:** `diversify` works over a **`neural`** query leg — the embedding is produced by an ML model
at query time, and `diversify` resolves the vector field's type **from the model** and runs MMR over
the model-generated vectors. (This path was previously exercised only in CI; here it runs live.)

**Setup:** a text-embedding model (`traced_small_model`, TORCH_SCRIPT, 768-d) was registered and
deployed via ml-commons, and an index `neural_demo` was created with a `text_embedding` ingest pipeline
(`passage` → `passage_knn`, a 768-d Lucene `knn_vector`). Seven passages were indexed (four near-dup
"reset password" + `twofa`, `lockout`, `phishing`); the model embedded them at ingest.

**D5 — baseline `neural` query** `query_text: "how do I reset my password"`:
```json
POST /neural_demo/_search
{ "size": 5, "_source": ["title"],
  "query": { "neural": { "passage_knn": { "query_text": "how do I reset my password",
    "model_id": "<model_id>", "k": 10 } } } }
```
→ `[twofa, r1, phishing, r3, lockout]`

**D6 — `neural` leg under `diversify`** (λ=0.2):
```json
POST /neural_demo/_search
{ "size": 5, "_source": ["title"],
  "retriever": { "diversify": { "vector_field": "passage_knn", "lambda": 0.2, "window_size": 10,
    "retriever": { "standard": { "query": { "neural": { "passage_knn": {
      "query_text": "how do I reset my password", "model_id": "<model_id>", "k": 10 } } } } } } } }
```
→ `[twofa, r2, lockout, phishing, r4]`. With `explain:true` the top hit shows:
```
diversify: selected by MMR [lambda=0.2000, diversity=0.8000, max_similarity_to_selected=0.0000, mmr_score=0.0379]
```

**Verdict:** ✅ (integration) The `neural → diversify` path runs end-to-end: the model deploys, embeds
at ingest, the `neural` query produces vectors, and `diversify` resolves the field info from the model
and applies MMR (confirmed by the explain output over the neural leg).

> **Honest caveat on the demo quality.** `traced_small_model` is a tiny 6-layer **test** BERT, not a
> production embedding model, so its vectors are semantically weak — the baseline `neural` ranking is
> already mixed (`twofa` on top, not the clean reset pileup we see with hand-crafted vectors). So D5/D6
> prove the **neural integration** works, but the **diversification effect** is demonstrated far more
> clearly by the hand-crafted-vector demo in Part C. For a live demo, show Part C for the "wow", and
> cite D5/D6 as proof the model-backed path also works.

## Part D summary

| # | Feature | Request → Result | Verdict |
|---|---------|------------------|---------|
| D1 | `rank_fusion` (RRF hybrid) | BM25 + k-NN → `[twofa, changepw, reset1, reset2, reset4]` | ✅ blends keyword + vector |
| D2 | `score_fusion` (normalized, weighted) | → `twofa 0.75`, resets ~0.33 | ✅ normalized blended scores |
| D3 | `pin` | pin `phishing,manager` → `[phishing, manager, reset1, reset2, reset4]` | ✅ curated docs forced to top |
| D4a | `profile` | tree: diversify → standard → per-shard KNNQuery timings | ✅ reconciling timings |
| D4b | pagination | `[reset1,phishing]` + `[lockout,twofa]` | ✅ stable paging |
| D5/D6 | `neural` → `diversify` | model-backed leg, MMR over model vectors | ✅ integration (toy model → weak semantics) |

Every Part D test ran on the same green 3-node cluster; all three node JVMs stayed up throughout.
