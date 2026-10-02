# Retriever Framework Playground — live public cluster

A hands-on guide to the **public demo cluster** running our OpenSearch 3.7 retriever-framework build.
Everything below was executed against that live cluster; the example outputs are **real responses**, not
illustrations.

- **Account / region:** `882640845411` / `us-east-1` · **Cluster:** `opensearch-infra-stack-882640845411-us-east-1`
- **Topology:** 3 nodes (1 cluster-manager+data seed, 2 data), security **enabled**, public (no VPN).
- **Build:** core `3.7.0-SNAPSHOT` from our forks (diversify retriever) + k-NN (Faiss/Lucene) + neural-search + ml-commons.

> ⚠️ **Demo cluster, not production.** Security is on, but the endpoint is open to the internet and
> protected only by one shared admin password, with self-signed TLS certs. Don't put sensitive data in
> it. Plan to tear it down after the demo.

---

## 1. Access

| What | Endpoint | Notes |
|------|----------|-------|
| **Dashboards UI** | `http://opense-clust-kso2xU47kUhX-3d5921f2290360f2.elb.us-east-1.amazonaws.com:8443` | **HTTP**, port **8443**. Log in with the credentials below. |
| **REST API** | `https://opense-clust-kso2xU47kUhX-3d5921f2290360f2.elb.us-east-1.amazonaws.com:443` | **HTTPS**, port **443**, self-signed cert (use `curl -k`). |

**Credentials:** `admin` / `Diversify_Demo_2026!`

> The load balancer maps **:443 → OpenSearch (9200)** and **:8443 → Dashboards (5601)**. Those are the
> only two ports you use. The self-signed cert will trigger a browser warning on the API — expected.

### Quick connectivity check
```bash
LB=opense-clust-kso2xU47kUhX-3d5921f2290360f2.elb.us-east-1.amazonaws.com

# REST API — cluster health (should be green, 3 nodes)
curl -s -k -u admin:'Diversify_Demo_2026!' "https://$LB:443/_cat/health?v"

# Dashboards — status (needs auth)
curl -s -u admin:'Diversify_Demo_2026!' "http://$LB:8443/api/status" | head -c 200
```
Real output of the health call:
```
status green  node.total 3  node.data 3  active_shards_percent 100.0%
```

### Using Dashboards Dev Tools (easiest way to try queries)
1. Open the Dashboards URL, log in as `admin`.
2. Left menu → **Management → Dev Tools**.
3. Paste any `GET <index>/_search { ... }` from this doc (drop the `curl`/host wrapper — Dev Tools adds it).

All `curl` examples below assume:
```bash
LB=opense-clust-kso2xU47kUhX-3d5921f2290360f2.elb.us-east-1.amazonaws.com
C="curl -s -k -u admin:Diversify_Demo_2026! -H Content-Type:application/json"
```

---

## 2. What the retriever framework is

A **retriever** goes under the top-level `"retriever"` key in `_search` and describes *how* to fetch and
rank results. Retrievers **compose** into a tree:

- **leaf** — `standard` (wraps any query, e.g. `match`, `knn`, `neural`)
- **fusion** — `rank_fusion` (RRF), `score_fusion` (normalized scores) combine multiple child legs
- **re-ranker** — `pin` (curate), `diversify` (relevance + diversity via MMR) wrap a single child

| Retriever | Kind | One-liner |
|-----------|------|-----------|
| `standard` | leaf | run any query as a retriever |
| `rank_fusion` | fusion | hybrid by rank (Reciprocal Rank Fusion) |
| `score_fusion` | fusion | hybrid by normalized, weighted score |
| `pin` | re-ranker | force chosen doc ids to the top |
| `diversify` | re-ranker | de-duplicate / diversify with MMR |

Cross-cutting: `explain`, `profile`, and `from`/`size` pagination work on any retriever tree.

---

## 3. Set up the demo data

Two indexes are already loaded on the live cluster. If you want to recreate them (or set up your own
cluster), here are the exact steps.

### 3a. `products` — tiny 2-D vector index (for crisp, hand-checkable examples)

Three documents: `a` is the top match, `b` is a **near-duplicate** of `a`, `c` is **orthogonal**
(diverse).

```bash
$C -X PUT "https://$LB:443/products" -d '{
  "settings": { "index": { "knn": true, "number_of_shards": 3, "number_of_replicas": 1 } },
  "mappings": { "properties": {
    "embedding": { "type": "knn_vector", "dimension": 2,
      "method": { "name": "hnsw", "space_type": "l2", "engine": "faiss" } },
    "title": { "type": "text" } } }
}'

$C -X POST "https://$LB:443/products/_bulk?refresh=true" -d '
{"index":{"_id":"a"}}
{"embedding":[1.00,0.00],"title":"wireless noise cancelling headphones"}
{"index":{"_id":"b"}}
{"embedding":[0.98,0.02],"title":"wireless noise-cancelling headphone"}
{"index":{"_id":"c"}}
{"embedding":[0.00,1.00],"title":"stainless steel water bottle"}
'
```

> **Gotcha:** use the **nested** `settings.index` form shown above. The flat `"index.knn": true` form is
> silently ignored and your field ends up as a plain array, not a `knn_vector` — you'll later see
> *"Field 'embedding' is not knn_vector type."* Verify with:
> `$C "https://$LB:443/products/_mapping" | grep knn_vector`

### 3b. `rag_demo` — richer RAG-style corpus (10 passages, 4-D vectors)

Models a help-center knowledge base: **five near-duplicate "password reset" passages** (`reset1`–`reset5`,
clustered near `[1,0,0,0]`) plus five **distinct-but-related** topics (`twofa`, `lockout`, `changepw`,
`phishing`, `manager`). Hand-crafted 4-D vectors so the diversification effect is obvious.

```bash
$C -X PUT "https://$LB:443/rag_demo" -d '{
  "settings": { "index": { "knn": true, "number_of_shards": 3, "number_of_replicas": 1 } },
  "mappings": { "properties": {
    "embedding": { "type": "knn_vector", "dimension": 4,
      "method": { "name": "hnsw", "space_type": "l2", "engine": "faiss" } },
    "title": { "type": "text" }, "passage": { "type": "text" } } }
}'

$C -X POST "https://$LB:443/rag_demo/_bulk?refresh=true" -d '
{"index":{"_id":"reset1"}}
{"embedding":[1.00,0.00,0.00,0.00],"title":"Reset your password","passage":"To reset your password, click Forgot Password on the login page and follow the emailed link."}
{"index":{"_id":"reset2"}}
{"embedding":[0.99,0.01,0.00,0.00],"title":"How to reset a forgotten password","passage":"If you forgot your password, use the Forgot Password link on the sign-in screen to receive a reset email."}
{"index":{"_id":"reset3"}}
{"embedding":[0.98,0.02,0.00,0.00],"title":"Password reset steps","passage":"Resetting your password: open the login page, choose Forgot Password, and open the link we email you."}
{"index":{"_id":"reset4"}}
{"embedding":[0.985,0.00,0.02,0.00],"title":"Cannot sign in reset password","passage":"Cannot sign in? Reset the password via the Forgot Password link sent to your registered email."}
{"index":{"_id":"reset5"}}
{"embedding":[0.975,0.015,0.00,0.00],"title":"Forgot password help","passage":"Use Forgot Password at sign-in; we will email you a secure link to set a new password."}
{"index":{"_id":"twofa"}}
{"embedding":[0.72,0.69,0.05,0.00],"title":"Set up two-factor authentication","passage":"Enable 2FA from Security Settings to add a one-time code on top of your password at login."}
{"index":{"_id":"lockout"}}
{"embedding":[0.70,0.00,0.71,0.00],"title":"Account locked after failed logins","passage":"After five failed sign-in attempts your account locks for 30 minutes; wait or contact support to unlock."}
{"index":{"_id":"changepw"}}
{"embedding":[0.80,0.00,0.00,0.60],"title":"Change your password while signed in","passage":"Already signed in? Change your password under Profile Security without needing a reset email."}
{"index":{"_id":"phishing"}}
{"embedding":[0.40,0.30,0.30,0.60],"title":"Avoid password phishing scams","passage":"We never ask for your password by email. Only enter it on the official login page to avoid phishing."}
{"index":{"_id":"manager"}}
{"embedding":[0.50,0.50,0.50,0.30],"title":"Use a password manager","passage":"A password manager generates and stores strong unique passwords so you do not reuse them across sites."}
'
```

The query vector for all `rag_demo` examples is `[1,0,0,0]` = **"password reset" intent**.

---

## 4. Testing each retriever (real input → real output)

### 4.1 `standard` + plain k-NN (the baseline)

Request:
```bash
$C "https://$LB:443/products/_search" -d '{
  "size": 3, "_source": false,
  "query": { "knn": { "embedding": { "vector": [1.0, 0.0], "k": 3 } } } }'
```
Real result (id · score): `a 1.0 · b 0.9992 · c 0.3333` → order **`[a, b, c]`**.

**Read:** the near-duplicate `b` (score 0.9992, almost identical to `a`) sits at #2; the diverse `c` is
last. This redundancy is what `diversify` fixes.

### 4.2 `diversify` — relevance + diversity (MMR)

Request (high diversity, `lambda=0.1`):
```bash
$C "https://$LB:443/products/_search" -d '{
  "size": 3,
  "retriever": { "diversify": {
    "vector_field": "embedding", "lambda": 0.1, "window_size": 10,
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0], "k": 10 } } } } }
  } } }'
```
Real result: **`[a, c, b]`** — the diverse `c` is promoted above the near-duplicate `b`.

`lambda` sweep (same query), **real results**:

| `lambda` | meaning | result |
|----------|---------|--------|
| `1.0` | pure relevance (no-op) | `[a, b, c]` |
| `0.1` | high diversity | `[a, c, b]` |

**Parameters:**

| Param | Required | Default | Meaning |
|-------|----------|---------|---------|
| `retriever` | yes | — | child to diversify (any subtree; usually `standard` over `knn`/`neural`) |
| `vector_field` | yes | — | the `knn_vector` field whose vectors drive diversity |
| `lambda` | no | `0.5` | `1.0` = pure relevance, `0.0` = max diversity. `~0.3` good for RAG dedup |
| `window_size` | no | `100` | how many top candidates to re-rank |
| `space_type` / `vector_data_type` | no | from mapping | override if needed |

> Works with `"_source": false` — diversify pulls vectors via a `docvalue_fields` ride-along, not from
> `_source`.

### 4.3 The RAG dedup story (the one to demo)

Same query (`[1,0,0,0]` = "password reset"), `size=5`, on `rag_demo`.

**Baseline — plain k-NN** (`GET rag_demo/_search {"query":{"knn":...}}`), real result (id · score):
```
reset1 1.0000 · reset2 0.9998 · reset4 0.9994 · reset3 0.9992 · reset5 0.9992
```
→ **all five hits are the same answer restated** (scores nearly identical). For RAG this wastes the LLM
context window.

**With `diversify` (`lambda=0.3`)** — real result (id · synthetic score):
```
reset1 10.0 · phishing 9.0 · lockout 8.0 · twofa 7.0 · changepw 6.0
```

| rank | baseline (plain k-NN) | diversify (λ=0.3) |
|------|-----------------------|-------------------|
| 1 | Reset your password | Reset your password |
| 2 | How to reset a forgotten password *(dup)* | **Avoid password phishing scams** |
| 3 | Cannot sign in - reset password *(dup)* | **Account locked after failed logins** |
| 4 | Password reset steps *(dup)* | **Set up two-factor authentication** |
| 5 | Forgot password help *(dup)* | **Change your password while signed in** |

**Read:** one best "reset" answer, then four genuinely different, still-relevant topics. (The `10,9,8,…`
are synthetic ranks the retriever assigns so the response — which the engine sorts by `_score` — comes
back in MMR order.)

Request for the diversify version:
```bash
$C "https://$LB:443/rag_demo/_search" -d '{
  "size": 5, "_source": ["title"],
  "retriever": { "diversify": {
    "vector_field": "embedding", "lambda": 0.3, "window_size": 10,
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 10 } } } } }
  } } }'
```

### 4.4 `rank_fusion` — hybrid by rank (RRF)

Fuse a vector leg and a keyword leg. Request:
```bash
$C "https://$LB:443/rag_demo/_search" -d '{
  "size": 5, "_source": ["title"],
  "retriever": { "rank_fusion": { "rank_window_size": 100, "retrievers": [
    { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 100 } } } } },
    { "standard": { "query": { "match": { "passage": "two-factor security" } } } }
  ] } } }'
```
Real result: **`[twofa, changepw, reset1, reset2, reset4]`** — the BM25 leg (keywords "two-factor
security") lifts `twofa`/`changepw`; the vector leg contributes the reset cluster; RRF blends them.

### 4.5 `score_fusion` — hybrid by normalized, weighted score

Request (keyword leg weighted 2×):
```bash
$C "https://$LB:443/rag_demo/_search" -d '{
  "size": 5, "_source": ["title"],
  "retriever": { "score_fusion": {
    "normalization": "min_max",
    "combination": { "technique": "arithmetic_mean", "parameters": { "weights": [1.0, 2.0] } },
    "rank_window_size": 100,
    "retrievers": [
      { "standard": { "query": { "knn":   { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 100 } } } } },
      { "standard": { "query": { "match": { "passage": "two-factor security" } } } }
    ] } } }'
```
Real result (id · score): `twofa 0.7483 · reset1 0.3333 · reset2 0.3332 · reset4 0.3329 · reset3 0.3328`.

> **Gotcha:** `weights` is **not** a top-level field — it nests under `combination.parameters`. Putting it
> at the top level fails with *"[score_fusion] unknown field [weights]"*.

`rank_fusion` combines *ranks* (scale-free); `score_fusion` combines *normalized scores* (lets you weight
legs, keeps score magnitude).

### 4.6 `pin` — force curated docs to the top

Request (pin `phishing` and `manager` above an organic k-NN ranking):
```bash
$C "https://$LB:443/rag_demo/_search" -d '{
  "size": 5, "_source": ["title"],
  "retriever": { "pin": {
    "ids": ["phishing", "manager"],
    "retriever": { "standard": { "query": { "knn": { "embedding": { "vector": [1.0,0.0,0.0,0.0], "k": 100 } } } } }
  } } }'
```
Real result: **`[phishing, manager, reset1, reset2, reset4]`** — the two pinned docs occupy #1–2 (in the
order given), then the organic k-NN results fill the rest.

### 4.7 `explain`

Add `"explain": true` to any retriever search. For the `rag_demo` diversify (λ=0.3) query, the top hit's
explanation is (real):
```
diversify: selected by MMR [lambda=0.3000, diversity=0.7000, max_similarity_to_selected=0.0000, mmr_score=0.3000]
```
The first-selected doc has `max_similarity_to_selected = 0.0` (nothing selected yet) — exactly right.

### 4.8 `profile`

Add `"profile": true`. The response gains a `profile` section with keys
`self_resolve`, `rank_docs_query`, `total_time_in_nanos`. The `self_resolve.retriever` tree shows the
node types and reconciling timings — for the diversify query, **real**: root `type: diversify` → child
`type: standard` → per-shard `KNNQuery` timings.

### 4.9 Pagination (`from` / `size`)

Over the diversify (λ=0.3) query whose full order is `[reset1, phishing, lockout, twofa, changepw]`:

| request | real result |
|---------|-------------|
| `{"from":0,"size":2, ...}` | `[reset1, phishing]` |
| `{"from":2,"size":2, ...}` | `[lockout, twofa]` |

The pages stitch together with no overlap or gap.

### 4.10 Validation error (what a wrong `vector_field` looks like)

Pointing `diversify.vector_field` at a `text` field (real response on this cluster):
```json
{ "error": { "type": "illegal_argument_exception",
  "reason": "Field [title] of type [text] does not support custom formats" } }
```
→ a 400. (The exact message depends on where the mismatch is caught; the point is a non-`knn_vector`
field is rejected up front, not silently.)

---

## 5. Neural (model-backed) search — setup + test

The build includes neural-search + ml-commons, so a `standard` leg can run a `neural` query (embeddings
generated by an ML model at query time), and you can wrap it in any retriever (e.g. `diversify`).

### 5a. Register + deploy a text-embedding model

```bash
# allow URL model registration (demo settings)
$C -X PUT "https://$LB:443/_cluster/settings" -d '{"persistent":{
  "plugins.ml_commons.allow_registering_model_via_url": true,
  "plugins.ml_commons.only_run_on_ml_node": false,
  "plugins.ml_commons.model_access_control_enabled": false }}'

# register a model group
$C -X POST "https://$LB:443/_plugins/_ml/model_groups/_register" \
  -d '{"name":"retriever_demo_group","description":"demo"}'
#  -> { "model_group_id": "<GID>" }

# register the model (a small traced test model) — returns a task_id
$C -X POST "https://$LB:443/_plugins/_ml/models/_register" -d '{
  "name":"huggingface/sentence-transformers/traced_small_model","version":"1.0.0",
  "model_group_id":"<GID>","model_format":"TORCH_SCRIPT","function_name":"TEXT_EMBEDDING",
  "model_content_hash_value":"e13b74006290a9d0f58c1376f9629d4ebc05a0f9385f40db837452b167ae9021",
  "model_config":{"model_type":"bert","embedding_dimension":768,"framework_type":"sentence_transformers",
    "all_config":"{\"architectures\":[\"BertModel\"],\"max_position_embeddings\":512,\"model_type\":\"bert\",\"num_attention_heads\":12,\"num_hidden_layers\":6}"},
  "url":"https://github.com/opensearch-project/ml-commons/raw/2.x/ml-algorithms/src/test/resources/org/opensearch/ml/engine/algorithms/text_embedding/traced_small_model.zip" }'

# poll the register task until COMPLETED to get the model_id
$C "https://$LB:443/_plugins/_ml/tasks/<task_id>"        # -> { "state":"COMPLETED", "model_id":"<MID>" }

# deploy the model (also returns a task_id to poll)
$C -X POST "https://$LB:443/_plugins/_ml/models/<MID>/_deploy"
$C "https://$LB:443/_plugins/_ml/models/<MID>"           # -> "model_state":"DEPLOYED"
```

A model is already deployed on the live cluster: **model_id `1GTr_qABYhi8FDw-UVK8`**.

### 5b. Ingest pipeline + neural index

```bash
# pipeline: embed `passage` text into a `passage_knn` vector field
$C -X PUT "https://$LB:443/_ingest/pipeline/embed_pipeline" -d '{
  "description":"text embedding",
  "processors":[{"text_embedding":{"model_id":"<MID>","field_map":{"passage":"passage_knn"}}}]}'

# index with a 768-d knn_vector + the pipeline as default
$C -X PUT "https://$LB:443/neural_demo" -d '{
  "settings":{"index":{"knn":true,"number_of_shards":3,"number_of_replicas":1,"default_pipeline":"embed_pipeline"}},
  "mappings":{"properties":{
    "passage_knn":{"type":"knn_vector","dimension":768,"method":{"name":"hnsw","space_type":"l2","engine":"lucene"}},
    "passage":{"type":"text"},"title":{"type":"text"}}}}'

# index text; the model embeds it automatically at ingest
$C -X POST "https://$LB:443/neural_demo/_bulk?refresh=true" -d '
{"index":{"_id":"r1"}}
{"title":"Reset your password","passage":"To reset your password, click Forgot Password on the login page and follow the emailed link."}
... (more docs) ...
'
```

### 5c. Query: `neural`, and `neural` under `diversify`

```bash
# plain neural
$C "https://$LB:443/neural_demo/_search" -d '{
  "size": 5, "_source": ["title"],
  "query": { "neural": { "passage_knn": {
    "query_text": "how do I reset my password", "model_id": "<MID>", "k": 10 } } } }'

# neural leg under diversify
$C "https://$LB:443/neural_demo/_search" -d '{
  "size": 5, "_source": ["title"],
  "retriever": { "diversify": { "vector_field": "passage_knn", "lambda": 0.2, "window_size": 10,
    "retriever": { "standard": { "query": { "neural": { "passage_knn": {
      "query_text": "how do I reset my password", "model_id": "<MID>", "k": 10 } } } } } } } }'
```
Real results on the live cluster:
- plain neural → `[twofa, phishing, r1, r3, lockout]`
- neural under diversify (λ=0.2) → `[twofa, r2, phishing, lockout, r1]`

> **Caveat:** `traced_small_model` is a tiny 6-layer **test** model — its embeddings are semantically
> weak, so the baseline neural ranking is already mixed (not the clean reset pileup you get with the
> hand-crafted vectors in `rag_demo`). These two queries prove the **neural integration path** works
> end-to-end (model embeds at ingest + query, diversify resolves the field info from the model and runs
> MMR); for a crisp *diversification* demo, use the `rag_demo` examples in §4.3.
>
> Point `vector_field` at a **directly-mapped `knn_vector`** field. The newer `semantic` field type
> (nested vector path) is **not** supported as `vector_field` in v1.

---

## 6. Cheat sheet

```bash
LB=opense-clust-kso2xU47kUhX-3d5921f2290360f2.elb.us-east-1.amazonaws.com
C="curl -s -k -u admin:Diversify_Demo_2026! -H Content-Type:application/json"

$C "https://$LB:443/_cat/health?v"                 # cluster health
$C "https://$LB:443/_cat/indices?v"                # indices
$C "https://$LB:443/_cat/plugins?v" | grep -E 'knn|neural'   # confirm plugins
$C "https://$LB:443/_plugins/_ml/models/_search" -d '{"query":{"match_all":{}}}'  # models
```

| Index | Dim | Engine | Purpose |
|-------|-----|--------|---------|
| `products` | 2 | faiss | tiny hand-checkable diversify example |
| `rag_demo` | 4 | faiss | RAG near-duplicate dedup story (§4.3) |
| `neural_demo` | 768 | lucene | model-backed neural search (§5) |

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `curl` to `:9200`/`:5601` times out | wrong LB ports | use **:443** (API) and **:8443** (Dashboards) |
| TLS error on the API | self-signed demo cert | use `curl -k` / accept the browser warning |
| `Field 'embedding' is not knn_vector type` | flat `"index.knn":true` settings form | use nested `"settings":{"index":{"knn":true}}` |
| `cluster create-index blocked (api)` | disk watermark | `PUT _cluster/settings {"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false}}` |
| `[score_fusion] unknown field [weights]` | `weights` at top level | nest under `combination.parameters.weights` |
| `diversify` field error | `vector_field` not a `knn_vector` | point it at the vector field |

## 8. Teardown

The cluster is CDK-managed (stacks `opensearch-network-stack` + `opensearch-infra-stack` in
`882640845411`/us-east-1). To destroy it when the demo is done:
```bash
cd opensearch-cluster-cdk
cdk destroy "*" --force   # with the same --context flags used at deploy
```
