---
description: "Databricks AI features in practice: SQL AI Functions, Genie Agents, AI Search for retrieval, plus a runnable keyword-retrieval toy."
---

# 05 · AI Functions, Genie Agents & AI Search

Databricks' AI features sit on top of the platform you have already built: your governed tables become inputs to LLM calls, to
natural-language question answering, and to retrieval for RAG applications. This lesson is an orientation for a data engineer: what each
feature does, what it needs, what it costs you in correctness, and where it can hurt.

!!! note "What was run, and what wasn't"
    **None of the Databricks AI features could be run** (they need a workspace, serverless/managed model compute, and in some
    cases specific regions). Function names, syntax overviews, requirements and renamed products come from the Databricks
    documentation (October 2026). These features change quickly; names have already changed (below), so verify against the
    current docs. I did run one small local program: a TF-IDF keyword-retrieval toy that mirrors the *shape* of vector search
    and shows, with real output, why plain keyword matching isn't enough. I also make no claims about the accuracy or cost of
    any model: I have no measurements.

## Names that changed

The documentation currently says **Genie Agents** (formerly Genie Spaces) and **AI Search** (formerly Databricks Vector Search).
Older tutorials, blog posts and many code samples still use the old names. Likewise **Lakeflow Spark Declarative Pipelines**
and **Declarative Automation Bundles** replaced older names (earlier in this course), and sharing is now **OpenSharing**. When
a search for a feature fails, look for its new name.

## AI Functions: LLMs inside SQL

AI Functions are built-in SQL functions that apply models to data stored on Databricks, callable from Databricks SQL,
notebooks, pipelines and jobs. From the docs:

- There are **task-specific functions** (purpose-built and maintained by Databricks) and one **general-purpose** function,
  `ai_query`, where you supply a prompt and choose a supported model or serving endpoint. The reference lists functions including
  `ai_classify`, `ai_extract`, `ai_summarize`, `ai_translate`, `ai_analyze_sentiment`, `ai_mask`, `ai_fix_grammar`,
  `ai_parse_document`, `ai_similarity`, `ai_forecast` and `ai_query` (check the current list; it grows).
- Requirements: they are **not available on classic SQL warehouses**; Databricks Runtime 15.4 LTS or above is required (18.2 or
  above recommended for performance and newest features).
- Your query runs on your compute, but model inference may run on separate Databricks-managed infrastructure that is billed **in
  addition** to that compute.
- Unity Catalog permissions can restrict which task-specific functions an organization may use.

```sql
-- Shapes only (not run). Check each function's current signature and options in the SQL reference.
SELECT ticket_id,
       ai_classify(body, ARRAY('billing', 'bug', 'feature request')) AS category,
       ai_query('my-llm-endpoint', CONCAT('Summarize in one sentence: ', body)) AS summary
FROM prod.support.tickets
WHERE created_at >= current_date() - INTERVAL 1 DAY;
```

`ai_classify(content, labels)` classifies text by custom labels (the docs describe adding label descriptions and instructions to
improve accuracy); `ai_extract` pulls structured fields according to a schema you provide; `ai_query(endpoint, request)` calls a
model serving endpoint (Lesson 04) or a hosted foundation model.

### Using them responsibly in pipelines

1. **Batch, not row-by-row loops.** Run AI Functions in SQL over a table and let the platform parallelize; but remember each row
   is a model call with latency and cost proportional to rows × tokens.
2. **Filter first.** Apply `WHERE` clauses and deduplicate before calling a model. Do not classify the same boilerplate email
   a million times.
3. **Materialize results** to a table (a Delta table, or a streaming/materialized view in a pipeline) so downstream queries read stored
   labels instead of re-calling the model.
4. **Model output is not deterministic data.** The same input can give different outputs across runs and model versions;
   treat results as probabilistic features, store the model/function version alongside them, and validate on a labeled sample
   (Lesson 02's testing mindset applies).
5. **Handle failures.** Expect NULLs, timeouts, rate limits and refusals. Count them (reconcile like Level 2 · 09).
6. **Mind the data.** Prompts built from table columns send that data to a model; governance for sensitive columns (Level 3 · 04)
   and your organization's rules about which models may see PII still apply. Check where each function's inference runs and which
   compliance standards the docs list for it.

## Genie Agents: natural language over governed data

Per the documentation, Genie lets business users ask questions in natural language. Domain experts configure an agent with
datasets (tables and views), sample queries, text instructions and a **knowledge store** (curated table and column descriptions,
synonyms, join relationships, SQL expressions). Genie uses a compound AI system, not a single model, to translate the question into SQL,
and responds with the generated query and results; if it can't answer it can ask clarifying questions. The knowledge-store settings are scoped to the agent and do
not change Unity Catalog metadata.

What this means for you as the data owner:

- **Quality depends on your metadata.** Clear table and column comments, good names, defined joins and certified datasets make
  answers better; cryptic column names make answers wrong with confidence. Descriptions (Level 3 · 01) are now a product feature.
- **Permissions still apply** because queries run under the user's identity on a SQL warehouse (Lesson 09), so row filters and masks
  (Level 3 · 04) restrict what Genie can show. Check the current docs for the exact identity and sharing model.
- **Always show the SQL.** The generated query is the audit trail; encourage users to read it, and have experts add "trusted" example
  queries for the questions that matter.
- **Use it for exploration, not as a source of record.** A number that drives a decision should come from a governed gold table or
  metric with an owner.

## AI Search (formerly Vector Search): retrieval for RAG

Retrieval-augmented generation fetches relevant documents and gives them to a model as context. **AI Search** is the managed index for
that: you create an index from a source table (a Delta table, streaming table or similar), it stores embeddings and metadata, and
you query it through a REST API (and SDK) for the most similar items. From the docs it supports hybrid keyword-and-similarity search,
filtering, reranking, ACLs on endpoints, syncing only selected columns, and automatic sync when the source table changes.

To see the idea (and its limit) without a workspace, here is a keyword-retrieval toy: index five support snippets, "embed" a query,
rank by cosine similarity.

```python
"""A TF-IDF retrieval toy: the same *shape* as vector search (index documents, embed a query, rank by cosine similarity).
TF-IDF vectors are sparse keyword vectors, NOT LLM embeddings, so they miss synonyms. Illustration only."""
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

docs = {
 "d1": "Refunds are issued to the original payment method within 5 business days.",
 "d2": "To reset your password, use the link on the sign-in page and check your email.",
 "d3": "Our warehouse ships orders Monday to Friday; delivery takes 3 to 7 days.",
 "d4": "Enterprise plans include single sign-on and audit logging.",
 "d5": "If a package is damaged, contact support with photos to get a replacement or your money back.",
}
vec = TfidfVectorizer(stop_words="english", ngram_range=(1, 2)).fit(docs.values())
M = vec.transform(docs.values())
def search(q, k=3):
    s = cosine_similarity(vec.transform([q]), M)[0]
    order = np.argsort(-s)[:k]
    return [(list(docs)[i], round(float(s[i]), 3)) for i in order]
for q in ["how do I reset my password", "get my money back for a broken item", "SSO for large companies"]:
    print(q, "->", search(q))
```

Output:

```text
how do I reset my password -> [('d2', 0.453), ('d1', 0.0), ('d3', 0.0)]
get my money back for a broken item -> [('d5', 0.277), ('d1', 0.0), ('d2', 0.0)]
SSO for large companies -> [('d1', 0.0), ('d2', 0.0), ('d3', 0.0)]
```

The first query works because "reset", "password" and "email" literally appear. The second finds the replacement/"money back" document
`d5` but, notably, not the refunds document `d1` (a real answer to the same need) because its words differ ("refunds",
"payment method"). The third finds **nothing**: `d4` says "single sign-on" and the query says "SSO", so there's no term overlap. This is the gap that LLM
**embeddings** close: they place texts with similar *meaning* near each other even without shared words. TF-IDF is not
that. A real embedding model would be expected to retrieve `d4` for the third query, but I can't show that here, so that's an
expectation, not a result.

Pipeline design for RAG on Databricks, in outline: ingest and chunk documents into a Delta table with stable IDs and Change Data Feed
(Level 2 · 01) → create an AI Search index synced from that table → at query time embed the question, retrieve the top-k
chunks with filters (for example by tenant or by access level) → send chunks plus the question to a model endpoint → log
inputs, retrieved IDs and answers for evaluation (MLflow tracing, Lesson 03). Chunk size, filters and evaluation matter more than which
model you pick.

## How It Actually Works

AI Functions are expressions that the SQL engine batches: it collects rows, calls a model endpoint (Databricks-managed or yours) with
those inputs, and returns the outputs as a column, handling retries and parallelism. That is why they can be used in any query, and why
their cost scales with rows and tokens. Genie's loop retrieves relevant metadata from the knowledge store, asks models to produce a
SQL plan, checks and runs it on a warehouse, and shows results. An AI Search index stores each chunk's embedding vector in an
approximate-nearest-neighbour structure; a query is embedded with the same model and compared to stored vectors. Embedding the query with a
*different* model than the index is a classic bug: the vectors live in different spaces and rankings are meaningless.

## Common mistakes

- **Calling a model on every row of a big table** in an interactive query.
- **Not storing the outputs**, so every dashboard refresh re-bills.
- **Treating generated labels as ground truth** without sampling and measuring.
- **Exposing sensitive columns** to models or to Genie without masking.
- **Poor table and column descriptions**, then blaming Genie.
- **Mismatched embedding models** between index and query.
- **No evaluation set** for a RAG system, so quality changes go unnoticed.
- **Relying on a name from an old tutorial** (Vector Search, Genie Spaces) and concluding the feature doesn't exist.

## Exercise

1. Extend the toy: add a synonym dictionary (SSO to "single sign-on") applied to queries and see whether `d4` is retrieved. Why
   doesn't this scale, and what replaces it?
2. Design a daily job (Level 3 · 07) that labels yesterday's support tickets with `ai_classify`, stores labels in a Delta table, and
   reconciles counts (input rows, labelled rows, NULLs). Write the SQL shape and the reconciliation checks.
3. List ten columns of a table you own that you would NOT send to an LLM function, and what you'd do instead for each.
4. Write the table and column comments a Genie user would need to ask "revenue by region last quarter" correctly for your
   `gold` schema.
