---
title: "Free Databricks Course: Lakehouse, Spark, Delta Lake, Unity Catalog, Pipelines, Jobs, SQL, MLflow, Bundles and Cost"
description: "Free Databricks course: 40 lessons on the lakehouse, Spark, Delta Lake, Auto Loader, Unity Catalog, pipelines, jobs, MLflow, bundles, cost and maintenance."
---

# Databricks Mastery Path

<div class="mp-hero-lockup">
  <div class="mp-hero-globe">
    <svg viewBox="0 0 160 160" class="mp-globe-svg mp-mark">
      <circle cx="80" cy="80" r="74" fill="var(--md-default-bg-color, #FAF7EF)" stroke="#2FA98D" stroke-width="5"/>
      <rect class="mp-bar mp-bar-1" x="36" y="90" width="17" height="24" rx="5" fill="#2FA98D"/>
      <rect class="mp-bar mp-bar-2" x="59" y="74" width="17" height="40" rx="5" fill="#7FC9B7"/>
      <rect class="mp-bar mp-bar-3" x="82" y="56" width="17" height="58" rx="5" fill="#E0B457"/>
      <rect class="mp-bar mp-bar-4" x="105" y="38" width="17" height="76" rx="5" fill="#C97B4A"/>
      <circle class="mp-dot" cx="44.5" cy="82" r="7" fill="#22261f"/>
    </svg>
  </div>
  <div class="mp-hero-divider"></div>
  <div class="mp-hero-text">
    <div class="mp-hero-title">Mastery Path</div>
    <div class="mp-hero-sub">BOOTCAMP</div>
    <div class="mp-hero-rule"></div>
    <div class="mp-hero-byline">VINOD BHANUJA</div>
  </div>
</div>

Databricks is Apache Spark and Delta Lake plus a lot of platform: a governance layer (Unity Catalog), compute you do or don't manage,
pipelines and jobs, SQL warehouses, ML tooling and AI features. It's easy to click through and hard to know what is actually
happening underneath. A one-row `DELETE` that rewrites 49,999 other rows. A partition column that turns 2 MB into 86,665 files and a
seven-minute write. A deleted checkpoint that duplicates every record. An `INSERT *` merge that silently drops a new column. An `ALTER TABLE ... CLUSTER BY` that rewrites nothing. A window function whose result changes with tied sort keys. None of these are
platform bugs. They follow from how Delta and Spark work, and this course shows each one with something you can run.

It assumes you know SQL and some Python. It doesn't assume any Databricks experience, and **most of Levels 1, 2 and 4 runs on your
own laptop** with open-source Spark and Delta Lake.

**Level 1** is the foundation: the lakehouse and its control and compute planes, compute types and notebooks, Spark DataFrames and SQL,
reading and writing files, Delta tables, the Delta log, time travel and `VACUUM`, tables, views and volumes, ending with a local
sales lakehouse. **Level 2** is Delta and data engineering: `MERGE` and the change feed, schema enforcement, `OPTIMIZE` and clustering,
partitioning, how Spark executes and why joins skew, Structured Streaming, Auto Loader and the medallion architecture, ending with an
incremental bronze/silver/gold pipeline. **Level 3** is the platform: Unity Catalog's object model, privileges, storage governance, row
filters and masks, lineage and system tables, declarative pipelines, jobs, OpenSharing and SQL warehouses, ending with a governance-as-code
project. **Level 4** is building and operating: bundles and CI/CD, testing, MLflow and model serving, AI features, cost, performance,
table maintenance and migration, ending with a platform plan and checker.

Every lesson has a **How It Actually Works** section: why Delta commits are atomic, why a file can be vacuumed out from under a
rollback, why Z-order can cut files read from 16 to 2 on one column and not another, why AQE can fix partition counts but
didn't fix a hot key on a small test, and why a deletion vector hides a row without erasing it.

!!! note "Versions and what was actually run"
    This course was written without access to a Databricks workspace, and **no output is presented as coming from Databricks**.
    What was run, and is labelled in each lesson: **Apache Spark 4.2.0 and Delta Lake 4.4.1** (open source) on a laptop for the Spark
    and Delta behaviour and the local pipeline project; the open-source Spark Declarative Pipelines runtime that ships with PySpark;
    **MLflow 3.17.2**, scikit-learn, pytest, sqlglot 30.24, and the `delta-sharing` and `databricks-sql-connector` clients (API checks
    only). Unity Catalog, jobs, SQL warehouses, Auto Loader, OpenSharing, Model Serving, AI Functions, Genie, Photon, serverless compute and
    Predictive Optimization **could not be run**; those lessons are built from the Databricks documentation as of **October 2026** and say
    so, and where a small Python or Spark program models documented behaviour, it is labelled as a model and not as Databricks code.
    The cost lesson uses invented mock billing rows. Where a local experiment failed or showed nothing (a caching micro-benchmark, a hung
    Python UDF timing), the lesson reports that instead of inventing results.

    Databricks renames and changes things often: Delta Live Tables became Lakeflow Spark Declarative Pipelines, Asset Bundles became
    Declarative Automation Bundles, Delta Sharing is now documented as OpenSharing, Genie Spaces became Genie Agents and Vector Search became AI
    Search. The lessons say which name is current. Re-check anything marked "as of October 2026" against your own workspace.

## How the program is organized

| Level | Focus | Modules | What you can do after |
|-------|-------|---------|------------------------|
| **1 · Lakehouse & Spark Foundations** | Architecture, compute, Spark, files, Delta, the log, time travel, tables and volumes | 10 | Build, change and recover a small Delta lakehouse |
| **2 · Delta Lake & Data Engineering** | MERGE, schema, clustering, partitioning, execution and skew, streaming, Auto Loader, medallion | 10 | Build incremental, idempotent pipelines and make them fast |
| **3 · Unity Catalog, Pipelines & Sharing** | Governance, privileges, storage, masks, lineage, declarative pipelines, jobs, sharing, SQL | 10 | Design and operate a governed, orchestrated platform |
| **4 · Development, ML & Operations** | Bundles, testing, MLflow, serving, AI features, cost, performance, maintenance, migration | 10 | Ship, test, monitor and afford it, and plan the whole thing |

Each level has 9 teaching modules plus a hands-on project (module 10), for **40 lessons** total.

## How to use this site

- **Run the local labs.** You need Python 3.12 and Java 17 or newer. `pip install pyspark delta-spark` is enough for Levels 1 and 2.
  Set `PYSPARK_PYTHON` to the same interpreter, or Spark will stop with a version-mismatch error (one of the things that happened while
  writing this course).
- **Get a Free Edition workspace** for the platform-only lessons. It's a serverless, quota-limited, no-cost workspace for learning;
  check its current limitations before relying on a feature.
- **Read the "what was run" note** at the top of each lesson. It tells you whether the output you see is observed, modelled or
  documented.
- **Do the exercises**, especially those that say "predict, then run". The surprises are the point.
- **Do the projects.** Each combines its level into something you could adapt, and each states what it doesn't cover.

## More from the Mastery Path series

Free, structured, module-wise training across 101 other languages, platforms and disciplines — expand a category to browse:

<div class="mastery-tracks">
<details class="mastery-card">
<summary>Languages <span class="mastery-count">20</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/python-mastery-path/">🐍 Python</a>
  <a href="https://learnmasterypath.com/java-mastery-path/">☕ Java</a>
  <a href="https://learnmasterypath.com/javascript-mastery-path/">🟨 JavaScript</a>
  <a href="https://learnmasterypath.com/typescript-mastery-path/">🔷 TypeScript</a>
  <a href="https://learnmasterypath.com/csharp-mastery-path/">🔵 C#</a>
  <a href="https://learnmasterypath.com/shell-mastery-path/">🐚 Shell/Bash</a>
  <a href="https://learnmasterypath.com/powershell-mastery-path/">💻 PowerShell</a>
  <a href="https://learnmasterypath.com/c-mastery-path/">🇨 C</a>
  <a href="https://learnmasterypath.com/cpp-mastery-path/">➕ C++</a>
  <a href="https://learnmasterypath.com/go-mastery-path/">🐹 Go</a>
  <a href="https://learnmasterypath.com/rust-mastery-path/">🦀 Rust</a>
  <a href="https://learnmasterypath.com/sql-mastery-path/">🗄️ SQL</a>
  <a href="https://learnmasterypath.com/ruby-mastery-path/">💎 Ruby</a>
  <a href="https://learnmasterypath.com/php-mastery-path/">🐘 PHP</a>
  <a href="https://learnmasterypath.com/kotlin-mastery-path/">🟣 Kotlin</a>
  <a href="https://learnmasterypath.com/swift-mastery-path/">🐦 Swift</a>
  <a href="https://learnmasterypath.com/dart-mastery-path/">🎯 Dart</a>
  <a href="https://learnmasterypath.com/scala-mastery-path/">🔴 Scala</a>
  <a href="https://learnmasterypath.com/r-mastery-path/">📊 R</a>
  <a href="https://learnmasterypath.com/matlab-mastery-path/">🟧 MATLAB</a>
</div>
</details>
<details class="mastery-card">
<summary>Web Frameworks <span class="mastery-count">11</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/react-mastery-path/">⚛️ React</a>
  <a href="https://learnmasterypath.com/nextjs-mastery-path/">▲ Next.js</a>
  <a href="https://learnmasterypath.com/angular-mastery-path/">🅰️ Angular</a>
  <a href="https://learnmasterypath.com/vue-mastery-path/">💚 Vue.js</a>
  <a href="https://learnmasterypath.com/html-css-mastery-path/">🎨 HTML & CSS</a>
  <a href="https://learnmasterypath.com/tailwind-mastery-path/">🌬️ Tailwind CSS</a>
  <a href="https://learnmasterypath.com/django-mastery-path/">🎸 Django</a>
  <a href="https://learnmasterypath.com/fastapi-mastery-path/">🚀 FastAPI</a>
  <a href="https://learnmasterypath.com/graphql-mastery-path/">🕸️ GraphQL</a>
  <a href="https://learnmasterypath.com/nodejs-mastery-path/">🟩 Node.js</a>
  <a href="https://learnmasterypath.com/spring-boot-mastery-path/">🍃 Spring Boot</a>
</div>
</details>
<details class="mastery-card">
<summary>Mobile <span class="mastery-count">2</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/flutter-mastery-path/">💙 Flutter</a>
  <a href="https://learnmasterypath.com/react-native-mastery-path/">📲 React Native</a>
</div>
</details>
<details class="mastery-card">
<summary>Testing & QA <span class="mastery-count">6</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/java-testing-mastery-path/">🧪 Java Testing</a>
  <a href="https://learnmasterypath.com/cpp-testing-mastery-path/">🧪 C/C++ Testing</a>
  <a href="https://learnmasterypath.com/python-testing-mastery-path/">🧪 Python Testing</a>
  <a href="https://learnmasterypath.com/automotive-testing-mastery-path/">🚗 Automotive Testing</a>
  <a href="https://learnmasterypath.com/selenium-mastery-path/">🧭 Selenium</a>
  <a href="https://learnmasterypath.com/cypress-mastery-path/">🌲 Cypress</a>
</div>
</details>
<details class="mastery-card">
<summary>Security <span class="mastery-count">2</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/cybersecurity-mastery-path/">🛡️ Cybersecurity</a>
  <a href="https://learnmasterypath.com/ethical-hacking-mastery-path/">🎯 Ethical Hacking</a>
</div>
</details>
<details class="mastery-card">
<summary>Cloud Platforms <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/aws-mastery-path/">☁️ AWS</a>
  <a href="https://learnmasterypath.com/azure-mastery-path/">☁️ Azure</a>
  <a href="https://learnmasterypath.com/gcp-mastery-path/">☁️ GCP</a>
  <a href="https://learnmasterypath.com/ibm-cloud-mastery-path/">☁️ IBM Cloud</a>
  <a href="https://learnmasterypath.com/adobe-mastery-path/">🎨 Adobe</a>
</div>
</details>
<details class="mastery-card">
<summary>Data & Analytics <span class="mastery-count">7</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/data-engineering-mastery-path/">🛠️ Data Engineering</a>
  <a href="https://learnmasterypath.com/data-science-mastery-path/">📈 Data Science</a>
  <a href="https://learnmasterypath.com/tableau-mastery-path/">📊 Tableau</a>
  <a href="https://learnmasterypath.com/excel-mastery-path/">📗 Excel</a>
  <a href="https://learnmasterypath.com/pyspark-mastery-path/">⚡ PySpark</a>
  <a href="https://learnmasterypath.com/etl-datalake-mastery-path/">🏞️ ETL & Data Lake</a>
  <a href="https://learnmasterypath.com/power-bi-mastery-path/">📉 Power BI</a>
</div>
</details>
<details class="mastery-card">
<summary>Databases <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/postgresql-mastery-path/">🐘 PostgreSQL</a>
  <a href="https://learnmasterypath.com/mongodb-mastery-path/">🍃 MongoDB</a>
  <a href="https://learnmasterypath.com/redis-mastery-path/">🟥 Redis</a>
  <a href="https://learnmasterypath.com/snowflake-mastery-path/">❄️ Snowflake</a>
  <a href="https://learnmasterypath.com/python-mastery-path/vector-databases-mastery-path/">🧭 Vector Databases</a>
</div>
</details>
<details class="mastery-card">
<summary>AI / ML / LLM <span class="mastery-count">12</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/ai-ml-mastery-path/">🤖 AI/ML</a>
  <a href="https://learnmasterypath.com/llm-dev-mastery-path/">🧠 LLM Dev</a>
  <a href="https://learnmasterypath.com/rag-mastery-path/">📚 RAG</a>
  <a href="https://learnmasterypath.com/edge-ai-mastery-path/">📱 Edge AI</a>
  <a href="https://learnmasterypath.com/claude-training-mastery-path/">🔶 Claude Training</a>
  <a href="https://learnmasterypath.com/ai-tools-mastery-path/">🧰 AI Tools</a>
  <a href="https://learnmasterypath.com/ml-math-mastery-path/">➗ ML Math Foundations</a>
  <a href="https://learnmasterypath.com/prompt-engineering-mastery-path/">✍️ Prompt Engineering</a>
  <a href="https://learnmasterypath.com/ai-agents-mastery-path/">🕹️ AI Agents</a>
  <a href="https://learnmasterypath.com/mlops-mastery-path/">🔁 MLOps</a>
  <a href="https://learnmasterypath.com/computer-vision-mastery-path/">👁️ Computer Vision</a>
  <a href="https://learnmasterypath.com/python-mastery-path/mcp-mastery-path/">🔌 MCP</a>
</div>
</details>
<details class="mastery-card">
<summary>Embedded Systems <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/embedded-mastery-path/">🔌 Embedded</a>
  <a href="https://learnmasterypath.com/embedded-linux-mastery-path/">🐧 Embedded Linux</a>
  <a href="https://learnmasterypath.com/embedded-python-mastery-path/">🐍 Embedded Python</a>
  <a href="https://learnmasterypath.com/freertos-mastery-path/">⏱️ FreeRTOS</a>
  <a href="https://learnmasterypath.com/s32k-mastery-path/">🔧 S32K</a>
</div>
</details>
<details class="mastery-card">
<summary>Leadership & Management <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/product-manager-mastery-path/">📋 Product Manager</a>
  <a href="https://learnmasterypath.com/product-lead-mastery-path/">🧭 Product Lead</a>
  <a href="https://learnmasterypath.com/project-manager-mastery-path/">📅 Project Manager</a>
  <a href="https://learnmasterypath.com/ai-manager-mastery-path/">🤖 AI Manager</a>
  <a href="https://learnmasterypath.com/servant-leadership-mastery-path/">🤝 Servant Leadership</a>
</div>
</details>
<details class="mastery-card">
<summary>Professional Skills <span class="mastery-count">4</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/english-fluency-mastery-path/">🗣️ English Fluency & IELTS</a>
  <a href="https://learnmasterypath.com/workday-mastery-path/">🧑‍💼 Workday</a>
  <a href="https://learnmasterypath.com/digital-marketing-mastery-path/">📣 Digital Marketing & SEO</a>
  <a href="https://learnmasterypath.com/tally-gst-mastery-path/">🧾 Tally Prime & GST</a>
</div>
</details>
<details class="mastery-card">
<summary>Careers & Interviews <span class="mastery-count">2</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/dsa-interview-mastery-path/">🧩 DSA & Coding Interviews</a>
  <a href="https://learnmasterypath.com/system-design-mastery-path/">🏗️ System Design</a>
</div>
</details>
<details class="mastery-card">
<summary>Process & APIs <span class="mastery-count">3</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/agile-mastery-path/">🔄 Agile/Scrum/Kanban</a>
  <a href="https://learnmasterypath.com/rest-api-mastery-path/">🔗 REST API</a>
  <a href="https://learnmasterypath.com/playwright-mastery-path/">🎭 Playwright</a>
</div>
</details>
<details class="mastery-card">
<summary>Infrastructure & Ops <span class="mastery-count">12</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/server-ops-mastery-path/">🖥️ Server Ops</a>
  <a href="https://learnmasterypath.com/linux-mastery-path/">🐧 Linux</a>
  <a href="https://learnmasterypath.com/nodemcu-mastery-path/">📶 NodeMCU/IoT</a>
  <a href="https://learnmasterypath.com/terraform-mastery-path/">🌍 Terraform</a>
  <a href="https://learnmasterypath.com/kubernetes-mastery-path/">☸️ Kubernetes</a>
  <a href="https://learnmasterypath.com/docker-mastery-path/">🐳 Docker</a>
  <a href="https://learnmasterypath.com/github-mastery-path/">🐙 GitHub & Git</a>
  <a href="https://learnmasterypath.com/cicd-mastery-path/">🔁 CI/CD</a>
  <a href="https://learnmasterypath.com/ansible-mastery-path/">🤖 Ansible</a>
  <a href="https://learnmasterypath.com/kafka-mastery-path/">📨 Kafka</a>
  <a href="https://learnmasterypath.com/microservices-mastery-path/">🧩 Microservices</a>
  <a href="https://learnmasterypath.com/observability-mastery-path/">📈 Observability</a>
</div>
</details>
</div>
