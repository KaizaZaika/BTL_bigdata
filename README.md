# Real-Time Streaming Analytics for GitHub Repositories

This project builds a production-style streaming pipeline that listens to GitHub’s public Search API, enriches the feed with advanced Apache Spark analytics, and surfaces the results on a near real-time dashboard. Everything (data source, Spark master/worker, Redis cache, Flask web app) is containerized with Docker Compose so you can turn the stack on with a single command.

---## Key Capabilities
- **Live GitHub telemetry** pulled every 15 seconds for Python, Java, and JavaScript repositories.
- **Spark Structured Streaming** micro-batches every 60 seconds and keeps lifetime + per-window state in memory + checkpoints.
- **Eight analytical tracks** covering descriptive stats, keyword extraction, topic modeling, NER, TF-IDF similarity, and time-series trend detection.
- **Auto-refreshing dashboard** served by Flask + Redis with Matplotlib charts and vanilla JS updates every minute.
- **Portable deployment** using pre-built Docker images for Spark, Redis, and lightweight Python services.

---

## Repository Layout
```
├── streaming/
│   ├── data_source.py          # GitHub poller that writes newline JSON over TCP
│   ├── spark_app.py            # Spark Streaming job orchestrating all analytics
│   ├── docker-compose.yaml     # One-stop stack (Spark master/worker, Redis, web app, data source)
│   └── webapp/
│       ├── web_app.py          # Flask API + Redis-backed cache + chart generation
│       ├── templates/index.html
│       └── static/index.js     # Dashboard auto-refresh logic
├── result/                     # Sample PNG exports used in the presentation/report
├── System_Architecture.png     # High-level system view
├── streaming_architecture.png  # Streaming data flow detail
└── Webapp.png                  # Dashboard layout reference
```

Supporting docs (`presentation_outline.md`, `presentation_script.md`, `QA.md`, `report.pdf`, `slide.pdf`) capture the academic deliverables for the course presentation.

---

## Architecture Overview
```
GitHub REST API
   ↓ (HTTP pull)
Data Source Service (Python requests + TCP socket on :9999)
   ↓ (newline JSON stream)
Spark Streaming (micro-batch = 60s)
   ↓ (HTTP POST /updateData)
Flask Web App + Redis cache
   ↓ (AJAX /getData refresh)
Browser Dashboard (auto-refresh every 60s)
```

- **Data Source (`streaming/data_source.py`)** authenticates with a GitHub Personal Access Token (PAT), cycles through three languages, de-duplicates, and streams lightweight JSON.
- **Spark Job (`streaming/spark_app.py`)** maintains two global states: lifetime repositories and the most recent batch. Each batch triggers all analytics functions (requirements 3.x and 4.x).
- **Web Layer (`streaming/webapp/`)** caches the latest payload in Redis, renders charts via Matplotlib, and exposes REST endpoints for Spark updates and browser consumption.
- **Container Orchestration (`streaming/docker-compose.yaml`)** assembles Spark master/worker, Redis, web app, and data source. Update the bind mount path + PAT before launching.

Refer to `System_Architecture.png`, `streaming_architecture.png`, and `Webapp.png` for diagrammatic context.

---

## Analytics Modules

| Requirement | Description | Output Surface |
|-------------|-------------|----------------|
| **Req 3.1** | Lifetime repository count per language (deduplicated by repo ID). | Dashboard counters |
| **Req 3.2** | Count of repositories with pushes in the last 60 seconds per language and batch. | Matplotlib line chart |
| **Req 3.3** | Average stars per language (rounded to 2 decimals). | Matplotlib bar chart |
| **Req 3.4** | Top 10 keywords per language using regex cleaning + Counter. | Text list |
| **Req 3.5** | LDA topic modeling (5 topics × top 5 words) per language via Spark MLlib. | Cards per language |
| **Req 4.1** | Named Entity Recognition using curated dictionaries (companies, frameworks, tools, technologies). | Tag cloud style list |
| **Req 4.2** | TF-IDF cosine similarity to reveal related repo pairs (>0.1 similarity). | Rich text cards |
| **Req 4.3** | Time-series rollups (synthetic or real `created_at`) with growth rate, peak month, and direction classification. | Trend cards |

Every requirement feeds a JSON payload posted to `/updateData`, cached in Redis, and rendered within the dashboard.

---

## Getting Started

### Prerequisites
- Docker and Docker Compose
- GitHub Personal Access Token (PAT) with `public_repo` scope (low risk but required to raise rate limits)
- ~4 GB RAM free for Spark master + worker containers
- Windows users: adjust bind mount paths inside `streaming/docker-compose.yaml` (`D:/GitHub-Real-time-Analytics/streaming` → your absolute path)

### 1. Clone the repo
```bash
git clone https://github.com/KaizaZaika/BTL_bigdata.git
cd BTL_bigdata/streaming
```

### 2. Configure environment
1. Open `streaming/docker-compose.yaml`.
2. Replace `TOKEN=your_api_token_here` with your GitHub PAT (never commit real tokens).
3. Update the `volumes:` paths to match your local directory, e.g. `- ${PWD:/streaming` on macOS/Linux or `- //c/path/to/streaming:/streaming` on Windows with WSL/Docker Desktop.

### 3. Launch the stack
```bash
docker-compose up -d
```
This brings up five services: `spark`, `spark-worker`, `data-source`, `redis`, and `webapp`.

### 4. Submit the Spark job
```bash
docker exec -it streaming-spark-1 \
  /opt/spark/bin/spark-submit /streaming/spark_app.py
```
Keep the logs visible; Spark prints a banner per requirement so you can trace outputs batch by batch.

### 5. View the dashboard
- Open `http://localhost:5000`.
- The countdown timer indicates the next refresh. Charts auto-reload as PNGs with cache-busting timestamps.
- Spark master UI: `http://localhost:8080`.
- Spark worker UI: `http://localhost:28081`.
- Spark application UI: `http://localhost:24040`.

### Optional: run components locally (without Docker)
1. Install Python 3.9+, Apache Spark 3.x with Hadoop binaries, and Redis locally.
2. Run `python streaming/data_source.py`.
3. Launch Spark job with `spark-submit streaming/spark_app.py`.
4. Start the web app from `streaming/webapp`: `pip install flask redis matplotlib` then `python web_app.py`.
Use environment variables (`TOKEN`, `REDIS_HOST`, etc.) to align ports and hostnames.

---

## Dashboard Tour
- **Totals + averages**: top section shows cumulative repository counts and per-language star averages.
- **Batch trends**: line chart (`req2`) reveals burstiness of pushes in the last minute.
- **Keyword + topic cards**: highlight qualitative signals (dominant descriptors, LDA topics).
- **Entity radar**: NER cards spotlight companies, frameworks, tools, and technology buzzwords.
- **Similarity pairs**: lists the top TF-IDF cosine matches with snippets for quick comparison.
- **Time-series cards**: monthly aggregates with growth arrows + peak month callouts.

The `/result/` folder contains pre-generated PNGs (`result1.png` … `result8.png`) used in the report and slide deck as visual references.

---

## Operations & Monitoring
- **Logs**: `docker-compose logs -f data-source spark webapp` for API errors, Spark stack traces, or Flask issues.
- **Spark Master UI**: validate executor status, batch durations, and storage levels.
- **Redis inspection**: `docker exec -it <redis-container> redis-cli get data` to inspect the cached payload.
- **Fault tolerance**: Spark checkpoints (see `checkpoint_EECS4415_Porject_3`) plus in-memory deduplication guard against duplicate analytics when the job restarts.
- **Scaling tips**: increase worker memory/cores via `SPARK_WORKER_MEMORY`/`SPARK_WORKER_CORES` in `docker-compose.yaml` if topic modeling or TF-IDF runs out of resources.

---

## Troubleshooting
- **Dashboard shows “no data”**: ensure the Spark job connected to `data-source:9999` and the GitHub PAT has not hit rate limits (429) or expired (401).
- **Charts stale after refresh**: delete cached PNGs in `streaming/webapp/static/` and restart the `webapp` container; confirm Redis is reachable.
- **Spark exits immediately**: check container logs for missing Python deps or incompatible Spark version; re-run `spark-submit` after the master/worker services are fully up.
- **PAT leaks in logs**: regenerate the token inside GitHub and update `docker-compose.yaml`; tokens are only read from `TOKEN` env var at container start.
- **Socket refusal on 9999**: restart the `data-source` container (it binds before Spark connects); make sure no host process uses the same port.

---

## Design Notes & Future Enhancements
- **Micro-batch cadence** of 60 seconds balances GitHub API limits with heavier ML workloads; consider 30s windows if you add caching or GraphQL.
- **State strategy** combines Python dictionaries for low-latency lookups with Spark checkpointing for recovery.
- **Mixed API usage** (DataFrame vs RDD) keeps complex text processing flexible; migrating to all DataFrames would simplify optimization.
- **Visualization strategy** uses server-side PNG rendering to avoid bundling a frontend build pipeline; could be swapped for client-side charting (Chart.js, D3) to reduce disk writes.
- **Potential extensions**: GitHub Actions to auto-run lint/tests, Grafana dashboards powered by Redis streams, or support for additional languages/topics.

---
