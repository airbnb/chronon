# Chronon

Chronon is an open-source Feature Platform for ML. It solves training-serving skew by ensuring the same feature logic runs in both batch (training) and online (serving) environments.

## Repository Structure

### Core Modules

**Python API** (`/api/py/ai/chronon/`)
- `group_by.py` - GroupBy definitions (aggregations over sources) (features)
- `join.py` - Join definitions (combine multiple GroupBys) (feature vectors)
- `staging_query.py` - SQL-based data preparation

**CLI Tools** (`/api/py/ai/chronon/repo/`)
- `compile.py` - Convert Python configs to Thrift JSON
- `run.py` - Execute jobs (backfill, upload, fetch, analyze, etc.)
- `validator.py` - Validate configurations

### Execution Engines

**Spark** (`/spark/src/main/scala/ai/chronon/spark/`)
- `Driver.scala` - Main entry point for all Spark modes
- `GroupBy.scala` - GroupBy materialization logic
- `Join.scala` - Join execution with point-in-time correctness
- `StagingQuery.scala` - SQL query execution

**Flink** (`/flink/src/main/scala/ai/chronon/flink/`)
- `FlinkJob.scala` - Streaming job entry point
- Real-time feature updates from Kafka sources

**Online Serving** (`/online/src/main/scala/ai/chronon/online/`)
- `Fetcher.scala` - Feature fetching with caching
- `FetcherBase.scala` - Base fetching logic
- `MetadataStore.scala` - Configuration management
- `Api.scala` - Online API interface (includes KVStore trait)

**Aggregator** (`/aggregator/src/main/scala/ai/chronon/aggregator/`)
- Shared aggregation logic across Spark/Flink/Online
- Windowed aggregations with tiling optimization
- Sketch algorithms (HyperLogLog, KLL, etc.)

### Supporting Modules

- `/docs/source/` - Sphinx documentation
- `/api/py/test/sample/` - Example configs and test data
- `/airflow/` - Orchestration DAG constructors
- `/service/` - REST API service (Vert.x)
- `/quickstart/` - Docker-based tutorial environment

## Key Concepts

### 1. Source
Where data comes from:
- **EventSource** - Log tables with timestamps (+ optional Kafka topic for streaming)
- **EntitySource** - Daily snapshot tables (dimension data)

### 2. GroupBy
Feature definitions with:
- **Keys** - Grouping dimensions (e.g., user_id)
- **Aggregations** - Operations like SUM, COUNT, AVG, APPROX_UNIQUE_COUNT
- **Windows** - Time ranges (e.g., 3 days, 14 days, 30 days)
- **Accuracy** - SNAPSHOT (midnight updates) or TEMPORAL (real-time)

### 3. Join
Combines multiple GroupBys:
- **Left side** - Defines the timeline for backfill (primary keys + timestamps)
- **Right parts** - GroupBys to include
- Ensures **point-in-time correctness** for ML training data

## Common Workflows

### Feature Development
```bash
# 1. Define GroupBy/Join in Python
# 2. Compile to Thrift
compile.py --conf=joins/team/my_join.py

# 3. Analyze (validate schema, check row counts)
run.py --mode analyze --conf production/joins/team/my_join.v1

# 4. Backfill historical data
run.py --mode backfill --conf production/joins/team/my_join.v1 --ds 2024-01-01

# 5. Upload to KV store (for online serving)
run.py --mode upload --conf production/group_bys/team/my_gb.v1 --ds 2024-01-01

# 6. Test online fetch
run.py --mode fetch --type join --name team/my_join.v1 --key-json '{"user_id":"123"}'
```

### run.py Modes
| Mode | Purpose |
|------|---------|
| `backfill` | Compute historical feature values (Spark) |
| `upload` | Prepare data for KV store upload |
| `streaming` | Real-time feature computation (Flink) |
| `metadata-upload` | Upload Join metadata for online serving |
| `fetch` | Query online features (test mode) |
| `analyze` | Validate configs (schema, row counts) |
| `consistency-metrics-compute` | Compare online/offline values |

## Build Systems

### SBT (Scala)
```bash
sbt compile              # Compile all modules
sbt test                 # Run tests
sbt "spark_uber/assembly"  # Build Spark uber JAR
```

### Bazel (Hermetic)
```bash
bazel build //...        # Build all targets
bazel test //...         # Run all tests
```

### Docker (Quickstart)
```bash
docker-compose up        # Start local environment (from repo root)
```

## Code Conventions

### Python API
- Use `select()` helper for column selections: `select("user_id", "amount")`
- Version GroupBys/Joins with suffixes: `purchases_v1`, `purchases_v2`
- Set `online=True` only when ready for production serving
- Use `env={}` for per-config resource overrides

### Scala
- Follow existing patterns in each module
- Use Builders API for configuration construction
- Leverage Extensions for common operations

## Key Entry Points

When exploring the codebase, start here:

| Purpose | File |
|---------|------|
| Python GroupBy API | `api/py/ai/chronon/group_by.py` |
| Python Join API | `api/py/ai/chronon/join.py` |
| CLI execution | `api/py/ai/chronon/repo/run.py` |
| Spark driver | `spark/src/main/scala/ai/chronon/spark/Driver.scala` |
| Online fetcher | `online/src/main/scala/ai/chronon/online/Fetcher.scala` |
| Aggregation logic | `aggregator/src/main/scala/ai/chronon/aggregator/` |

## Documentation

- **Feature authoring**: `docs/source/authoring_features/`
- **Setup & integration**: `docs/source/setup/`
- **Testing & deployment**: `docs/source/test_deploy_serve/`
- **Examples**: `api/py/test/sample/`

## Custom Commands

This repository includes Claude Code commands for common tasks:

**For Feature Authors (Data Scientists):**
- `user/groupby` - Help writing GroupBy definitions
- `user/join` - Help writing Join definitions
- `user/debug` - Debug compilation and backfill errors
- `user/staging-query` - Help with StagingQuery definitions

**For Platform Integrators (Engineers):**
- `dev/architecture` - System architecture overview
- `dev/integrate` - Integration guidance (KV stores, Airflow, etc.)
- `dev/specialist/join-backfill` - Deep dive into Spark Join execution
- `dev/specialist/feature-serving` - Deep dive into online serving
- `dev/specialist/streaming` - Deep dive into Flink streaming
- `dev/specialist/aggregator` - Deep dive into aggregation/tiling
