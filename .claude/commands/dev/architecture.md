You are explaining Chronon's architecture to an engineer integrating it into their infrastructure.

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Python Config Layer                          │
│  GroupBy/Join/StagingQuery definitions (api/py/ai/chronon/)     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ compile.py
┌─────────────────────────────────────────────────────────────────┐
│                      Thrift JSON Configs                         │
│              (production/group_bys/, production/joins/)          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ run.py
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
    ┌──────────┐        ┌──────────┐        ┌──────────┐
    │  Spark   │        │  Flink   │        │  Online  │
    │(Backfill)│        │(Streaming)│       │(Serving) │
    └────┬─────┘        └────┬─────┘        └────┬─────┘
         │                   │                   │
         ▼                   ▼                   ▼
    ┌──────────┐        ┌──────────┐        ┌──────────┐
    │Data Lake │───────▶│ KV Store │◀───────│ Fetcher  │
    │(Hive/etc)│        │(Redis/etc)│       │  (API)   │
    └──────────┘        └──────────┘        └──────────┘
```

## Key Components

### 1. Python API Layer (`/api/py/ai/chronon/`)

User-facing DSL for defining features:
- `group_by.py` - Feature aggregations
- `join.py` - Feature composition
- `staging_query.py` - Data preparation
- `query.py` - Query builder

Compiles to Thrift for cross-language compatibility.

### 2. Spark Engine (`/spark/src/main/scala/ai/chronon/spark/`)

Batch computation and backfills:
- **`Driver.scala`** - Main entry point, handles all Spark modes
- **`GroupBy.scala`** - GroupBy materialization
- **`Join.scala`** - Point-in-time correct joins
- **`StagingQuery.scala`** - SQL query execution

Key modes: `backfill`, `upload`, `analyze`, `consistency-metrics-compute`

### 3. Flink Engine (`/flink/src/main/scala/ai/chronon/flink/`)

Real-time feature updates:
- **`FlinkJob.scala`** - Streaming job orchestration
- Consumes from Kafka topics
- Writes to KV store in real-time

### 4. Online Serving (`/online/src/main/scala/ai/chronon/online/`)

Low-latency feature fetching:
- **`Fetcher.scala`** - Main fetching logic with caching
- **`FetcherBase.scala`** - Base implementation
- **`MetadataStore.scala`** - Config/schema caching
- **`KVStore.scala`** - Storage interface (pluggable)
- **`Api.scala`** - Extension point for custom behavior

### 5. Aggregator (`/aggregator/src/main/scala/ai/chronon/aggregator/`)

Shared aggregation logic:
- Used by Spark, Flink, AND Online serving
- Windowed aggregations with tiling
- Sketch algorithms (HyperLogLog, KLL)
- Ensures online/offline consistency

### 6. Service (`/service/`)

REST API layer (Java):
- Built on Vert.x framework
- Exposes Fetcher via HTTP endpoints
- Health checks and metrics

## Orchestration (`/airflow/`)

Control plane for scheduled execution:
- DAG constructors for GroupBy/Join/StagingQuery
- Triggered by upstream data landing
- One DAG per team for batch jobs
- One DAG per Join for backfills

## Key Entry Points

| Purpose | File |
|---------|------|
| CLI orchestration | `api/py/ai/chronon/repo/run.py` |
| Spark driver | `spark/src/main/scala/ai/chronon/spark/Driver.scala` |
| Online fetcher | `online/src/main/scala/ai/chronon/online/Fetcher.scala` |
| Aggregation | `aggregator/src/main/scala/ai/chronon/aggregator/` |
| Thrift schema | `api/thrift/api.thrift` |

## Data Flow Details

### Batch Flow (Training Data)
1. Python config → `compile.py` → Thrift JSON
2. `run.py --mode backfill` → Spark Driver
3. Spark reads sources, computes aggregations, writes to Data Lake
4. Join backfill reads GroupBy outputs, produces training tables

### Streaming Flow (Real-time Updates)
1. Events arrive on Kafka topic
2. Flink job consumes, computes aggregations
3. Writes updated values to KV store

### Serving Flow (Online Inference)
1. Request hits Fetcher with keys
2. Fetcher reads metadata (cached)
3. Fetches values from KV store (batched, deduplicated)
4. Returns feature vector

## When Explaining Architecture

1. Start with the data flow diagram
2. Explain how the same aggregation logic runs everywhere (Aggregator)
3. Highlight the Thrift layer for cross-language consistency
4. Show how run.py orchestrates different engines
5. Reference specific files for deep dives

Read the key files listed above to provide detailed architectural explanations.
