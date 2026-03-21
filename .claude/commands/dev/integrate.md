You are helping an engineer integrate Chronon into their infrastructure.

## Key Files to Reference

- **KV Store Interface**: `online/src/main/scala/ai/chronon/online/KVStore.scala`
- **API Interface**: `online/src/main/scala/ai/chronon/online/Api.scala`
- **Reference Implementation**: `quickstart/mongo-online-impl/`
- **Configuration**: `api/py/test/sample/teams.json`

## Integration Points

### 1. Key-Value Store

Chronon requires a KV store for online feature serving.

**Interface to Implement**: `ai.chronon.online.KVStore` (trait defined in `Api.scala`)

```scala
trait KVStore {
  def create(dataset: String): Unit
  def get(request: GetRequest): Future[GetResponse]
  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]
  def put(request: PutRequest): Future[Boolean]
  def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]]
  def bulkPut(sourceOfflineTable: String, destinationOnlineDataset: String, partition: String): Unit
}
```

**Recommended Stores**:
- Redis (low latency, simple)
- Cassandra (high throughput, scalable)
- DynamoDB (managed, AWS-native)

**Reference**: See `quickstart/mongo-online-impl/` for MongoDB example.

### 2. Data Warehouse

For batch computation and offline storage.

**Requirements**:
- Spark-compatible (Hive, Iceberg, Delta Lake)
- Date-partitioned tables (default: `ds` column)
- Read/write access for Spark jobs

**Configuration in teams.json**:
```json
{
  "common_env": {
    "PARTITION_COLUMN": "ds",
    "PARTITION_FORMAT": "yyyy-MM-dd"
  }
}
```

### 3. Streaming (Optional)

For real-time feature updates.

**Requirements**:
- Kafka for event streaming
- Flink cluster for processing

**Configuration**:
- Add `topic` to EventSource definitions
- Configure Flink job submission in run.py

### 4. Orchestration

For scheduled job execution.

**Default: Airflow**
- DAG constructors in `/airflow/`
- Customize templates for your environment

**Alternative**: Any scheduler that can invoke:
```bash
run.py --mode <mode> --conf <config> --ds <date>
```

### 5. Online API

For custom online behavior.

**Abstract Class to Extend**: `ai.chronon.online.Api`

```scala
abstract class Api(userConf: Map[String, String]) {
  def genKvStore: KVStore                    // Required: your KV store implementation
  def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder
  def externalRegistry: ExternalSourceRegistry
  def logResponse(resp: LoggableResponse): Unit  // Required: logging hook
  def buildFetcher(debug: Boolean = false,
                   callerName: String = null,
                   disableErrorThrows: Boolean = false): Fetcher
}
```

## Configuration Hierarchy

```
teams.json (global)
├── default (shared settings)
│   ├── table_properties
│   ├── common_env (all modes)
│   ├── production
│   │   ├── backfill (Spark settings)
│   │   ├── upload
│   │   └── streaming
│   └── dev
└── team_name
    ├── namespace
    ├── production
    └── dev
```

**Example teams.json**:
```json
{
  "default": {
    "common_env": {
      "SPARK_SUBMIT_PATH": "/path/to/spark-submit",
      "PARTITION_COLUMN": "ds"
    },
    "production": {
      "backfill": {
        "EXECUTOR_MEMORY": "8G",
        "EXECUTOR_CORES": "2"
      }
    }
  },
  "my_team": {
    "namespace": "my_features_db",
    "production": {
      "backfill": {
        "EXECUTOR_MEMORY": "16G"
      }
    }
  }
}
```

## Integration Steps

### Step 1: Implement KVStore

```scala
class MyKVStore extends KVStore {
  override def get(request: GetRequest): Future[GetResponse] = {
    // Implement for your storage
  }
  // ... implement other methods
}
```

### Step 2: Configure teams.json

Set up namespaces, Spark settings, and environment variables.

### Step 3: Set Up Orchestration

Either use Airflow DAGs or integrate run.py with your scheduler.

### Step 4: Build Online Service

Package the Fetcher with your KVStore implementation:
```scala
val api = new MyApi(myKvStore)
val fetcher = api.buildFetcher()
// Expose via HTTP/gRPC
```

### Step 5: Run Quickstart

Validate your setup:
```bash
cd quickstart
docker-compose up
# Follow tutorial to verify end-to-end flow
```

## Common Integration Patterns

### Multi-Region Deployment
- Replicate KV store across regions
- Run Spark jobs in one region, stream to all

### Gradual Rollout
- Start with batch-only (no streaming)
- Add streaming for latency-sensitive features
- Enable consistency checking

### Schema Management
- Chronon manages schemas via Thrift
- Register schemas with your schema registry if needed

## When Helping with Integration

1. Understand their existing infrastructure (Spark version, storage, scheduler)
2. Identify the minimal integration path
3. Point to relevant interfaces and examples
4. Recommend starting with quickstart for validation
5. Suggest incremental adoption (batch → streaming → online)

Read the key files listed above to provide specific integration guidance.
