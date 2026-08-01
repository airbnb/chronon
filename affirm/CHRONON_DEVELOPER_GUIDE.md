# Chronon Developer Guide

## Overview

This guide is designed for **Chronon operators and developers** who need to understand the complete Chronon system architecture, production workflows, and how to simulate production environments locally using Docker. It covers both the technical implementation details and production-like orchestration.

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Production Workflows](#production-workflows)
3. [Local Development Setup](#local-development-setup)
4. [GroupBy Development Workflow](#groupby-development-workflow)
5. [Airflow Integration](#airflow-integration)
6. [Production Simulation](#production-simulation)
7. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
8. [Best Practices](#best-practices)

## System Architecture

### Key Components

- **Spark**: Batch processing engine for GroupBy computations
- **Iceberg**: Table format for ACID transactions and time travel
- **MongoDB**: Online KV store for local development (Chronon's built-in support)
- **DynamoDB**: Online KV store for production environments
- **Airflow**: Workflow orchestration and job scheduling
- **S3/MinIO**: Object storage for Iceberg tables and raw data

## Production Workflows

### How Production Works

1. **Data Ingestion**: Raw data flows into data warehouse (S3/HDFS)
2. **Feature Definition**: MLEs define GroupBy configurations in Python
3. **Compilation**: Python configs are compiled to Thrift JSON
4. **Orchestration**: Airflow DAGs automatically discover and schedule jobs
5. **Execution**: Spark processes GroupBy computations
6. **Storage**: Results stored in Iceberg tables (S3) and DynamoDB (online serving)
7. **Serving**: Features served via online API

### Key Components

- **Spark**: Batch processing engine for GroupBy computations
- **Iceberg**: Table format for ACID transactions and time travel
- **MongoDB**: Online KV store for local development (Chronon's built-in support)
- **DynamoDB**: Online KV store for production environments
- **Airflow**: Workflow orchestration and job scheduling
- **S3/MinIO**: Object storage for Iceberg tables and raw data

## Local Development Setup

### Prerequisites

```bash
# Check Docker is installed
docker --version
docker-compose --version

# Start Docker if it's not running
open -a Docker  # macOS
# or start Docker Desktop from Applications

# Check ports are available
lsof -i :8080 -i :8085 -i :8888 -i :9000 -i :9001 -i :27017 -i :8000
```

### Start Development Environment

```bash
# Clone Chronon repository
git clone git@github.com:airbnb/chronon.git
cd chronon/affirm

# Start the developer environment (full setup)
./setup-chronon-developer.sh
```

### Verify Services

```bash
# Check all containers are running
docker-compose -f docker-compose-developer.yml ps
```

**Expected Output**:
```
NAME                       IMAGE                             STATUS
chronon-airflow-db-1       postgres:13                       Up (healthy)
chronon-chronon-main-1     ezvz/chronon                      Up
chronon-dynamodb-local-1   amazon/dynamodb-local:latest      Up
chronon-jupyter-1          jupyter/pyspark-notebook:latest   Up (healthy)
chronon-minio-1            minio/minio:latest                Up (healthy)
chronon-mongodb-1          mongo:latest                      Up (healthy)
chronon-spark-master-1     bitnami/spark:3.5.0               Up
chronon-spark-worker-1     bitnami/spark:3.5.0               Up
chronon-spark-worker-2     bitnami/spark:3.5.0               Up
```

### Access Points

- **Spark Master**: http://localhost:8080 - GroupBy computation
- **Jupyter Notebooks**: http://localhost:8888 (token: chronon-dev) - Data exploration
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin) - S3 storage
- **MongoDB**: localhost:27017 - Online feature serving
- **Airflow Dashboard**: http://localhost:8085 (admin/admin) - Production-like orchestration
- **DynamoDB Local**: http://localhost:8000 - Optional testing

## GroupBy Development Workflow

### 1. Explore Sample Data

The setup script has already copied sample Parquet files to MinIO. You can explore them:

```python
# In Jupyter Notebook (http://localhost:8888)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataExploration").getOrCreate()

# Load sample data from MinIO
purchases_df = spark.read.parquet("s3a://chronon/warehouse/data/purchases/purchases.parquet")
users_df = spark.read.parquet("s3a://chronon/warehouse/data/users/users.parquet")

# Explore the data
purchases_df.show(10)
purchases_df.describe().show()
```

### 2. Setup Team Information

Before compiling, you need to create a `teams.json` file to define your team configuration:

```bash
# Create teams.json file in the root directory
cat > teams.json << 'EOF'
{
    "my_team": {
        "description": "My team for GroupBy development",
        "namespace": "default",
        "user": "developer",
        "production": {
            "backfill": {
                "EXECUTOR_CORES": "2",
                "DRIVER_MEMORY": "1G",
                "EXECUTOR_MEMORY": "2G",
                "PARALLELISM": "4"
            },
            "upload": {
                "EXECUTOR_CORES": "2",
                "EXECUTOR_MEMORY": "2G",
                "PARALLELISM": "4"
            }
        }
    }
}
EOF
```

**Key Points**:
- **Team name** (`my_team`) must match your directory structure
- **Namespace** defines the database namespace for your tables
- **Production settings** configure Spark resources for different job types
- **User** field is used for job ownership and permissions

### 3. Define GroupBy Configuration

Create your GroupBy configuration in Python:

```python
# group_bys/my_team/user_purchase_features.py
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit

# Define the source using sample data from MinIO
source = Source(
    events=EventSource(
        table="purchases",  # Temp view created from MinIO Parquet file
        query=Query(
            selects=select("user_id", "purchase_price", "item_category"),
            time_column="ts"  # Event timestamp column
        )
    )
)

# Define time windows for aggregations
window_sizes = [
    Window(length=1, timeUnit=TimeUnit.DAYS),    # 1 day
    Window(length=7, timeUnit=TimeUnit.DAYS),    # 7 days
    Window(length=30, timeUnit=TimeUnit.DAYS),   # 30 days
]

# Create the GroupBy configuration
v1 = GroupBy(
    sources=[source],
    keys=["user_id"],  # Primary key for grouping
    aggregations=[
        # Sum of purchase prices in different windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=window_sizes
        ),
        # Count of purchases in different windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
        # Average purchase price in different windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
        # Last 10 purchase prices (no window = all time)
        Aggregation(
            input_column="purchase_price",
            operation=Operation.LAST_K(10),
        ),
    ],
    online=True,  # Enable online serving
)
```

### 4. Compile Configuration

Convert your Python configuration to Thrift JSON:

```bash
# Compile the GroupBy configuration
python3 api/py/ai/chronon/repo/compile.py \
    --conf group_bys/my_team/user_purchase_features.py \
    --force-overwrite

# This creates: production/group_bys/my_team/user_purchase_features.v1
```

### 5. Run Local Backfill (Direct)

Execute the GroupBy computation for a specific date:

```bash
# Run backfill for a specific date
python3 api/py/ai/chronon/repo/run.py \
    --conf production/group_bys/my_team/user_purchase_features.v1 \
    --mode backfill \
    --ds 2023-12-01 \
    --repo /srv/chronon

# For date ranges
python3 api/py/ai/chronon/repo/run.py \
    --conf production/group_bys/my_team/user_purchase_features.v1 \
    --mode backfill \
    --start-ds 2023-12-01 \
    --end-ds 2023-12-07 \
    --repo /srv/chronon
```

### 6. Check Output in Iceberg Tables

Verify your computed features in Iceberg tables:

```bash
# Access Spark SQL
docker-compose -f docker-compose-developer.yml exec chronon-main spark-sql

# Inside Spark SQL
USE default;
SHOW TABLES LIKE '*user_purchase*';

# Check the computed features
SELECT * FROM default.my_team_user_purchase_features_v1 
WHERE ds = '2023-12-01' 
LIMIT 10;

# Verify it's an Iceberg table
DESCRIBE EXTENDED default.my_team_user_purchase_features_v1;
# Should show: Table Type: ICEBERG
```

### 7. Upload to Online Store

Upload computed features to MongoDB for online serving:

```bash
# Upload features to MongoDB
python3 api/py/ai/chronon/repo/run.py \
    --conf production/group_bys/my_team/user_purchase_features.v1 \
    --mode upload \
    --ds 2023-12-01 \
    --repo /srv/chronon
```

### 8. Test Online Serving

Test fetching features from the online store:

```bash
# Test fetch for a specific user
python3 api/py/ai/chronon/repo/run.py \
    --mode fetch \
    --conf-type group_bys \
    --name my_team/user_purchase_features.v1 \
    -k '{"user_id":"user_1"}' \
    --repo /srv/chronon
```

## Airflow Integration

### Production-Like Spark Job Submission

Airflow is the **standard way** to submit Spark jobs in production environments. Using Airflow locally gives you a production-like development experience.

### Why Use Airflow for GroupBy Development

- **Production-like workflow** - Same job submission process as production
- **Spark job orchestration** - Proper job scheduling and dependency management
- **Monitoring and alerting** - Web UI for job status and failure notifications
- **Error handling** - Automatic retries and failure notifications
- **Resource management** - Proper Spark cluster resource allocation
- **Job history** - Track job execution history and performance

### Submit via Airflow (Production-Like)

Instead of using `run.py` directly, you can submit your compiled JSON configs to Airflow using the OSS Chronon DAGs. This provides a production-like workflow with proper orchestration, monitoring, and error handling.

#### Step 1: Copy Configs to Airflow DAGs Folder

Copy your compiled JSON configs to the Airflow DAGs folder where Chronon DAGs can discover them:

```bash
# Create the team directory in Airflow DAGs
docker-compose -f docker-compose-developer.yml exec airflow-webserver \
    mkdir -p /opt/airflow/dags/chronon/group_bys/my_team

# Copy your compiled GroupBy config
docker cp production/group_bys/my_team/user_purchase_features.v1 \
    chronon-airflow-webserver-1:/opt/airflow/dags/chronon/group_bys/my_team/

# Verify the file was copied
docker-compose -f docker-compose-developer.yml exec airflow-webserver \
    ls -la /opt/airflow/dags/chronon/group_bys/my_team/
```

#### Step 2: Configure Chronon Constants

Edit the Airflow constants file to point to your configs and configure the DAG behavior:

```bash
# Access the Airflow container
docker-compose -f docker-compose-developer.yml exec airflow-webserver bash

# Edit the constants file
vi /opt/airflow/dags/chronon/constants.py
```

Update the constants:
```python
# In /opt/airflow/dags/chronon/constants.py
import os

# Path to your compiled JSON configs
CHRONON_PATH = "/opt/airflow/dags/chronon"

# Team name (should match your directory structure)
TEST_TEAM_NAME = "my_team"

# Concurrency settings (adjust based on your resources)
GROUP_BY_BATCH_CONCURRENCY = 5
JOIN_CONCURRENCY = 3

# Time partition columns
time_parts = ["ds", "ts", "hr"]

# Spark configuration
SPARK_MASTER = "spark://spark-master:7077"
SPARK_DRIVER_MEMORY = "1g"
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_EXECUTOR_CORES = "2"

# Online store configuration (MongoDB for local dev)
ONLINE_STORE_CONFIG = {
    "host": "mongodb",
    "port": 27017,
    "database": "admin",
    "username": "admin",
    "password": "admin"
}
```

#### Step 3: Restart Airflow Services

Restart Airflow to pick up the new configuration:

```bash
# Restart Airflow services
docker-compose -f docker-compose-developer.yml restart airflow-webserver airflow-scheduler

# Wait for services to be ready
sleep 30
```

#### Step 4: Monitor DAGs in Airflow UI

1. **Access Airflow UI**: http://localhost:8085 (admin/admin)
2. **Find your DAG**: Look for `chronon_batch_dag_my_team`
3. **Check DAG structure**: Click on the DAG to see the task graph
4. **Trigger manually**: Click "Trigger DAG" to run your GroupBy
5. **Monitor progress**: Watch task execution in real-time

#### Step 5: Verify Airflow Execution

```bash
# Check DAG status
docker-compose -f docker-compose-developer.yml exec airflow-webserver \
    airflow dags list | grep chronon

# Check specific DAG runs
docker-compose -f docker-compose-developer.yml exec airflow-webserver \
    airflow dags state chronon_batch_dag_my_team 2023-12-01

# View DAG logs
docker-compose -f docker-compose-developer.yml exec airflow-webserver \
    airflow tasks logs chronon_batch_dag_my_team backfill_task 2023-12-01

# Check task details
docker-compose -f docker-compose-developer.yml exec airflow-webserver \
    airflow tasks list chronon_batch_dag_my_team
```

#### Step 6: Monitor Spark Jobs

While Airflow is running your DAG, monitor the Spark jobs:

1. **Spark UI**: http://localhost:8080 - See job progress and resource usage
2. **Airflow UI**: http://localhost:8085 - See task status and logs
3. **Command Line**: Use the verification commands above

#### Step 7: Verify Results

After the DAG completes successfully:

```bash
# Check if your GroupBy table was created
docker-compose -f docker-compose-developer.yml exec chronon-main spark-sql -e "
SHOW TABLES LIKE '*my_team*';
SELECT * FROM default.my_team_user_purchase_features_v1 WHERE ds = '2023-12-01' LIMIT 5;
"

# Check online store (MongoDB)
docker-compose -f docker-compose-developer.yml exec mongodb mongosh -u admin -p admin --authenticationDatabase admin -e "
use admin
db.my_team_user_purchase_features_v1.find().limit(3)
"
```

### Understanding the OSS Chronon DAGs

The OSS Chronon DAGs automatically discover your JSON configs and create DAGs with the following structure:

```
chronon_batch_dag_my_team
├── backfill_task          # Runs GroupBy backfill
├── upload_task           # Uploads features to online store
└── metadata_upload_task  # Uploads metadata
```

**DAG Naming Convention**:
- `chronon_batch_dag_{team_name}` - For GroupBy configs
- `chronon_join_{join_name}` - For Join configs
- `chronon_staging_query_{team_name}` - For Staging Query configs

### Benefits of Airflow Approach

- ✅ **Production-like workflow** - Same as production environment
- ✅ **Automatic discovery** - DAGs auto-generate from JSON configs
- ✅ **Scheduling** - Daily/weekly automated execution
- ✅ **Monitoring** - Web UI for job status and logs
- ✅ **Error handling** - Automatic retries and failure notifications
- ✅ **Dependency management** - Chain multiple GroupBys together
- ✅ **Resource management** - Proper Spark cluster allocation
- ✅ **Job history** - Track execution history and performance

## Production Simulation

### Understanding Auto-Generated DAGs

Chronon automatically creates DAGs based on your configurations:

- **GroupBy DAGs**: `chronon_batch_dag_{team_name}` - Runs backfill and upload jobs
- **Join DAGs**: `chronon_join_{join_name}` - Runs join computations
- **Staging Query DAGs**: `chronon_staging_query_{team_name}` - Runs staging queries

### Workflow Comparison

| Approach | Use Case | Pros | Cons |
|----------|----------|------|------|
| **Direct run.py** | Development, testing, debugging | Fast iteration, immediate feedback, direct control | Manual execution, no scheduling |
| **Airflow + OSS DAGs** | Production simulation, scheduling | Production-like, automated, monitoring, error handling | Setup overhead, less direct control |

### Hybrid Approach: Development + Production Simulation

**Recommended Workflow**:

1. **Initial Development** (Direct run.py):
   ```bash
   # Quick iteration and testing
   python3 api/py/ai/chronon/repo/run.py --conf ... --mode backfill --ds 2023-12-01
   ```

2. **Production Simulation** (Airflow with OSS DAGs):
   ```bash
   # Copy compiled JSON configs to Airflow DAGs folder
   # Use OSS Chronon DAGs for production-like execution
   # Monitor via Airflow UI at http://localhost:8085
   ```

3. **Production Deployment**:
   ```bash
   # Same Airflow DAGs and JSON configs work in production
   # No changes needed for deployment
   ```

## Monitoring and Troubleshooting

### Real-time Monitoring During GroupBy Execution

#### 1. Spark UI Monitoring (Best Option)

**Access**: http://localhost:8080

**What to Monitor**:
- **Jobs Tab**: See all Spark jobs (active, completed, failed)
- **Stages Tab**: Monitor individual stage progress
- **Executors Tab**: Check resource usage (CPU, memory, disk)
- **SQL Tab**: See SQL query execution plans and metrics
- **Environment Tab**: Verify Spark configuration

#### 2. Command Line Monitoring

```bash
# Monitor Spark logs in real-time
docker-compose -f docker-compose-developer.yml logs -f chronon-main

# Check specific Spark application
docker-compose -f docker-compose-developer.yml exec chronon-main \
    spark-submit --status <application-id>

# Monitor system resources
docker stats chronon-chronon-main-1
```

#### 3. Airflow Monitoring

- Access Airflow UI at http://localhost:8085
- Monitor DAG runs, task status, and logs
- Set up alerts for failed jobs
- Review execution times and resource usage

### Common Errors and Solutions

#### Compilation Errors
```bash
# Error: Module not found
# Solution: Check PYTHONPATH and imports
export PYTHONPATH=/srv/chronon:$PYTHONPATH

# Error: Invalid source configuration
# Solution: Verify table exists and columns are correct
spark-sql -e "DESCRIBE data.your_table;"
```

#### Backfill Errors
```bash
# Error: Out of memory
# Solution: Reduce parallelism or increase memory
python3 api/py/ai/chronon/repo/run.py --conf ... --mode backfill --ds 2023-12-01 --parallelism 1

# Error: Table not found
# Solution: Check source table exists
spark-sql -e "SHOW TABLES IN data;"
```

#### Upload Errors
```bash
# Error: MongoDB connection failed
# Solution: Check MongoDB is running
docker-compose -f docker-compose-developer.yml exec mongodb mongosh --eval "db.adminCommand('ping')"

# Error: Invalid key format
# Solution: Check key format in fetch command
python3 api/py/ai/chronon/repo/run.py --mode fetch --conf-type group_bys --name team/feature.v1 -k '{"user_id":"123"}'
```

## Best Practices

### Feature Naming Conventions

```python
# Good naming examples
Aggregation(
    input_column="purchase_amount",
    operation=Operation.SUM,
    windows=[Window(length=7, timeUnit=TimeUnit.DAYS)]
)
# Results in: purchase_amount_sum_7d

# Use descriptive names
Aggregation(
    input_column="user_engagement_score",
    operation=Operation.AVERAGE,
    windows=[Window(length=30, timeUnit=TimeUnit.DAYS)]
)
# Results in: user_engagement_score_avg_30d
```

### Testing Strategies

1. **Start Small**: Test with single day, small user set
2. **Validate Incrementally**: Check each aggregation separately
3. **Use Test Data**: Create controlled test scenarios
4. **Compare Manually**: Verify key calculations by hand
5. **Monitor Resources**: Watch Spark UI for performance issues

### Documentation Requirements

```python
# Document your GroupBy configuration
"""
User Purchase Features

This GroupBy computes user purchase aggregations for the last 1, 7, and 30 days.

Features:
- purchase_amount_sum_{window}: Total amount spent
- purchase_amount_count_{window}: Number of purchases  
- purchase_amount_avg_{window}: Average purchase amount

Source: data.purchases table
Key: user_id
Windows: 1d, 7d, 30d
"""

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[...],
    online=True,
)
```

### Version Control Workflow

```bash
# 1. Create feature branch
git checkout -b feature/user-purchase-features

# 2. Develop and test locally
# ... development work ...

# 3. Commit changes
git add group_bys/my_team/user_purchase_features.py
git commit -m "Add user purchase aggregations for 1d, 7d, 30d windows"

# 4. Test in staging environment
# ... deploy to staging ...

# 5. Create pull request
git push origin feature/user-purchase-features
```

---

This guide provides everything you need to understand Chronon's architecture, develop GroupBy features, and simulate production workflows locally. Use it as a reference for both development and production deployment.
