# Chronon GroupBy Bootcamp

## Overview

This bootcamp is designed for **end users** who want to learn Chronon GroupBy features through hands-on practice. It provides step-by-step commands to get you up and running quickly with minimal complexity.

## What You'll Learn

- Create your first GroupBy feature
- Run backfill computations
- Upload features to online store
- Test online serving
- Validate your results

## Prerequisites

- Docker installed and running
- Basic command-line knowledge
- No prior Chronon experience required

## Quick Start

### 1. Setup Environment

```bash
# Clone Chronon repository
git clone git@github.com:airbnb/chronon.git
cd chronon/affirm

# Start the bootcamp environment (minimal setup)
./setup-chronon-bootcamp.sh
```

Wait for the setup to complete. You should see:
```
✅ Sample data has been copied to MinIO and is ready for GroupBy development!
📊 Parquet files available at: s3a://chronon/warehouse/data/
📁 purchases.parquet (~155 records), users.parquet (100 records)
```

### 2. Verify Services

```bash
# Check all services are running
docker-compose -f docker-compose-bootcamp.yml ps
```

All containers should show "Up" status.

## Hands-On Tutorial

### Step 1: Setup Team Information

Before creating your GroupBy, you need to setup team information:

```bash
# Create teams.json file in the root directory
cat > teams.json << 'EOF'
{
    "bootcamp": {
        "description": "Bootcamp team for learning Chronon",
        "namespace": "default",
        "user": "bootcamp_user",
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

### Step 2: Create Your First GroupBy

Create a new GroupBy configuration:

```bash
# Create the configuration directory
mkdir -p group_bys/bootcamp

# Create your GroupBy configuration
cat > group_bys/bootcamp/user_purchase_features.py << 'EOF'
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit

# Define the source using sample data
source = Source(
    events=EventSource(
        table="purchases",  # Sample purchase data
        query=Query(
            selects=select("user_id", "purchase_price", "item_category"),
            time_column="ts"
        )
    )
)

# Define time windows
window_sizes = [
    Window(length=1, timeUnit=TimeUnit.DAYS),    # 1 day
    Window(length=7, timeUnit=TimeUnit.DAYS),    # 7 days
]

# Create the GroupBy configuration
v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
    ],
    online=True,
)
EOF
```

### Step 3: Compile Your Configuration

```bash
# Compile the GroupBy configuration
python3 api/py/ai/chronon/repo/compile.py \
    --conf group_bys/bootcamp/user_purchase_features.py \
    --force-overwrite
```

You should see compilation success message.

### Step 4: Run Backfill

```bash
# Run backfill for December 1st, 2023
python3 api/py/ai/chronon/repo/run.py \
    --conf production/group_bys/bootcamp/user_purchase_features.v1 \
    --mode backfill \
    --ds 2023-12-01 \
    --repo /srv/chronon
```

**Monitor Progress**: Open http://localhost:8080 to watch your job in Spark UI.

### Step 5: Check Your Results

```bash
# Access Spark SQL to check results
docker-compose -f docker-compose-bootcamp.yml exec chronon-main spark-sql
```

Inside Spark SQL:
```sql
-- List your tables
SHOW TABLES LIKE '*bootcamp*';

-- Check your computed features
SELECT * FROM default.bootcamp_user_purchase_features_v1 
WHERE ds = '2023-12-01' 
LIMIT 10;

-- Exit Spark SQL
EXIT;
```

### Step 6: Upload to Online Store

```bash
# Upload features to MongoDB for online serving
python3 api/py/ai/chronon/repo/run.py \
    --conf production/group_bys/bootcamp/user_purchase_features.v1 \
    --mode upload \
    --ds 2023-12-01 \
    --repo /srv/chronon
```

### Step 7: Test Online Serving

```bash
# Test fetching features for a specific user
python3 api/py/ai/chronon/repo/run.py \
    --mode fetch \
    --conf-type group_bys \
    --name bootcamp/user_purchase_features.v1 \
    -k '{"user_id":"user_1"}' \
    --repo /srv/chronon
```

You should see JSON output with feature values like:
```json
{
  "user_id": "user_1",
  "purchase_price_sum_1d": 45.67,
  "purchase_price_count_1d": 3,
  "purchase_price_avg_1d": 15.22,
  "purchase_price_sum_7d": 123.45,
  "purchase_price_count_7d": 8,
  "purchase_price_avg_7d": 15.43
}
```

## Validation Exercise

### Step 8: Manual Verification

Let's verify your results are correct:

```bash
# Check the source data for user_1 on 2023-12-01
docker-compose -f docker-compose-bootcamp.yml exec chronon-main spark-sql
```

```sql
-- Load sample data
CREATE OR REPLACE TEMPORARY VIEW purchases AS
SELECT * FROM parquet.`s3a://chronon/warehouse/data/purchases/purchases.parquet`;

-- Check user_1's purchases on 2023-12-01
SELECT 
    user_id,
    purchase_price,
    ts
FROM purchases 
WHERE user_id = 'user_1' 
AND DATE(ts) = '2023-12-01'
ORDER BY ts;

-- Manual calculation
SELECT 
    user_id,
    SUM(purchase_price) as manual_sum_1d,
    COUNT(*) as manual_count_1d,
    AVG(purchase_price) as manual_avg_1d
FROM purchases 
WHERE user_id = 'user_1' 
AND DATE(ts) = '2023-12-01'
GROUP BY user_id;

EXIT;
```

Compare the manual calculation with your GroupBy results - they should match!

## Next Steps

### Try Different Aggregations

Modify your GroupBy configuration to try different operations:

```python
# Add to your aggregations list
Aggregation(
    input_column="purchase_price",
    operation=Operation.MAX,  # Maximum purchase
    windows=window_sizes
),
Aggregation(
    input_column="purchase_price",
    operation=Operation.MIN,  # Minimum purchase
    windows=window_sizes
),
```

### Try Different Windows

```python
# Add longer windows
window_sizes = [
    Window(length=1, timeUnit=TimeUnit.DAYS),    # 1 day
    Window(length=7, timeUnit=TimeUnit.DAYS),    # 7 days
    Window(length=30, timeUnit=TimeUnit.DAYS),   # 30 days
    Window(length=90, timeUnit=TimeUnit.DAYS),   # 90 days
]
```

### Run Date Ranges

```bash
# Run backfill for multiple days
python3 api/py/ai/chronon/repo/run.py \
    --conf production/group_bys/bootcamp/user_purchase_features.v1 \
    --mode backfill \
    --start-ds 2023-12-01 \
    --end-ds 2023-12-07 \
    --repo /srv/chronon
```

## Troubleshooting

### Common Issues

**Compilation Error**:
```bash
# Make sure you're in the chronon directory
cd /path/to/chronon
python3 api/py/ai/chronon/repo/compile.py --conf group_bys/bootcamp/user_purchase_features.py --force-overwrite
```

**Backfill Error**:
```bash
# Check if Spark is running
docker-compose -f docker-compose-bootcamp.yml ps spark-master

# Check Spark UI
open http://localhost:8080
```

**No Results**:
```bash
# Check if your table was created
docker-compose -f docker-compose-bootcamp.yml exec chronon-main spark-sql -e "SHOW TABLES LIKE '*bootcamp*';"
```

### Getting Help

- **Spark UI**: http://localhost:8080 - Monitor job execution
- **MinIO Console**: http://localhost:9001 - Check data storage
- **Jupyter**: http://localhost:8888 - Interactive data exploration

## Congratulations! 🎉

You've successfully completed the Chronon GroupBy bootcamp! You now know how to:

- ✅ Create GroupBy configurations
- ✅ Compile and run backfill jobs
- ✅ Upload features to online store
- ✅ Test online serving
- ✅ Validate your results

## What's Next?

- **Explore More**: Try different aggregations and windows
- **Production**: Learn about Airflow orchestration in the Developer Guide
- **Advanced**: Study the full Developer Guide for production workflows
- **Join Community**: Connect with other Chronon users

Happy feature engineering! 🚀
