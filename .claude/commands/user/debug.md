You are helping a Chronon user debug issues with their feature definitions.

## Key Files to Reference

- **Compilation**: `api/py/ai/chronon/repo/compile.py`
- **Execution**: `api/py/ai/chronon/repo/run.py`
- **Validation**: `api/py/ai/chronon/repo/validator.py`

## Common Error Categories

### 1. Compilation Errors (compile.py)

**Symptoms**: Errors when running `compile.py --conf=...`

| Error | Cause | Fix |
|-------|-------|-----|
| Missing column | Selected column doesn't exist in source | Check source table schema |
| Type mismatch | Aggregation incompatible with column type | Use appropriate operation for data type |
| Invalid window | Window < 1 hour or wrong TimeUnit | Use `TimeUnit.HOURS` or `TimeUnit.DAYS` |
| Duplicate name | GroupBy/Join name already exists | Use unique names or version suffix |
| Invalid key | Key column not in selects | Add key to `selects` in Query |

**Debugging**:
```bash
compile.py --conf=path/to/config.py --debug
```

### 2. Backfill Errors (run.py --mode backfill)

**Symptoms**: Spark job failures during backfill

| Error | Cause | Fix |
|-------|-------|-----|
| Schema mismatch | Source table schema changed | Update selects to match current schema |
| Partition not found | Date partition missing | Check `--ds` date has data |
| OOM / Memory errors | Insufficient executor memory | Increase in env settings |
| Key mismatch | Join keys don't align | Verify key_mapping in JoinParts |
| Time column error | Invalid time_column expression | Ensure returns Long (milliseconds) |

**Debugging**:
```bash
# Analyze config first
run.py --mode analyze --conf production/joins/team/join.v1 --enable-hitter

# Check Spark logs for detailed errors
run.py --mode backfill --conf ... --ds 2024-01-01 2>&1 | tee backfill.log
```

### 3. Upload Errors (run.py --mode upload)

**Symptoms**: Failures when uploading to KV store

| Error | Cause | Fix |
|-------|-------|-----|
| Connection failed | KV store unreachable | Check online store configuration |
| Serialization error | Type not Avro-compatible | Use supported data types |
| Key too large | Key exceeds store limits | Simplify key structure |

### 4. Online/Offline Consistency Issues

**Symptoms**: Different values in training vs serving

| Issue | Cause | Fix |
|-------|-------|-----|
| Time mismatch | Wrong time_column precision | Ensure millisecond timestamps |
| Accuracy mismatch | TEMPORAL vs SNAPSHOT confusion | Check accuracy setting |
| Streaming lag | Real-time updates delayed | Allow propagation time |
| Schema drift | Online/offline schemas diverged | Re-upload with matching schemas |

**Debugging**:
```bash
# Compare online vs offline values
run.py --mode consistency-metrics-compute --conf production/joins/team/join.v1

# Test online fetch directly
run.py --mode fetch --type join --name team/join.v1 --key-json '{"user_id":"123"}'
```

## Debugging Commands

```bash
# Validate configuration
compile.py --conf=path/to/config.py --debug

# Analyze (schema validation, row counts, heavy hitters)
run.py --mode analyze --conf production/joins/team/join.v1 --enable-hitter

# Test backfill on small date range
run.py --mode backfill --conf production/joins/team/join.v1 --ds 2024-01-01 --end-ds 2024-01-02

# Check consistency metrics
run.py --mode consistency-metrics-compute --conf production/joins/team/join.v1

# Test online fetch
run.py --mode fetch --type join --name team/join.v1 --key-json '{"user_id":"123"}'
run.py --mode fetch --type group-by --name team/purchases.v1 --key-json '{"user_id":"123"}'
```

## Memory Tuning

If encountering OOM errors, add env overrides:
```python
v1 = GroupBy(
    ...,
    env={
        "backfill": {
            "EXECUTOR_MEMORY": "16G",
            "EXECUTOR_CORES": "4",
            "DRIVER_MEMORY": "8G",
        }
    }
)
```

## When Helping Users Debug

1. **Ask for the full error message** - Often contains the specific cause
2. **Identify the stage** - Compile, backfill, upload, or serve?
3. **Check recent changes** - What changed since it last worked?
4. **Verify source data** - Is the source table accessible with expected schema?
5. **Test incrementally** - Start with analyze mode before full backfill

Read the key files listed above and examine error messages to provide specific fixes.
