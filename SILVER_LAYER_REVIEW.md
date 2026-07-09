# 🔍 SILVER LAYER REVIEW - Code Quality Analysis

## ✅ WHAT'S WORKING

### 1. **Bronze Ingestion (Confirmed)**
- ✅ Kafka topic created successfully (`raw_transactions`)
- ✅ Data generator sending 9+ records
- ✅ Spark streaming to Nessie working (9 messages read, written to bronze.transactions)
- ✅ Iceberg table created in nessie.bronze.transactions

### 2. **dbt Configuration**
- ✅ Airflow has `dbt-spark[PyHive]==1.7.1` installed
- ✅ dbt profiles.yml correctly configured:
  - Host: `spark` (Docker network)
  - Port: 10000 (Thrift)
  - Catalog: `nessie` ✅
  - Schema: `silver`
- ✅ dbt_project.yml has `on-run-start: USE nessie` ✅
- ✅ sources.yml correctly references nessie.bronze.transactions
- ✅ Airflow DAG structure: debug → run (proper ordering)

### 3. **SQL Transformation Logic**
- ✅ Deduplication logic solid (ROW_NUMBER window function)
- ✅ Data cleaning (UPPER, CAST, naming)
- ✅ Timestamp enrichment (current_timestamp)
- ✅ Materialized as table (good for performance)

---

## 🔴 CRITICAL ISSUES FOUND

### Issue 1: **Missing dbt-nessie Dependency in Airflow**
**Severity:** CRITICAL 🔴

The Airflow container installs dbt-spark but may miss the Nessie adapter:

```dockerfile
# Current: Only dbt-spark[PyHive]
RUN pip install --no-cache-dir "dbt-spark[PyHive]==1.7.1"

# MISSING: Nessie connector for dbt
```

**Fix Required:**
```dockerfile
RUN pip install --no-cache-dir \
    "dbt-spark[PyHive]==1.7.1" \
    "dbt-nessie==0.21.1"  # ← MISSING
```

---

### Issue 2: **Spark Thrift Server Not Running**
**Severity:** CRITICAL 🔴

The profiles.yml uses `method: thrift` on port 10000, but the Spark container logs show **streaming mode** running, NOT a Thrift server.

```yaml
# profiles.yml expects:
method: thrift
host: spark
port: 10000
```

**Current Spark behavior:** Running `ingest_bronze.py` (streaming), NOT a Thrift server for dbt connection.

**Fix Required:**
After bronze ingestion, start Spark Thrift server:
```bash
/opt/spark/sbin/start-thriftserver.sh --hiveconf hive.server2.thrift.port=10000
```

---

### Issue 3: **Docker Compose: Spark Service Configuration Issue**
**Severity:** HIGH 🟡

The Spark container runs streaming ingestion, then may exit. dbt needs Spark running as a persistent service.

Current command:
```yaml
command: /opt/spark/bin/spark-submit --master local[1] --driver-memory 512m --executor-memory 512m /opt/spark/work/scripts/ingest_bronze.py
```

**Problem:** This script runs indefinitely as a streaming job, BUT dbt connection will time out.

**Solution:** Run both bronze ingestion AND keep Thrift server alive:
```bash
command: bash -c "
  /opt/spark/bin/spark-submit --master local[1] 
    --driver-memory 512m --executor-memory 512m 
    /opt/spark/work/scripts/ingest_bronze.py &
  /opt/spark/sbin/start-thriftserver.sh 
    --hiveconf hive.server2.thrift.port=10000
"
```

---

### Issue 4: **Airflow DAG Doesn't Wait for Spark Readiness**
**Severity:** MEDIUM 🟡

```python
depends_on:
  - spark  # ← Container started, but service may not be ready
```

Airflow starts immediately, but Thrift server needs time to initialize.

**Fix:**
```python
# Add retry delay
dbt_debug = BashOperator(
    task_id='dbt_debug_connection',
    bash_command=f'sleep 30 && dbt debug ...',  # ← Wait 30s for Thrift server
)
```

---

### Issue 5: **Missing dbt Seed/Ref Configuration**
**Severity:** MEDIUM 🟡

The silver_transactions.sql references `nessie.bronze.transactions` directly, not via `ref()`:

```sql
-- Current (works but breaks dbt lineage):
SELECT * FROM nessie.bronze.transactions

-- Should be (dbt best practice):
SELECT * FROM {{ source('nessie_source', 'transactions') }}
```

**Status:** sources.yml is defined but not used in the model.

**Fix:** Update silver_transactions.sql:
```sql
WITH raw_data AS (
    SELECT * FROM {{ source('nessie_source', 'transactions') }}
)
```

---

### Issue 6: **Potential Data Type Incompatibility**
**Severity:** LOW 🟡

The dbt model uses Spark SQL:
```sql
CAST(timestamp AS TIMESTAMP) as transaction_time,
```

If bronze.timestamp is already a TIMESTAMP, this cast is redundant. If it's STRING, ensure format is compatible.

**Verify:** Check what type timestamp is in bronze.transactions (currently STRING per ingest_bronze.py).

---

## 🔧 WHAT NEEDS TO BE FIXED

### Priority 1: Update Airflow Dockerfile
```dockerfile
USER airflow
# Install dbt-spark with Nessie support
RUN pip install --no-cache-dir \
    "dbt-spark[PyHive]==1.7.1" \
    "dbt-nessie==0.21.1"
```

### Priority 2: Update Spark Service in docker-compose
Make Spark run streaming + Thrift server simultaneously.

### Priority 3: Update Silver Transformation DAG
Add sleep delay before dbt debug:
```python
dbt_debug = BashOperator(
    task_id='dbt_debug_connection',
    bash_command=f'sleep 30 && dbt debug --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
)
```

### Priority 4: Update dbt Model
Use `source()` reference for lineage:
```sql
WITH raw_data AS (
    SELECT * FROM {{ source('nessie_source', 'transactions') }}
)
```

---

## 📊 Test Checklist

- [ ] Bronze data flowing ✅ (confirmed working)
- [ ] Kafka topic exists ✅ (confirmed)
- [ ] dbt-nessie installed in Airflow ❌
- [ ] Spark Thrift server running on :10000 ❌
- [ ] dbt debug connects successfully ❌
- [ ] dbt run completes without errors ❌
- [ ] Silver table created in nessie.silver.silver_transactions ❌

---

## 🚀 Current Status

| Component | Status | Notes |
|-----------|--------|-------|
| Bronze Ingestion | ✅ Working | Data flowing into Nessie |
| Kafka Topic | ✅ Created | `raw_transactions` exists |
| dbt-spark | ✅ Installed | PyHive mode |
| dbt-nessie | ❌ Missing | Critical dependency |
| Spark Thrift Server | ❌ Not Running | Needed for dbt connection |
| Airflow DAG | ⚠️ Ready to test | Needs fixes 1-3 |
| Overall Readiness | 🔴 NOT READY | Fix dependencies 1-2 first |

---

## 🎯 Next Steps

1. **Fix Airflow Dockerfile** - Add dbt-nessie
2. **Fix Spark docker-compose** - Run Thrift server + streaming
3. **Rebuild containers** - `docker-compose -f new-docker-compose.yml build`
4. **Test dbt connection** - Run silver DAG debug task
5. **Monitor logs** - Check for errors in dbt_debug

**Estimated time to fix:** 15-20 minutes

