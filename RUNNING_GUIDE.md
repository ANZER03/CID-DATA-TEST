# Setup & Running Guide

This guide provides a step-by-step walkthrough to start and initialize all the platform services in the correct order using `new-docker-compose.yml`.

---

## Prerequisites

Before starting, ensure you have:
1. **Docker** and **Docker Compose** installed.
2. Port availability for:
   - Zookeeper/Kafka: `2181`, `9092`
   - Nessie: `19120`
   - MinIO: `9000`, `9001`
   - Spark: `10000`, `8081`
   - Airflow: `18084`
   - Trino: `18081`

---

## Step-by-Step Deployment Order

Follow these steps exactly in sequence to ensure all service dependencies resolve successfully.

### 1. Start Zookeeper & Kafka (including Kafka initialization)
Start the message broker services and initialize the topic:
```bash
docker compose -f new-docker-compose.yml up -d zookeeper kafka kafka-init
```
*Note: Kafka will automatically wait for Zookeeper to be healthy. The helper container `kafka-init` creates the required `raw_transactions` topic.*

---

### 2. Start Nessie & MinIO (including MinIO initialization)
Next, bring up the metadata catalog (Nessie) and object storage (MinIO), and initialize the storage buckets:
```bash
docker compose -f new-docker-compose.yml up -d nessie minio minio-init
```
*Note: The helper container `minio-init` will run to initialize the required bucket namespaces (`bronze`, `silver`, `gold`).*

---

### 3. Start Data Generator
Start the data generator container to begin streaming mock transaction records into Kafka:
```bash
docker compose -f new-docker-compose.yml up -d data-generator
```

---

### 4. Install JAR Dependencies
Download all required Java/Scala archive (`.jar`) dependencies (Spark SQL Kafka integration, Iceberg runtime, Nessie Spark extensions, AWS/Hadoop libraries) by executing the helper script on your host system:

* **Linux / macOS:**
  ```bash
  chmod +x scripts/download_jars.sh
  ./scripts/download_jars.sh
  ```

* **Windows (Git Bash):**
  ```bash
  bash scripts/download_jars.sh
  ```

* **Windows (PowerShell):**
  ```powershell
  .\scripts\download_jars.ps1
  ```

---

### 5. Start Apache Spark
Build and start the Spark processing engine. When the Spark container starts, it will copy the downloaded JARs into `/opt/spark/jars/` and launch the bronze ingestion script:
```bash
docker compose -f new-docker-compose.yml build spark
docker compose -f new-docker-compose.yml up -d spark
```

---

### 6. Start the Spark Thrift Server
Before starting the Airflow orchestrator, you must initialize the Spark Hive Thrift Server inside the Spark container. This enables JDBC/Thrift clients (like Trino and Airflow) to run queries on the Spark context:
```bash
docker exec -d spark bash -c "/opt/spark/sbin/start-thriftserver.sh --hiveconf hive.server2.thrift.port=10000"
```

---

### 7. Start Apache Airflow
Build and start the Airflow orchestrator (the image now includes Spark + all JARs for the silver job):
```bash
docker-compose -f new-docker-compose.yml build airflow
docker-compose -f new-docker-compose.yml up -d airflow
```

#### 7.1 Create the Airflow Admin User
After the container starts, create a user to log in to the Airflow UI:
```bash
docker exec airflow airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```
To retrieve the auto-generated password from Airflow standalone logs:
```bash
docker logs airflow 2>&1 | grep -i "password"
```
*Access the UI at **http://localhost:18084** with the username and password shown in the output.*

---

### 8. Start Trino Query Engine
Finally, start the Trino SQL query engine:
```bash
docker compose -f new-docker-compose.yml up -d trino
```

---

## Verification Commands

To check the logs and ensure everything is running correctly:

* **Check running containers:**
  ```bash
  docker compose -f new-docker-compose.yml ps
  ```
* **Verify Kafka streaming data ingestion:**
  ```bash
  docker compose -f new-docker-compose.yml logs -f data-generator
  ```
* **Verify Spark streaming output:**
  ```bash
  docker compose -f new-docker-compose.yml logs -f spark
  ```
