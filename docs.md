# Spark Service Documentation: Setup, Challenges & Resolution

This document summarizes the steps taken to bring up the Apache Spark service in the `new-docker-compose.yml` environment, the permission challenges encountered during streaming operations to Iceberg/Nessie, and how they were solved.

---

## 1. Initial State & Setup

The infrastructure is defined in [new-docker-compose.yml](file:///home/anouar.zerrik/projects/souf-project/CID-DATA-TEST/new-docker-compose.yml). Before starting Spark, we verified that all support services were running:
- **Project Nessie** (Catalog) at `http://localhost:19120`
- **MinIO** (Object Storage) at `http://localhost:9000` (API) & `http://localhost:9001` (Console)
- **Zookeeper** & **Kafka** (Streaming Broker) at `http://localhost:9092`

We brought up the Spark service with:
```bash
docker-compose -f new-docker-compose.yml up -d spark
```

---

## 2. Problems Faced: Iceberg Write Failure

When the Spark streaming script (`test_kafka_spark.py`) attempted to write the parsed Kafka stream to the Iceberg table in Nessie/MinIO, the container exited/failed.

### Error Logs
```
py4j.protocol.Py4JJavaError: An error occurred while calling o71.toTable.
: java.io.IOException: mkdir of file:/tmp/spark-checkpoints/bronze/test_transactions failed
       at org.apache.hadoop.fs.FileSystem.primitiveMkdir(FileSystem.java:1357)
       at org.apache.hadoop.fs.DelegateToFileSystem.mkdir(DelegateToFileSystem.java:185)
       ...
```

### Root Cause Analysis
1. **Non-Root Execution:** The Spark Docker image (based on `apache/spark:3.5.3`) executes as `USER spark` (UID 185) for security.
2. **Volume Mount Permissions:** In [new-docker-compose.yml](file:///home/anouar.zerrik/projects/souf-project/CID-DATA-TEST/new-docker-compose.yml), a named volume `spark-checkpoints` is mounted to `/tmp/spark-checkpoints`:
   ```yaml
   volumes:
     - spark-checkpoints:/tmp/spark-checkpoints
   ```
3. **Directory Ownership:** When Docker mounts a new named volume to a directory that does not exist or isn't pre-configured in the image, it creates the folder inside the container with `root:root` ownership.
4. **Write Block:** Because `/tmp/spark-checkpoints` was owned by `root`, the `spark` user process could not create the checkpoint subfolders (`bronze/test_transactions`), halting the streaming write query immediately.

---

## 3. The Solution

To resolve the permission conflict, we updated the Spark image construction to pre-configure and authorize the checkpoint directory.

### Step 1: Dockerfile Update
We modified [docker-infra/spark/Dockerfile](file:///home/anouar.zerrik/projects/souf-project/CID-DATA-TEST/docker-infra/spark/Dockerfile) under the `USER root` section to create the directory and grant ownership to the `spark` user:
```dockerfile
# Create checkpoint directory and fix permissions
RUN mkdir -p /tmp/spark-checkpoints && chown -R spark:spark /tmp/spark-checkpoints
```
*Note: Because Docker copies directory permissions/ownership of the container mount-point into the named volume upon initialization, this fixes the mounted volume's permissions automatically.*

### Step 2: Build & Re-run Spark
We rebuilt the Spark image and recreated the container to apply the modifications:
```bash
docker-compose -f new-docker-compose.yml build spark
docker-compose -f new-docker-compose.yml up -d --force-recreate spark
```

---

## 4. Verification & Validation Results

After restarting the Spark service, the test script ran successfully:

1. **Successful Stream Initialization:**
   ```
   spark  | Writing stream to Nessie Iceberg Bronze table in MinIO...
   ```
2. **Successful Table Write & Query Output:**
   The streaming query finished writing, stopped cleanly, and successfully queried the Nessie Iceberg table:
   ```
   spark  | Verifying data written to Nessie Iceberg table:
   spark  | +--------------+------+-------+------+-------------------+
   spark  | |id_transaction|client|produit|prix  |timestamp          |
   spark  | +--------------+------+-------+------+-------------------+
   spark  | |123           |Anouar|Laptop |1200.0|2026-07-09 00:00:00|
   spark  | +--------------+------+-------+------+-------------------+
   ```

---

## 5. Data Generator Setup & Resolution

To feed streaming transaction data into Kafka, we ran the `data-generator` service.

### Problem Encountered
In the data generator script [scripts/data_generator.py](file:///home/anouar.zerrik/projects/souf-project/CID-DATA-TEST/scripts/data_generator.py), the client was configured with:
```python
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    ...
)
```
- **Port Mismatch:** Inside the Docker network (`data-network`), containers communicate using the internal listener `kafka:9092`. The port `29092` is the external port intended for host-to-broker communication.
- **Indefinite Blocking:** Since Kafka returned `localhost:29092` as the advertised listener to the containerized client, the producer blocked indefinitely while trying to connect back to localhost.

### Resolution
1. We changed the bootstrap servers config in [scripts/data_generator.py](file:///home/anouar.zerrik/projects/souf-project/CID-DATA-TEST/scripts/data_generator.py) to point to the internal broker port:
   ```python
   producer = KafkaProducer(
       bootstrap_servers=['kafka:9092'],
       ...
   )
   ```
2. Rebuilt the data-generator image and restarted the container:
   ```bash
   docker-compose -f new-docker-compose.yml build data-generator
   docker-compose -f new-docker-compose.yml up -d --force-recreate data-generator
   ```
3. Checking `docker-compose logs data-generator` confirmed that it successfully writes mock transaction data to Kafka without errors:
   ```
   data_generator  | Starting data generation... Sending to Kafka:9092
   data_generator  | Sent: Ecran | 3505.0 DH
   data_generator  | Sent: Clavier | 1444.0 DH
   ```
