Spark ThriftServer — Quick Guide

Purpose

Start a Spark ThriftServer inside the running `spark` container so dbt (via Thrift) can connect on port 10000.

Command (detached)

```
docker exec -d spark bash -c "/opt/spark/sbin/start-thriftserver.sh --hiveconf hive.server2.thrift.port=10000"
```

What it does

- Launches ThriftServer in background (detached) inside the `spark` container.
- Binds HiveServer2 thrift port 10000 so clients (dbt, JDBC/ODBC) can connect.

Verification

- Check listening port inside container:
  docker exec spark ss -ltnp | grep 10000

- Inspect container logs for thrift startup messages:
  docker logs spark --tail 50 | grep -i thrift

- From Airflow container test dbt connection:
  docker exec airflow bash -c "dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"

Stopping or restarting

- Stop ThriftServer (inside container):
  docker exec spark pkill -f ThriftServer || true

- Or restart container:
  docker restart spark

Docker Compose hint (run streaming + thrift)

If spark currently runs a streaming job (e.g., bronze ingestion) and you want Thrift available too, use a combined command in docker-compose:

```
command: >
  bash -c "
    /opt/spark/bin/spark-submit --master local[1] \
      --driver-memory 512m --executor-memory 512m \
      /opt/spark/work/scripts/ingest_bronze.py &
    /opt/spark/sbin/start-thriftserver.sh \
      --hiveconf hive.server2.thrift.port=10000
  "
```

Caveats

- Ensure port `10000` is free on the container and not blocked by host firewall.
- ThriftServer logs to the main container logs; monitor them for errors.
- For full Nessie/Iceberg support with dbt, the dbt runtime must include the appropriate Nessie/Iceberg adapters (dbt-nessie or an alternative connector). If dbt run fails with catalog errors, either rebuild the dbt environment with the Nessie adapter or run dbt from a container that includes it.

