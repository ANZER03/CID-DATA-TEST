#!/usr/bin/env bash

set -euo pipefail

JARS_DIR="./jars"
mkdir -p "$JARS_DIR"

echo "Downloading Spark dependencies using Huawei Cloud mirror..."

URLS=(
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.76.3/nessie-spark-extensions-3.5_2.12-0.76.3.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar"
    "https://mirrors.huaweicloud.com/repository/maven/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    "https://mirrors.huaweicloud.com/repository/maven/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
)

# Download in parallel
for url in "${URLS[@]}"; do
    filename=$(basename "$url")
    if [ ! -f "$JARS_DIR/$filename" ]; then
        echo "Downloading $filename..."
        curl -sL "$url" -o "$JARS_DIR/$filename" &
    else
        echo "$filename already exists, skipping."
    fi
done

wait
echo "All downloads completed!"
