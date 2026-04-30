#!/usr/bin/env bash
set -euo pipefail

TOPIC="${KAFKA_TOPIC_FLIGHT_RAW:-flight_raw}"
BOOTSTRAP_IN_DOCKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

echo "[mvp] creating kafka topic: $TOPIC"
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1

echo "[mvp] running kafka producer (finite stream)"
export BTS_CSV_PATH="${BTS_CSV_PATH:-./flight/T_ONTIME_REPORTING_Jan2025.csv}"

python3 -m src.producer.bts_csv_to_json_stream \
  --input-csv "$BTS_CSV_PATH" \
  --limit "${MVP_NUM_RECORDS:-5000}" \
  --duplicate-rate "${MVP_DUPLICATE_RATE:-0.25}" \
  --seed "${MVP_SEED:-42}" \
  ${MVP_SHUFFLE:+--shuffle} \
| docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --property acks=1 \
  >/dev/null

echo "[mvp] cleaning output + checkpoint dirs"
rm -rf ./hdfs_like/curated_parquet || true
rm -rf ./hdfs_like/checkpoints/streaming_etl || true

echo "[mvp] running spark structured streaming ETL (process available backlog once)"
chmod -R a+rwx ./hdfs_like || true
docker compose run --rm -e PYTHONPATH=/app spark-client /opt/spark/bin/spark-submit \
    --master 'spark://spark-master:7077' \
    --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
    --conf 'spark.sql.shuffle.partitions=2' \
    --conf 'spark.jars.ivy=/tmp/ivy' \
    --conf 'spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy' \
    --conf 'spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/ivy' \
    src/streaming/structured_streaming_etl.py \
    --topic "$TOPIC" \
    --bootstrap-servers "$BOOTSTRAP_IN_DOCKER" \
    --run-mode available_now \
    --checkpoint-dir ./hdfs_like/checkpoints/streaming_etl \
    --output-dir ./hdfs_like/curated_parquet

echo "[mvp] running GraphX PageRank + Connected Components"
docker compose run --rm spark-client /opt/spark/bin/spark-shell \
  --master 'spark://spark-master:7077' \
  -i src/graphx/graphx_page_rank_cc.scala

echo "[mvp] done. Check output: ./hdfs_like/curated_parquet"

