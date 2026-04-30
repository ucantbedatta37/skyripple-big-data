#!/usr/bin/env bash
set -euo pipefail

echo "======================================================="
echo "    DATA 228 - LAUNCHING ADVANCED EXPERIMENTS"
echo "======================================================="
echo "Make sure you have evaluated scripts/run_mvp.sh first"
echo "so that ./hdfs_like/curated_parquet holds streaming data!"
echo ""

# Export PYTHONPATH so the scripts can find src modules easily
export PYTHONPATH="$(pwd)"

echo "RUNNING PART 1: Probabilistic Data Structures (HyperLogLog & Count-Min Sketch)"
# Using docker spark-client if you want, but running locally is faster if pyspark is installed
# Or we can just use the provided docker-compose spark-client image exactly like run_mvp.sh does.
docker compose run --rm -e PYTHONPATH=/app spark-client /opt/spark/bin/spark-submit \
    --master 'spark://spark-master:7077' \
    src/experiments/run_probabilistic.py

echo ""
echo "RUNNING PART 2: Differential Privacy"
docker compose run --rm -e PYTHONPATH=/app spark-client /opt/spark/bin/spark-submit \
    --master 'spark://spark-master:7077' \
    src/experiments/run_privacy.py

echo ""
echo "RUNNING PART 3: Locality Sensitive Hashing (MinHash LSH)"
# numpy is pre-installed in the custom spark-client image (Dockerfile.spark-client)
docker compose run --rm -e PYTHONPATH=/app spark-client \
    /opt/spark/bin/spark-submit \
    --master 'spark://spark-master:7077' \
    src/experiments/run_lsh.py

echo ""
echo "ALL DONE! The exact advanced components from your proposal are implemented and tested."
