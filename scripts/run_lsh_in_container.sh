#!/usr/bin/env bash
set -euo pipefail
# Install numpy (required by pyspark.ml / MinHashLSH)
pip install --quiet numpy
# Now run the LSH experiment
export PYTHONPATH=/app
exec /opt/spark/bin/spark-submit \
    --master "spark://spark-master:7077" \
    src/experiments/run_lsh.py
