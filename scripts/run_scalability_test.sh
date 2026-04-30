#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

###############################################################################
# DATA 228 — Experiment 1: Scalability Test across Tiers
#
# Runs the full pipeline (Kafka produce → Spark Streaming ETL → GraphX →
# Advanced experiments) on three data tiers and captures metrics:
#   - Wall-clock latency (seconds)
#   - Throughput (records/sec)
#   - Output Parquet size
#   - GraphX graph stats
#
# Usage:
#   bash scripts/run_scalability_test.sh           # run all 3 tiers
#   TIER=2 bash scripts/run_scalability_test.sh    # run only tier 2
###############################################################################

TOPIC="${KAFKA_TOPIC_FLIGHT_RAW:-flight_raw}"
BOOTSTRAP_IN_DOCKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
RESULTS_DIR="./evidence/scalability"
mkdir -p "$RESULTS_DIR"

# ─── Tier definitions ────────────────────────────────────────────────────────
# Tier 1: 1 month   (~540K rows)  — Algorithm validation
# Tier 2: 6 months  (~3.5M rows)  — Mid-scale benchmarking
# Tier 3: 8 months  (~4.7M rows)  — Scalability testing (all data we have)
TIER1_FILES="./flight/T_ONTIME_REPORTING_Jan2025.csv"

TIER2_FILES=(
  "./flight/T_ONTIME_REPORTING_Jan2025.csv"
  "./flight/T_ONTIME_REPORTING_Feb2025.csv"
  "./flight/T_ONTIME_REPORTING_Mar2025.csv"
  "./flight/T_ONTIME_REPORTING_Apr2025.csv"
  "./flight/T_ONTIME_REPORTING_May2025.csv"
  "./flight/T_ONTIME_REPORTING_Jun2025.csv"
)

TIER3_FILES=(
  "./flight/T_ONTIME_REPORTING_Jan2025.csv"
  "./flight/T_ONTIME_REPORTING_Feb2025.csv"
  "./flight/T_ONTIME_REPORTING_Mar2025.csv"
  "./flight/T_ONTIME_REPORTING_Apr2025.csv"
  "./flight/T_ONTIME_REPORTING_May2025.csv"
  "./flight/T_ONTIME_REPORTING_Jun2025.csv"
  "./flight/T_ONTIME_REPORTING_Jul2025.csv"
  "./flight/T_ONTIME_REPORTING_Aug2025.csv"
  "./flight/T_ONTIME_REPORTING_Sep2025.csv"
  "./flight/T_ONTIME_REPORTING_Oct2025.csv"
  "./flight/T_ONTIME_REPORTING_Nov2025.csv"
  "./flight/T_ONTIME_REPORTING_Dec2025.csv"
)

# ─── Helper functions ────────────────────────────────────────────────────────
now_epoch() { python3 -c "import time; print(time.time())"; }
elapsed() { python3 -c "print(round($2 - $1, 2))"; }

ensure_topic() {
  echo "[scalability] ensuring kafka topic: $TOPIC"
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || true
}

delete_topic() {
  echo "[scalability] deleting kafka topic to start fresh"
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --delete --if-exists \
    --topic "$TOPIC" 2>/dev/null || true
  sleep 2
}

clean_outputs() {
  echo "[scalability] cleaning output + checkpoint dirs"
  rm -rf ./hdfs_like/curated_parquet || true
  rm -rf ./hdfs_like/checkpoints/streaming_etl || true
  rm -rf ./hdfs_like/graphx_results || true
  chmod -R a+rwx ./hdfs_like 2>/dev/null || true
}

produce_files() {
  local desc="$1"
  shift
  local files=("$@")
  echo "[scalability] producing $desc into Kafka (${#files[@]} files)..."
  for f in "${files[@]}"; do
    echo "  → $(basename $f)"
    python3 -m src.producer.bts_csv_to_json_stream \
      --input-csv "$f" \
      --limit 999999999 \
      --duplicate-rate 0.05 \
      --seed 42 \
    | docker compose exec -T kafka kafka-console-producer \
      --bootstrap-server kafka:9092 \
      --topic "$TOPIC" \
      --property acks=1 \
      >/dev/null
  done
}

run_streaming_etl() {
  echo "[scalability] running spark structured streaming ETL"
  docker compose run -T --rm \
      -e PYTHONPATH=/app \
      -e BF_M_BITS=100000000 \
      -e BF_K_HASHES=7 \
      spark-client /opt/spark/bin/spark-submit \
      --master 'spark://spark-master:7077' \
      --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' \
      --conf 'spark.sql.shuffle.partitions=8' \
      --conf 'spark.jars.ivy=/tmp/ivy' \
      --conf 'spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy' \
      --conf 'spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/ivy' \
      --conf 'spark.executor.memory=3g' \
      --conf 'spark.driver.memory=1g' \
      --conf 'spark.driver.maxResultSize=1g' \
      --conf 'spark.network.timeout=800s' \
      --conf 'spark.executor.heartbeatInterval=60s' \
      src/streaming/structured_streaming_etl.py \
      --topic "$TOPIC" \
      --bootstrap-servers "$BOOTSTRAP_IN_DOCKER" \
      --run-mode available_now \
      --checkpoint-dir ./hdfs_like/checkpoints/streaming_etl \
      --output-dir ./hdfs_like/curated_parquet 2>&1 | tee /tmp/spark_etl_output.log
}

run_graphx() {
  echo "[scalability] running GraphX PageRank + Connected Components"
  docker compose run -T --rm spark-client /opt/spark/bin/spark-shell \
    --master 'spark://spark-master:7077' \
    -i src/graphx/graphx_page_rank_cc.scala 2>&1 | tee /tmp/graphx_output.log
}

run_experiments() {
  echo "[scalability] running advanced experiments (HLL, CMS, DP, LSH)"
  export PYTHONPATH="$(pwd)"
  docker compose run -T --rm -e PYTHONPATH=/app spark-client /opt/spark/bin/spark-submit \
      --master 'spark://spark-master:7077' \
      --conf 'spark.executor.memory=3g' \
      --conf 'spark.network.timeout=800s' \
      --conf 'spark.executor.heartbeatInterval=60s' \
      --conf 'spark.ui.enabled=false' \
      src/experiments/run_probabilistic.py 2>&1 | tee /tmp/exp_prob_output.log

  docker compose run -T --rm -e PYTHONPATH=/app spark-client /opt/spark/bin/spark-submit \
      --master 'spark://spark-master:7077' \
      --conf 'spark.executor.memory=3g' \
      --conf 'spark.network.timeout=800s' \
      --conf 'spark.executor.heartbeatInterval=60s' \
      --conf 'spark.ui.enabled=false' \
      src/experiments/run_privacy.py 2>&1 | tee /tmp/exp_priv_output.log

  docker compose run -T --rm -e PYTHONPATH=/app spark-client \
      /opt/spark/bin/spark-submit \
      --master 'spark://spark-master:7077' \
      --conf 'spark.executor.memory=3g' \
      --conf 'spark.network.timeout=800s' \
      --conf 'spark.executor.heartbeatInterval=60s' \
      --conf 'spark.ui.enabled=false' \
      src/experiments/run_lsh.py 2>&1 | tee /tmp/exp_lsh_output.log
}

collect_metrics() {
  local tier="$1"
  local t_produce="$2"
  local t_etl="$3"
  local t_graphx="$4"
  local t_experiments="$5"
  local total_time="$6"

  local parquet_size="N/A"
  if [ -d "./hdfs_like/curated_parquet" ]; then
    parquet_size=$(du -sh ./hdfs_like/curated_parquet | awk '{print $1}')
  fi

  local graphx_size="N/A"
  if [ -d "./hdfs_like/graphx_results" ]; then
    graphx_size=$(du -sh ./hdfs_like/graphx_results | awk '{print $1}')
  fi

  # Extract key stats from logs
  local etl_stats=$(grep "\[stream-etl\]" /tmp/spark_etl_output.log 2>/dev/null | tail -1 || echo "N/A")
  local graphx_stats=$(grep "\[graphx\]" /tmp/graphx_output.log 2>/dev/null | tail -1 || echo "N/A")

  # Extract accepted record count for throughput calc
  local accepted=$(echo "$etl_stats" | grep -o "accepted=[0-9]*" | head -1 | cut -d= -f2 || echo "0")
  local throughput="N/A"
  if [ -n "$accepted" ] && [ "$accepted" != "0" ]; then
    throughput=$(python3 -c "print(round(int('$accepted') / float('$t_etl'), 1))" 2>/dev/null || echo "N/A")
  fi

  local log_file="$RESULTS_DIR/tier${tier}_results.log"
  cat > "$log_file" <<EOF
================================================================================
DATA 228 - SCALABILITY TEST RESULTS — TIER $tier
================================================================================
Timestamp:            $(date)

--- Timing (seconds) ---
Kafka Produce:        ${t_produce}s
Spark Streaming ETL:  ${t_etl}s
GraphX Analytics:     ${t_graphx}s
Experiments (HLL/CMS/DP/LSH): ${t_experiments}s
TOTAL Wall-Clock:     ${total_time}s

--- Throughput ---
Records accepted:     $accepted
ETL Throughput:       ${throughput} records/sec

--- Output Sizes ---
Curated Parquet:      $parquet_size
GraphX Results:       $graphx_size

--- ETL Stats ---
$etl_stats

--- GraphX Stats ---
$graphx_stats

================================================================================
EOF

  echo ""
  echo "============================================================"
  echo "  TIER $tier RESULTS SUMMARY"
  echo "============================================================"
  cat "$log_file"
  echo "Results saved to: $log_file"
}

# ─── Main: run a single tier ────────────────────────────────────────────────
run_tier() {
  local tier=$1
  shift
  local desc="$1"
  shift
  local files=("$@")

  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║  TIER $tier: $desc"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo ""

  # Step 1: Produce into Kafka
  local t0=$(now_epoch)
  if [ "${SKIP_PRODUCE:-0}" != "1" ]; then
    delete_topic
    ensure_topic
    produce_files "$desc" "${files[@]}"
    clean_outputs
  else
    echo "[scalability] skipping kafka produce due to SKIP_PRODUCE=1"
    echo "[scalability] keeping existing checkpoints and outputs to RESUME safely..."
  fi
  local t1=$(now_epoch)
  local dt_produce=$(elapsed $t0 $t1)

  # Step 2: Spark Streaming ETL
  local t2=$(now_epoch)
  run_streaming_etl
  local t3=$(now_epoch)
  local dt_etl=$(elapsed $t2 $t3)

  # Step 3: GraphX
  local t4=$(now_epoch)
  run_graphx
  local t5=$(now_epoch)
  local dt_graphx=$(elapsed $t4 $t5)

  # Step 4: Advanced Experiments
  local t6=$(now_epoch)
  run_experiments
  local t7=$(now_epoch)
  local dt_experiments=$(elapsed $t6 $t7)

  local dt_total=$(elapsed $t0 $t7)

  # Collect and display metrics
  collect_metrics "$tier" "$dt_produce" "$dt_etl" "$dt_graphx" "$dt_experiments" "$dt_total"
}

# ─── Entry point ─────────────────────────────────────────────────────────────
echo "=========================================================="
echo "  DATA 228 — EXPERIMENT 1: SCALABILITY BENCHMARK"
echo "  Flight Delay Propagation Pipeline"
echo "=========================================================="
echo "Started at: $(date)"
echo ""

RUN_TIER="${TIER:-all}"

if [ "$RUN_TIER" = "1" ] || [ "$RUN_TIER" = "all" ]; then
  run_tier 1 "1 month (~540K rows)" "$TIER1_FILES"
fi

if [ "$RUN_TIER" = "2" ] || [ "$RUN_TIER" = "all" ]; then
  run_tier 2 "6 months (~3.5M rows)" "${TIER2_FILES[@]}"
fi

if [ "$RUN_TIER" = "3" ] || [ "$RUN_TIER" = "all" ]; then
  run_tier 3 "12 months (~7.2M rows)" "${TIER3_FILES[@]}"
fi

# ─── Summary comparison ─────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  SCALABILITY TEST COMPLETE                               ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "Results saved in: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"
echo ""
echo "Finished at: $(date)"
