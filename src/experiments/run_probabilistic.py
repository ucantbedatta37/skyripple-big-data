import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import our custom modules
from src.approx.hyperloglog import HyperLogLog
from src.approx.count_min_sketch import CountMinSketch

def run_probabilistic_experiments():
    print("="*60)
    print("EXPERIMENT 1: PROBABILISTIC DATA STRUCTURES (HLL & CMS)")
    print("="*60)
    
    # 1. Initialize Spark
    spark = SparkSession.builder \
        .appName("ProbabilisticExperiments") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
        
    parquet_path = "./hdfs_like/curated_parquet/"
    if not os.path.exists(parquet_path):
        print(f"Error: Cannot find curated data at {parquet_path}.")
        print("Please make sure you have run the MVP pipeline first!")
        spark.stop()
        return

    print(f"Loading curated parquet dataset from: {parquet_path}")
    df = spark.read.parquet(parquet_path)
    total_records = df.count()
    print(f"Total flights loaded: {total_records}")

    # ==========================================================
    # A. HYPERLOGLOG: Estimate Distinct Routes (Origin -> Dest)
    # ==========================================================
    print("\n--- [A] HyperLogLog: Distinct Flight Routes ---")
    hll = HyperLogLog(p=12)  # 2^12 = 4096 registers
    
    # We will collect the origin and dest to simulate a stream locally, 
    # since our HLL is a custom Python class.
    # Note: For massive scale, we'd use mapPartitions, but collecting is fine for the MVP subset.
    routes_df = df.select("origin", "destination").na.drop()
    print("Collecting routes for HLL processing (10% sample to avoid OOM)...")
    routes_rows = routes_df.sample(fraction=0.1, seed=42).collect()
    
    start_time = time.time()
    for row in routes_rows:
        route_str = f"{row.origin}-{row.destination}"
        hll.add(route_str)
    
    hll_estimate = hll.estimate()
    hll_time = time.time() - start_time
    
    # Calculate EXACT distinct count using Spark for comparison
    start_time = time.time()
    exact_distinct = routes_df.withColumn("route", F.concat_ws("-", "origin", "destination")).select("route").distinct().count()
    exact_time = time.time() - start_time
    
    error_pct = abs(exact_distinct - hll_estimate) / exact_distinct * 100
    
    print(f"Exact Distinct Routes:   {exact_distinct} (took {exact_time:.4f}s)")
    print(f"HLL Estimated Routes:    {hll_estimate:.2f} (took {hll_time:.4f}s)")
    print(f"HLL Error Margin:        {error_pct:.2f}%")

    # ==========================================================
    # B. COUNT-MIN SKETCH: Estimate Flight Frequencies per Airline
    # ==========================================================
    print("\n--- [B] Count-Min Sketch: Airline Flight Frequencies ---")
    cms = CountMinSketch(width=1000, depth=5)
    
    tail_df = df.select("tail_number").na.drop()
    print("Collecting tail_numbers (10% sample to avoid OOM)...")
    tail_rows = tail_df.sample(fraction=0.1, seed=42).collect()
    
    print("Streaming tail_number occurrences into Count-Min Sketch...")
    for row in tail_rows:
        cms.update(row.tail_number, count=1)
        
    # Get top 5 tail_numbers exact counts to compare
    top_tails = tail_df.groupBy("tail_number").count().orderBy(F.desc("count")).limit(5).collect()
    
    print(f"{'Tail #':<12} | {'Exact Count':<15} | {'CMS Estimate':<15} | {'Error Amount':<15}")
    print("-" * 65)
    for r in top_tails:
        tail_code = r.tail_number
        exact_count = r["count"]
        cms_est = cms.estimate(tail_code)
        err = abs(exact_count - cms_est)
        print(f"{tail_code:<12} | {exact_count:<15} | {cms_est:<15} | {err:<15}")

    print("\n[C] Sweeping CMS Width (Memory) vs. Average Error Rate")
    # Higher width -> More memory -> Lower hash collisions -> Higher Accuracy (Lower Error)
    widths = [10, 50, 100, 250, 500, 1000, 2000]
    cms_sweep_results = []
    
    # Pre-select top 100 tails for a representative study
    all_tails_df = tail_df.groupBy("tail_number").count().orderBy(F.desc("count")).limit(100)
    all_tails_exact = {r.tail_number: r["count"] for r in all_tails_df.collect()}
    
    print(f"{'CMS Width':<15} | {'Memory (bytes)':<15} | {'Avg Abs Error'}")
    print("-" * 50)

    for w in widths:
        # Create a new sketch with this width (reuse the 10% sample already collected)
        s = CountMinSketch(width=w, depth=5)
        for row in tail_rows:
            s.update(row.tail_number, 1)
        
        # Calculate MAE for the top 100
        errors = []
        for code, exact in all_tails_exact.items():
            est = s.estimate(code)
            errors.append(abs(est - exact))
        
        avg_err = sum(errors) / len(errors)
        mem_approx = w * 5 * 4 # 5 rows * w cols * 4 bytes/int
        print(f"{w:<15} | {mem_approx:<15} | {avg_err:.2f}")
        cms_sweep_results.append((w, avg_err))

    print("\nProbabilistic experiments finished running!")
    spark.stop()

if __name__ == "__main__":
    run_probabilistic_experiments()
