import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.lsh.minhash_lsh_spark import run_minhash_lsh

def run_lsh_experiments():
    print("="*60)
    print("🔍 EXPERIMENT 3: SIMILARITY SEARCH (MinHash LSH)")
    print("="*60)
    
    # 1. Initialize Spark
    spark = SparkSession.builder \
        .appName("LSHExperiments") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
        
    parquet_path = "./hdfs_like/curated_parquet/"
    if not os.path.exists(parquet_path):
        print(f"Error: Cannot find curated data at {parquet_path}.")
        spark.stop()
        return

    df = spark.read.parquet(parquet_path)
    
    # Make sure we don't have nulls in critical columns
    clean_df = df.na.drop(subset=["origin", "destination", "delay_minutes"])
    
    # Fill in a dummy "UNKNOWN" if delay_cause is empty
    clean_df = clean_df.withColumn("delay_cause", F.coalesce(F.col("delay_cause"), F.lit("UNKNOWN")))

    print("\n[A] Grouping flights by Route (Origin, Destination) and finding matching delay characteristics.")
    print("Creating tokens based on delay minutes + delay source using LSH Hash Tables...")

    # We want to find routes (origin -> destination) that suffer from the SAME patterns of delays.
    # We use a Jaccard distance threshold of 0.8 to find "somewhat similar" routes (lower means MORE similar)
    lsh_results_df = run_minhash_lsh(
        spark_df=clean_df,
        group_key_cols=["origin", "destination"],
        delay_minutes_col="delay_minutes",
        delay_cause_col="delay_cause",
        delay_bucket_size=15,    # Group delays into 15 minute buckets
        num_hash_tables=3,
        approx_similarity_threshold=0.85 
    )

    print("\n[B] Computing Top 10 Most Similar Flight Routes (by Delay Patterns):")
    # Clean up output presentation for the terminal
    presentation_df = lsh_results_df.select(
        F.col("group_a").alias("Route_A"),
        F.col("group_b").alias("Route_B"),
        F.round(F.col("jaccard_distance_est"), 3).alias("Jaccard_Distance")
    ).filter(F.col("group_a") < F.col("group_b")) # Deduplicate A-B / B-A
    
    presentation_df.orderBy("Jaccard_Distance").show(10, truncate=False)

    print("LSH experiments finished running!")
    spark.stop()

if __name__ == "__main__":
    run_lsh_experiments()
