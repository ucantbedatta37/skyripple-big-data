import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.privacy.differential_privacy import DifferentialPrivacy, epsilon_sweep_mae

def run_privacy_experiments():
    print("="*60)
    print("EXPERIMENT 2: DIFFERENTIAL PRIVACY (Laplace Noise)")
    print("="*60)
    
    # 1. Initialize Spark
    spark = SparkSession.builder \
        .appName("PrivacyExperiments") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
        
    parquet_path = "./hdfs_like/curated_parquet/"
    if not os.path.exists(parquet_path):
        print(f"Error: Cannot find curated data at {parquet_path}.")
        spark.stop()
        return

    df = spark.read.parquet(parquet_path)

    # We want to publish a public statistical report regarding flight frequencies per ORIGIN AIRPORT.
    # But we want to preserve privacy (hypothetically masking the exact dataset size/identifiability).
    print("\n[A] Scenario: Releasing aggregate flight counts per Origin Airport")
    
    # Calculate exact (true) counts
    exact_df = df.groupBy("origin").count().orderBy(F.desc("count"))
    
    # Pull into driver to apply our custom MVP python Privacy code
    # Format: Dict[str, float]
    true_counts = {row["origin"]: float(row["count"]) for row in exact_df.collect() if row["origin"] is not None}
    
    # Adding Noise using Laplace Mechanism
    # Sensitivity of counting queries is 1 (adding or removing one flight changes count by exactly 1).
    dp = DifferentialPrivacy(sensitivity=1.0)
    
    # Let's test with a moderate epsilon (0.5).
    target_epsilon = 0.5
    noisy_counts = dp.dp_noisy_counts(true_counts, epsilon=target_epsilon, clamp_min=0.0)
    
    print(f"\nComparing Exact vs Noisy Counts (Epsilon = {target_epsilon}):")
    print(f"{'Origin':<10} | {'Exact Count':<15} | {'Noisy DP Estimate':<20} | {'Absolute Error':<15}")
    print("-" * 65)
    
    # Print top 10 for demonstration
    sorted_origins = sorted(true_counts.keys(), key=lambda k: true_counts[k], reverse=True)[:10]
    
    for origin in sorted_origins:
        exact = true_counts[origin]
        noisy = noisy_counts[origin]
        err = abs(exact - noisy)
        print(f"{origin:<10} | {exact:<15.0f} | {noisy:<20.2f} | {err:<15.2f}")

    print("\n[B] Sweeping Privacy Budgets (Epsilons) for Mean Absolute Error (MAE)")
    # The smaller the epsilon, the greater the privacy coverage, but the greater the error (noise added).
    # The larger the epsilon, the lesser the privacy coverage, but significantly less error.
    epsilon_values = [0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
    sweep_results = epsilon_sweep_mae(true_counts, dp, epsilons=epsilon_values, num_trials=50)
    
    print(f"{'Epsilon (Privacy Budget)':<25} | {'Mean Absolute Error (Avg deviation)'}")
    print("-" * 65)
    for eps, mae in sweep_results:
        # Lower epsilon -> More Privacy -> More Error
        print(f"{eps:<25.2f} | {mae:.2f}")

    print("\nPrivacy experiments finished running!")
    spark.stop()

if __name__ == "__main__":
    run_privacy_experiments()
