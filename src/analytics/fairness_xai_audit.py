
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, avg, count, desc, when
import matplotlib.pyplot as plt
import pandas as pd

def run_fairness_xai_audit():
    print("Starting Explainable AI (XAI) & Fairness Audit...")
    
    spark = SparkSession.builder.appName("bdgp-fairness-audit").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load curated data
    curated_path = "./hdfs_like/curated_parquet"
    if not os.path.exists(curated_path):
        print(f"Error: Curated data not found at {curated_path}")
        return

    df = spark.read.parquet(curated_path)

    # 1. Extract Carrier from flight_id (origin|dest|carrier|...)
    # index 2 in split
    df_with_carrier = df.withColumn("carrier", split(col("flight_id"), "\|").getItem(2))
    
    # 2. XAI: Why are certain hubs "important"? 
    # We attribute delay minutes to carriers per origin airport.
    print("Calculating XAI Attribution (Hub Importance by Carrier)...")
    attribution = (
        df_with_carrier.groupBy("origin", "carrier")
        .agg(
            count("*").alias("flight_count"),
            avg("delay_minutes").alias("avg_delay")
        )
        .orderBy(desc("flight_count"))
    )
    
    attribution_pd = attribution.limit(50).toPandas()
    os.makedirs("./results/tables", exist_ok=True)
    attribution_pd.to_csv("./results/tables/xai_hub_attribution.csv", index=False)

    # 3. Fairness Audit: "Is the model biased against Regional Carriers?"
    # Define Major vs Regional (simplified legacy mapping)
    majors = ["AA", "DL", "UA", "WN", "AS", "B6"]
    df_fairness = df_with_carrier.withColumn(
        "carrier_type", 
        when(col("carrier").isin(majors), "Major/Legacy").otherwise("Regional/Low-Cost")
    )

    print("Running Fairness Audit...")
    fairness_stats = (
        df_fairness.groupBy("carrier_type")
        .agg(
            avg("delay_minutes").alias("mean_delay_minutes"),
            count("*").alias("sample_size")
        )
    ).toPandas()
    
    fairness_stats.to_csv("./results/tables/fairness_audit.csv", index=False)

    # 4. Visualization
    os.makedirs("./results/charts", exist_ok=True)
    
    plt.figure(figsize=(10, 6))
    plt.bar(fairness_stats['carrier_type'], fairness_stats['mean_delay_minutes'], color=['#4285F4', '#EA4335'])
    plt.title("Fairness Audit: Mean Delay Minutes by Carrier Class")
    plt.ylabel("Avg Delay (Minutes)")
    plt.xlabel("Carrier Categorization")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig("./results/charts/fairness_audit.png")
    
    print(f"Audit Complete. Results saved to ./results/tables/ and ./results/charts/")

if __name__ == "__main__":
    run_fairness_xai_audit()
