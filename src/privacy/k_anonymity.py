
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, floor, concat, lit

def apply_k_anonymity_generalization(df: DataFrame) -> DataFrame:
    """
    Applies K-Anonymity principles via Generalization and Binning:
    1. Time Binning: Converts exact HH:MM to 4-hour blocks (e.g., Morning, Mid-day).
    2. Value Generalization: Buckets delay minutes.
    """
    
    # 1. Generalize Time into 4-hour blocks
    # Scheduled Departure is ISO string, we extract HH
    df_p = df.withColumn("hour", floor(col("scheduled_departure").cast("string").substr(12, 2).cast("int")))
    
    df_k = df_p.withColumn(
        "time_block",
        when((col("hour") >= 0) & (col("hour") < 4), "Late Night (00-04)")
        .when((col("hour") >= 4) & (col("hour") < 8), "Early Morning (04-08)")
        .when((col("hour") >= 8) & (col("hour") < 12), "Morning (08-12)")
        .when((col("hour") >= 12) & (col("hour") < 16), "Afternoon (12-16)")
        .when((col("hour") >= 16) & (col("hour") < 20), "Evening (16-20)")
        .otherwise("Night (20-24)")
    )
    
    # 2. Generalize Delay Minutes into bins of 15 mins
    df_k = df_k.withColumn(
        "delay_bin",
        concat(
            (floor(col("delay_minutes") / 15) * 15).cast("string"),
            lit("-"),
            (floor(col("delay_minutes") / 15) * 15 + 15).cast("string"),
            lit(" min")
        )
    )
    
    return df_k.select("flight_id", "origin", "destination", "time_block", "delay_bin", "delay_cause")

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import os
    
    spark = SparkSession.builder.appName("k-anonymity-audit").getOrCreate()
    curated_path = "./hdfs_like/curated_parquet"
    
    if os.path.exists(curated_path):
        raw_df = spark.read.parquet(curated_path)
        anon_df = apply_k_anonymity_generalization(raw_df)
        
        print("K-Anonymity Generalization Sample:")
        anon_df.show(10, truncate=False)
        
        os.makedirs("./results/tables", exist_ok=True)
        anon_df.limit(100).toPandas().to_csv("./results/tables/k_anonymity_sample.csv", index=False)
        print("Sample saved to results/tables/k_anonymity_sample.csv")
