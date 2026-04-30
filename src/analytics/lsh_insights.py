
from pyspark.sql import SparkSession
from src.lsh.minhash_lsh_spark import run_minhash_lsh
import os

def generate_lsh_insights():
    print("Generating LSH Similarity Insights (Delay Siblings)...")
    
    spark = SparkSession.builder.appName("lsh-insights").getOrCreate()
    curated_path = "./hdfs_like/curated_parquet"
    
    if os.path.exists(curated_path):
        df = spark.read.parquet(curated_path)
        
        # Run LSH on Origin-Destination pairs
        # We want to find routes with similar delay profiles
        lsh_results = run_minhash_lsh(
            df, 
            group_key_cols=["origin", "destination"],
            approx_similarity_threshold=0.3 # 70% Jaccard similarity
        )
        
        # Format the output into "Delay Siblings"
        # Filter out self-similarity if it exists
        siblings = lsh_results.filter("group_a != group_b").orderBy("jaccard_distance_est")
        
        print("LSH Similarity (Delay Siblings) Found:")
        siblings.show(10, truncate=False)
        
        os.makedirs("./results/tables", exist_ok=True)
        siblings_pd = siblings.limit(100).toPandas()
        siblings_pd.to_csv("./results/tables/lsh_similarity_report.csv", index=False)
        print("LSH Report saved to results/tables/lsh_similarity_report.csv")

if __name__ == "__main__":
    generate_lsh_insights()
