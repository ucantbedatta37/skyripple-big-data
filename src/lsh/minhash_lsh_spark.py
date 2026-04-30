from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import MinHashLSH
from pyspark.sql import functions as F


def build_delay_tokens_udf(delay_minutes_col: str, delay_cause_col: str, delay_bucket_size: int = 5):
    """
    Helper to build tokens like "b{bucket}|{cause}".
    Implemented as Spark SQL expression rather than Python UDF when possible.
    """
    # Using concat_ws with array of size 1 to keep it token-like.
    return F.array(
        F.concat(
            F.lit("b"),
            (F.floor(F.col(delay_minutes_col) / F.lit(delay_bucket_size)) * F.lit(delay_bucket_size)).cast("int"),
            F.lit("|"),
            F.col(delay_cause_col),
        )
    )


def run_minhash_lsh(
    spark_df: DataFrame,
    group_key_cols: List[str],
    delay_minutes_col: str = "delay_minutes",
    delay_cause_col: str = "delay_cause",
    delay_bucket_size: int = 5,
    num_hash_tables: int = 3,
    bucket_length: int = 2,
    approx_similarity_threshold: Optional[float] = None,
) -> DataFrame:
    """
    Build a delay-feature vector per group and run Spark's MinHashLSH to find near-duplicates.

    For the 4-day MVP we keep it simple:
    - group rows by airport pair (or origin airport) and collect tokens
    - use CountVectorizer -> MinHashLSH
    """
    tokens = build_delay_tokens_udf(delay_minutes_col, delay_cause_col, delay_bucket_size=delay_bucket_size)
    df_tokens = spark_df.select(*group_key_cols, tokens.alias("tokens_row"))

    # Aggregate tokens into a single bag per group.
    df_grouped = (
        df_tokens.groupBy(*group_key_cols)
        .agg(F.flatten(F.collect_list("tokens_row")).alias("tokens"))
        .withColumn("group_id", F.concat_ws("|", *[F.col(c).cast("string") for c in group_key_cols]))
    )

    cv = CountVectorizer(inputCol="tokens", outputCol="features", vocabSize=5000, binary=True)
    model = cv.fit(df_grouped)
    features_df = model.transform(df_grouped).select("group_id", "features")

    lsh = MinHashLSH(
        inputCol="features",
        outputCol="hashes",
        numHashTables=num_hash_tables,
    )
    lsh_model = lsh.fit(features_df)

    if approx_similarity_threshold is not None:
        # Example similarity join; for reports you can limit pairs for readability.
        joined = lsh_model.approxSimilarityJoin(features_df, features_df, approx_similarity_threshold)
        out = joined.select(
            F.col("datasetA.group_id").alias("group_a"),
            F.col("datasetB.group_id").alias("group_b"),
            F.col("distCol").alias("jaccard_distance_est"),
        )
        return out

    # Otherwise return hashes so downstream can run nearest-neighbor queries.
    return lsh_model.transform(features_df).select("group_id", "hashes")

