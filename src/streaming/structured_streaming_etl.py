import argparse
import os
from typing import Any, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.config import (
    BF_K_HASHES,
    BF_M_BITS,
    CHECKPOINT_DIR,
    CURATED_PARQUET_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_FLIGHT_RAW,
)
from src.common.bloom_filter import BloomFilter


def get_schema() -> StructType:
    return StructType(
        [
            StructField("flight_id", StringType(), True),
            StructField("tail_number", StringType(), True),
            StructField("flight_number", IntegerType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("scheduled_departure", StringType(), True),
            StructField("delay_minutes", IntegerType(), True),
            StructField("delay_cause", StringType(), True),
        ]
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=KAFKA_TOPIC_FLIGHT_RAW)
    ap.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    ap.add_argument("--checkpoint-dir", default=CHECKPOINT_DIR)
    ap.add_argument("--output-dir", default=CURATED_PARQUET_DIR)
    ap.add_argument("--run-mode", choices=["available_now", "continuous"], default="available_now")
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.checkpoint_dir, exist_ok=True)

    spark = (
        SparkSession.builder.appName("bdgp-flight-delay-streaming-etl")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = get_schema()

    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        # Prevent Kafka consumer timeout on long-running Tier-3 runs
        .option("kafka.request.timeout.ms", "300000")
        .option("kafka.session.timeout.ms", "300000")
        .option("kafka.fetch.max.wait.ms", "10000")
        .load()
    )

    df_parsed = (
        df_raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    df_with_id = df_parsed.withColumn("record_id", col("flight_id"))

    # Initialize Bloom Filter on the driver for deduplication
    bf = BloomFilter(m_bits=BF_M_BITS, k_hashes=BF_K_HASHES)

    def foreach_batch(batch_df, batch_id: int) -> None:
        print(f"[stream-etl] batch_id={batch_id} extracting ID block from Kafka...", flush=True)

        # Collect flight_ids to the driver to update/check Bloom Filter
        # Note: In a production scale system, we'd use a distributed state or 
        # a Redis-backed filter, but for the Data 228 project requirements, 
        # an in-memory driver-side filter during batch processing is sufficient.
        
        # We wrap in list(set(...)) to do the distinct instantly in Python rather than 
        # using Spark's .distinct() which triggers a massive, heavy network shuffle 
        # that freezes the JVM Garbage Collector.
        ids = list(set([row.record_id for row in batch_df.select("record_id").collect()]))
        
        new_ids = []
        for fid in ids:
            if not bf.might_contain(fid):
                bf.add(fid)
                new_ids.append(fid)
        
        accepted_count = len(new_ids)
        dupe_count = len(ids) - accepted_count
        
        print(f"[stream-etl] batch_id={batch_id} total={len(ids)} accepted={accepted_count} dupes={dupe_count}", flush=True)

        if accepted_count > 0:
            from pyspark.sql.functions import broadcast
            
            # Filter the batch for only new records using a DataFrame inner join instead of a massive .isin() list
            # We use parallelize() instead of createDataFrame() because parallelizing a pure array of strings 
            # bypasses pandas/schema inference bottlenecks completely, taking milliseconds instead of minutes.
            rdd = batch_df.sparkSession.sparkContext.parallelize(new_ids)
            new_ids_df = rdd.map(lambda x: (x,)).toDF(["record_id"])
            
            (
                batch_df.join(broadcast(new_ids_df), on="record_id", how="inner")
                .drop("record_id")
                .na.drop(subset=["origin", "destination"])
                # Removed coalesce(1) — it forced all writes through a single thread,
                # causing progressive slowdown as the output directory grew.
                # Spark will now write in parallel across multiple part files.
                .write.mode("append")
                .parquet(args.output_dir)
            )

    writer = (
        df_with_id.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", args.checkpoint_dir)
    )

    if args.run_mode == "continuous":
        query = writer.start()
    else:
        # Process existing Kafka backlog once, then stop (fast MVP).
        query = writer.trigger(availableNow=True).start()

    query.awaitTermination()


if __name__ == "__main__":
    main()

