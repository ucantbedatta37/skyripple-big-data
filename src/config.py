import os


def env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    return int(val)


# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_FLIGHT_RAW = os.environ.get("KAFKA_TOPIC_FLIGHT_RAW", "flight_raw")

# Streaming output
OUTPUT_BASE_DIR = os.environ.get("OUTPUT_BASE_DIR", "hdfs_like")
CURATED_PARQUET_DIR = os.path.join(OUTPUT_BASE_DIR, "curated_parquet")
CHECKPOINT_DIR = os.path.join(OUTPUT_BASE_DIR, "checkpoints", "streaming_etl")

# Bloom filter dedup (MVP params; tune later)
BF_M_BITS = env_int("BF_M_BITS", 1 << 20)  # size of bit array
BF_K_HASHES = env_int("BF_K_HASHES", 4)   # number of hash functions

