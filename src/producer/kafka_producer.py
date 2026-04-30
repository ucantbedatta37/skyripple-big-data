import argparse
import json
import time
from typing import Iterator

from kafka import KafkaProducer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_FLIGHT_RAW
from src.data.sample_generator import generate_records


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        acks="all",
        retries=3,
        linger_ms=5,
    )


def run(
    topic: str,
    num_records: int,
    duplicate_rate: float,
    seed: int,
    target_records_per_sec: float,
) -> None:
    producer = build_producer()
    key_prefix = "flight"

    interval = 1.0 / max(target_records_per_sec, 1e-9)
    sent = 0
    start = time.time()

    for rec in generate_records(num_records=num_records, duplicate_rate=duplicate_rate, seed=seed):
        # Use a stable key so Spark can partition consistently if we later add dedup state per key.
        key = f"{key_prefix}-{rec['flight_id']}"
        producer.send(topic, key=key, value=rec)
        sent += 1
        if sent % 1000 == 0:
            elapsed = time.time() - start
            print(f"[producer] sent={sent} elapsed_s={elapsed:.2f} rate={sent/max(elapsed,1e-9):.1f}/s")
        time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start
    print(f"[producer] done sent={sent} elapsed_s={elapsed:.2f} avg_rate={sent/max(elapsed,1e-9):.1f}/s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=KAFKA_TOPIC_FLIGHT_RAW)
    ap.add_argument("--num-records", type=int, default=5000)
    ap.add_argument("--duplicate-rate", type=float, default=0.10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--rate", type=float, default=200.0, help="target records/sec")
    args = ap.parse_args()

    run(
        topic=args.topic,
        num_records=args.num_records,
        duplicate_rate=args.duplicate_rate,
        seed=args.seed,
        target_records_per_sec=args.rate,
    )


if __name__ == "__main__":
    main()

