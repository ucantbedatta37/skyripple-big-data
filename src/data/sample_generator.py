import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterator, List


AIRPORTS = [
    "ATL", "ORD", "DFW", "DEN", "JFK", "LAX", "SFO", "SEA", "MIA", "BOS",
    "PHX", "IAH", "CLT", "DTW", "LAS",
]

CAUSES = [
    "carrier", "weather", "nas", "security", "late_aircraft",
]


def _random_date(start: datetime, days: int) -> datetime:
    return start + timedelta(days=random.randint(0, days - 1), hours=random.randint(0, 23))


def make_record(idx: int, base: datetime) -> Dict:
    origin = random.choice(AIRPORTS)
    dest = random.choice([a for a in AIRPORTS if a != origin])
    delay_minutes = max(0, int(random.gauss(12, 18)))
    cause = random.choice(CAUSES)

    sched_dt = _random_date(base, days=30)
    flight_number = random.randint(100, 9000)
    tail_number = f"N{random.randint(1000000, 9999999)}"

    return {
        # Used to approximate a unique flight identifier for dedup experiments.
        "flight_id": f"{tail_number}|{flight_number}|{origin}|{dest}|{sched_dt.isoformat()}",
        "tail_number": tail_number,
        "flight_number": flight_number,
        "origin": origin,
        "destination": dest,
        "scheduled_departure": sched_dt.isoformat(),
        "delay_minutes": delay_minutes,
        "delay_cause": cause,
    }


def generate_records(num_records: int, duplicate_rate: float, seed: int) -> Iterator[Dict]:
    """
    duplicate_rate: fraction of emitted records that are duplicates of earlier ones.
    """
    random.seed(seed)
    base = datetime(2024, 1, 1)

    produced: List[Dict] = []
    for i in range(num_records):
        if produced and random.random() < duplicate_rate:
            # Re-emit an earlier record to simulate at-least-once delivery duplicates.
            record = random.choice(produced)
        else:
            record = make_record(i, base)
            produced.append(record)
        yield record


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--num-records", type=int, default=5000)
    ap.add_argument("--duplicate-rate", type=float, default=0.10)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    for rec in generate_records(args.num_records, args.duplicate_rate, args.seed):
        print(json.dumps(rec))


if __name__ == "__main__":
    main()

