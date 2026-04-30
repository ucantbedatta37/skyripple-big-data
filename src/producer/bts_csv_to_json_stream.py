import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


CAUSE_COLS = [
    ("CARRIER_DELAY", "carrier"),
    ("WEATHER_DELAY", "weather"),
    ("NAS_DELAY", "nas"),
    ("SECURITY_DELAY", "security"),
    ("LATE_AIRCRAFT_DELAY", "late_aircraft"),
]


def _parse_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if x == "" or x.lower() == "nan":
        return None
    return float(x)


def _parse_int(x: str) -> Optional[int]:
    x = (x or "").strip()
    if x == "" or x.lower() == "nan":
        return None
    return int(float(x))


def _safe_date(year_s: str, month_s: str, day_s: str) -> datetime:
    y = int(float(year_s))
    m = int(float(month_s))
    d = int(float(day_s))
    return datetime(y, m, d)


def _parse_crs_dep_time_to_iso(year_s: str, month_s: str, day_s: str, crs_dep_time_s: str) -> str:
    """
    CRS_DEP_TIME in BTS is HHMM (sometimes like 5 for 0005, like 0659, like 2400).
    We convert into ISO string using YEAR/MONTH/DAY_OF_MONTH.
    """
    base = _safe_date(year_s, month_s, day_s)
    t = crs_dep_time_s.strip()
    if t == "":
        return base.isoformat()

    # Handle cases like "2400", "65", "659", "0659" by interpreting as HHMM after float->int.
    from datetime import timedelta

    hhmm = int(float(t))
    if hhmm == 2400:
        # 24:00 is treated as the next day's 00:00.
        dt = base + timedelta(days=1)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.isoformat()

    hh = hhmm // 100
    mm = hhmm % 100

    # If HH parses to 24 due to malformed data, treat as next day 00:mm.
    if hh >= 24:
        dt = base + timedelta(days=1)
        dt = dt.replace(hour=0, minute=mm if mm < 60 else 0, second=0, microsecond=0)
    else:
        dt = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    return dt.isoformat()


def _pick_delay_cause(row: Dict[str, str]) -> str:
    # Choose the cause with the maximum positive delay minutes among cause columns.
    best_label = "unknown"
    best_val = -1.0
    for col, label in CAUSE_COLS:
        v = _parse_float(row.get(col, ""))
        if v is None:
            continue
        if v > best_val:
            best_val = v
            best_label = label
    if best_val <= 0:
        return "on_time_or_no_cause"
    return best_label


def _make_flight_id(
    origin: str,
    dest: str,
    carrier: str,
    flight_num: int,
    year_s: str,
    month_s: str,
    day_s: str,
    crs_dep_time_s: str,
) -> str:
    # CRS_DEP_TIME can be 4-digit-like (e.g., 659 for 0659) so normalize it.
    t = crs_dep_time_s.strip()
    t_norm = t
    if t != "":
        # Keep as integer of HHMM, without leading zeros.
        t_norm = str(int(float(t)))
    return f"{origin}|{dest}|{carrier}|{flight_num}|{year_s}-{month_s}-{day_s}|{t_norm}"


def make_record(row: Dict[str, str]) -> Dict:
    origin = row.get("ORIGIN", "").strip()
    dest = row.get("DEST", "").strip()
    carrier = row.get("OP_UNIQUE_CARRIER", "").strip()
    flight_num = _parse_int(row.get("OP_CARRIER_FL_NUM", "")) or 0

    year_s = row.get("YEAR", "").strip()
    month_s = row.get("MONTH", "").strip()
    day_s = row.get("DAY_OF_MONTH", "").strip()
    crs_dep_time_s = row.get("CRS_DEP_TIME", "").strip()

    flight_id = _make_flight_id(
        origin=origin,
        dest=dest,
        carrier=carrier,
        flight_num=flight_num,
        year_s=year_s,
        month_s=month_s,
        day_s=day_s,
        crs_dep_time_s=crs_dep_time_s,
    )

    dep_delay = _parse_float(row.get("DEP_DELAY", ""))  # minutes (can be negative)
    dep_delay_minutes = int(round(dep_delay)) if dep_delay is not None else 0
    delay_minutes = max(0, dep_delay_minutes)

    delay_cause = _pick_delay_cause(row)
    scheduled_departure = _parse_crs_dep_time_to_iso(year_s, month_s, day_s, crs_dep_time_s)

    return {
        "flight_id": flight_id,
        "tail_number": "",
        "flight_number": int(flight_num),
        "origin": origin,
        "destination": dest,
        "scheduled_departure": scheduled_departure,
        "delay_minutes": int(delay_minutes),
        "delay_cause": delay_cause,
    }


def stream_json_records(
    input_csv: str,
    limit: int,
    duplicate_rate: float,
    seed: int,
    shuffle: bool,
    max_cache: int,
) -> None:
    random.seed(seed)
    produced_cache = []
    emitted = 0

    # For this MVP we do a streaming read. If shuffle=True, we reservoir sample
    # by caching rows up to a bounded buffer.
    if not shuffle:
        with open(input_csv, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if emitted >= limit:
                    break

                if produced_cache and random.random() < duplicate_rate:
                    rec = random.choice(produced_cache)
                    print(json.dumps(rec))
                    emitted += 1
                    continue

                rec = make_record(row)
                produced_cache.append(rec)
                if len(produced_cache) > max_cache:
                    # Keep cache bounded for memory.
                    produced_cache = produced_cache[-max_cache:]
                print(json.dumps(rec))
                emitted += 1
    else:
        # Reservoir sampling for bounded shuffle.
        reservoir = []
        with open(input_csv, "r", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                rec = make_record(row)
                if len(reservoir) < limit:
                    reservoir.append(rec)
                else:
                    j = random.randint(0, i)
                    if j < limit:
                        reservoir[j] = rec

        # Now emit with duplicates.
        for i in range(limit):
            if reservoir and random.random() < duplicate_rate:
                rec = random.choice(reservoir)
            else:
                rec = reservoir[i]
            produced_cache.append(rec)
            if len(produced_cache) > max_cache:
                produced_cache = produced_cache[-max_cache:]
            print(json.dumps(rec))
            emitted += 1

    # No explicit return.


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--duplicate-rate", type=float, default=0.10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--shuffle", action="store_true", help="Reservoir-sample rows before emitting")
    ap.add_argument("--max-cache", type=int, default=10000, help="Cache size for duplicate emission")
    args = ap.parse_args()

    stream_json_records(
        input_csv=args.input_csv,
        limit=args.limit,
        duplicate_rate=args.duplicate_rate,
        seed=args.seed,
        shuffle=args.shuffle,
        max_cache=args.max_cache,
    )


if __name__ == "__main__":
    main()

