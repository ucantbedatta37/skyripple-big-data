# SkyRipple — Large-Scale Flight Delay Propagation Analysis

**Course:** DATA 228 — Big Data Technologies | San José State University
**Team:** Mamta Jha, Akanksha Shukla, Sujith Dugyala, Praveen Seera, Sree Datta Bachampally

## Project Overview
SkyRipple is an end-to-end distributed Big Data pipeline that analyzes how flight delays propagate through the U.S. airport network using the BTS On-Time Performance dataset (70M+ records).

## Tech Stack
- **Stream Ingestion:** Apache Kafka
- **Stream Processing:** Spark Structured Streaming
- **Storage:** HDFS + Parquet
- **Algorithms:** Bloom Filter, HyperLogLog, Count-Min Sketch, MinHash LSH
- **Graph Analytics:** Spark GraphX (PageRank + Connected Components)
- **Privacy:** Differential Privacy (Laplace Mechanism)

## Repo Structure
## Team Roles
| Member | Role |
|--------|------|
| Mamta Jha | Infrastructure & Ingestion |
| Sujith Dugyala | Spark Streaming ETL |
| Akanksha Shukla | Approximate Algorithms & LSH |
| Praveen Seera | Graph Analytics |
| Sree Datta Bachampally | Differential Privacy & Experiments |

## Dataset
BTS On-Time Performance Data — https://www.transtats.bts.gov/
- Tier 1: 1 month (~500K records)
- Tier 2: 6 months (~3M records)
- Tier 3: 1 year (~6M records)
