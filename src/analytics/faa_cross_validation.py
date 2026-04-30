import os
import pandas as pd
from typing import List

# Official FAA Top 20 US Airports by Passenger Boarding Volume (CY2022)
FAA_TOP_20 = [
    "ATL", "DFW", "DEN", "ORD", "LAX", "JFK", "LAS", "MCO", "MIA", "CLT",
    "SEA", "PHX", "EWR", "SFO", "IAH", "BOS", "SLC", "FLL", "BWI", "DCA"
]

def main() -> None:
    print("==========================================================")
    print("  GRAPHX vs FAA CROSS-VALIDATION")
    print("==========================================================")
    
    pr_path = "hdfs_like/graphx_results/pagerank_top.parquet"
    if not os.path.exists(pr_path):
        print(f"Error: {pr_path} not found. Run GraphX pipeline first.")
        return

    # Load GraphX Results
    try:
        df_pr = pd.read_parquet(pr_path)
    except Exception as e:
        print(f"Failed to load parquet: {e}")
        return

    # Extract our GraphX Top 20
    graphx_top = df_pr.head(20)['airport'].tolist()

    print("\n--- Top 20 Rankings ---")
    print(f"{'Rank':<5} | {'GraphX (Delay Propagation)':<30} | {'FAA (Passenger Volume)':<30}")
    print("-" * 75)
    
    for i in range(20):
        # Handle cases where graphx output has fewer than 20 items
        gx_airport = graphx_top[i] if i < len(graphx_top) else "N/A"
        faa_airport = FAA_TOP_20[i]
        print(f"{i+1:<5} | {gx_airport:<30} | {faa_airport:<30}")

    # Calculate Overlap Metric
    intersection = set(graphx_top).intersection(set(FAA_TOP_20))
    overlap_pct = (len(intersection) / 20) * 100

    print("\n--- Validation Statistics ---")
    print(f"GraphX Top 20 Airports generated: {len(graphx_top)}")
    print(f"Overlapping Airports in Top 20    : {len(intersection)}")
    print(f"Overlap Percentage                : {overlap_pct:.2f}%")
    
    if overlap_pct >= 70:
        print("\nVALIDATED: The delay propagation hubs highly correlate with actual FAA physical volume hubs.")
    else:
        print("\nNote: The delay network shows different hub patterns than pure passenger volume.")

    print("==========================================================\n")

if __name__ == "__main__":
    main()
