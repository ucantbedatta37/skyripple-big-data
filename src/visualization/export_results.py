import pandas as pd
import json
import os
import glob

def export_graph_results():
    results_dir = "./hdfs_like/graphx_results"
    pagerank_path = f"{results_dir}/pagerank_top.parquet"
    cc_path = f"{results_dir}/connected_components.parquet"
    
    # PageRank outputs a directory in Spark. Read the parquet files in it.
    if not os.path.exists(pagerank_path):
        print(f"Error: {pagerank_path} not found.")
        return
    
    try:
        pr_df = pd.read_parquet(pagerank_path)
        cc_df = pd.read_parquet(cc_path)
        
        # Merge on 'airport'
        # The Spark outputs columns are 'airport', 'pagerank' and 'airport', 'component_id'
        merged = pr_df.merge(cc_df, on="airport")
        
        # Add basic coordinate mapping for major US airports
        coordinates = {
            "ATL": [33.6407, -84.4277], "DFW": [32.8998, -97.0403], "DEN": [39.8561, -104.6737],
            "ORD": [41.9742, -87.9073], "LAX": [33.9416, -118.4085], "JFK": [40.6413, -73.7781],
            "LAS": [36.0840, -115.1537], "ORL": [28.4312, -81.3081], "MCO": [28.4312, -81.3081],
            "MIA": [25.7959, -80.2870], "CLT": [35.2140, -80.9431], "SEA": [47.4502, -122.3088],
            "PHX": [33.4352, -112.0101], "EWR": [40.6895, -74.1745], "SFO": [37.6213, -122.3790],
            "IAH": [29.9902, -95.3368], "BOS": [42.3656, -71.0096], "SLC": [40.7899, -111.9791],
            "MSP": [44.8848, -93.2223], "FLL": [26.0742, -80.1506], "DTW": [42.2124, -83.3533],
            "PHL": [39.8729, -75.2437], "LGA": [40.7769, -73.8740], "BWI": [39.1774, -76.6684],
            "TPA": [27.9772, -82.5328], "SAN": [32.7338, -117.1933], "MDW": [41.7868, -87.7522],
            "DCA": [38.8512, -77.0402], "IAD": [38.9531, -77.4565], "BNA": [36.1263, -86.6774],
            "AUS": [30.1975, -97.6664], "STL": [38.7472, -90.3595], "RSW": [26.5362, -81.7551],
            "PDX": [45.5898, -122.5951], "CLE": [41.4108, -81.8494], "IND": [39.7173, -86.2941],
            "PIT": [40.4914, -80.2329], "CVG": [39.0461, -84.6625], "MSY": [29.9911, -90.2592],
            "RDU": [35.8776, -78.7875], "SAT": [29.5312, -98.4683], "SMF": [38.6954, -121.5908],
            "SJC": [37.3639, -121.9289], "SNA": [33.6762, -117.8674], "MKE": [42.9472, -87.8967],
            "PBI": [26.6832, -80.0956], "JAX": [30.4941, -81.6879], "BDL": [41.9389, -72.6832],
            "BUF": [42.9405, -78.7322], "ABQ": [35.0402, -106.6092], "OMA": [41.3032, -95.8941]
        }
        
        # Attach coords if found, else default to 0,0
        def get_coords(row):
            return coordinates.get(row['airport'], [0, 0])
        
        merged['lat'] = merged['airport'].apply(lambda x: coordinates.get(x, [0,0])[0])
        merged['lng'] = merged['airport'].apply(lambda x: coordinates.get(x, [0,0])[1])
        
        results = merged.to_dict(orient="records")
        
        output_file = "src/visualization/results.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Exported {len(results)} airports to {output_file}")
        
    except Exception as e:
        print(f"Error during export: {e}")

if __name__ == "__main__":
    if not os.path.exists("src/visualization"):
        os.makedirs("src/visualization")
    export_graph_results()
