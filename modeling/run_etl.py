import argparse
import sys
import os

# Add local directory to path so we can import config/src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from src import processing

def main():
    parser = argparse.ArgumentParser(description="Run LOB Data ETL Pipeline")
    parser.add_argument("--test", action="store_true", help="Run in test mode (small subset of data)")
    parser.add_argument("--groups", type=int, default=11, help="Number of row groups to process (default 11, strictly following user request)")
    
    args = parser.parse_args()
    
    # Determine row groups to read
    # User originally requested reading 11 groups: `range(11)`
    if args.test:
        print("Running in TEST mode (2 row groups)...")
        # Override config for small data test
        config.WINDOW_ROLLING_STATS = 50
        config.WINDOW_CVD_FAST = 10
        row_groups = range(2)
    else:
        print(f"Running in FULL mode ({args.groups} row groups)...")
        row_groups = range(args.groups)
        
    try:
        # Run Pipeline
        print("Starting Pipeline...")
        df_processed = processing.process_pipeline(config.RAW_DATA_PATH, row_groups=row_groups)
        print(f"Pipeline complete. Generated {len(df_processed)} samples.")
        
        # Save Split Data
        print("Splitting and Saving...")
        processing.save_split_data(df_processed, config.PROCESSED_DATA_DIR)
        print("ETL Job Finished Successfully.")
        
    except Exception as e:
        print(f"ETL Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
