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
    parser.add_argument("--groups", type=int, default=11, help="Number of row groups to process (for single file mode)")
    parser.add_argument("--hive", action="store_true", default=True, help="Use Hive partitioned dataset (default: True)")
    parser.add_argument("--start_date", type=str, help="Start date for filtering (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date for filtering (YYYY-MM-DD)")
    parser.add_argument("--chunk_size", type=int, default=1_000_000, help="Number of rows to buffer per batch (for Hive mode)")
    
    args = parser.parse_args()
    
    # Determine data source and filters
    if args.hive:
        raw_path = config.RAW_DATA_HIVE
        # Start with base filters from config
        filters = list(config.DATA_FILTERS)
        
        # Add CLI overrides for date range
        if args.start_date:
            filters.append(('date', '>=', args.start_date))
        if args.end_date:
            filters.append(('date', '<=', args.end_date))
            
        row_groups = None
        print(f"Using HIVE data source: {raw_path}")
    else:
        raw_path = config.RAW_DATA_PATH
        filters = None
        print(f"Using SINGLE FILE data source: {raw_path}")

    if args.test:
        print("Running in TEST mode...")
        # Override config for small data test
        config.WINDOW_ROLLING_STATS = 50
        config.WINDOW_CVD_FAST = 10
        if args.hive:
            # Example: restrict to a single date for test in hive
            filters = filters + [('date', '==', '2025-09-01')]
        else:
            row_groups = range(2)
    else:
        row_groups = range(args.groups) if not args.hive else None
        
    try:
        # Run Pipeline
        print("Starting Pipeline...")
        df_processed = processing.process_pipeline(
            raw_path, 
            row_groups=row_groups, 
            filters=filters,
            chunk_size=args.chunk_size
        )
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
