import pandas as pd
import os
import glob
import argparse
import subprocess
from datetime import datetime, timedelta
def merge_parquet(folder, output_file, exclude_filenames=None):
    files = glob.glob(os.path.join(folder, "*.parquet"))
    
    # Exclude specific filenames if provided
    if exclude_filenames:
        files = [f for f in files if os.path.basename(f) not in exclude_filenames]

    if not files:
        print(f'{folder} No Parquet files found to merge (excluded files ignored).')
        return

    # Special check: If the only file remaining is the output file itself, 
    # and we are intending to merge, we might want to skip if there's nothing NEW to add.
    # The original code had: if len(files) == 1 and files[0] == 'merged.parquet': return
    # Let's preserve similar logic.
    if len(files) == 1 and os.path.abspath(files[0]) == os.path.abspath(output_file):
        print(f'{folder} Only one merged (output) file exists, no new files to merge.')
        return

    dfs = []
    files = sorted(files)
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"Skipping corrupted file: {f}, Error: {e}")
    
    if not dfs:
        print("No valid data read.")
        return

    # Merge data
    df_merged = pd.concat(dfs, axis=0, ignore_index=True)
    df_merged = df_merged.dropna(axis=1, how='all')

    # ---------------------------------------------------------
    # Critical change: Step 1 - Attempt to save file first
    # ---------------------------------------------------------
    try:
        df_merged.to_parquet(output_file)
        print(f"Merge successful, file saved to: {output_file}")
        
        # -----------------------------------------------------
        # Critical change: Step 2 - Delete source files after successful save
        # -----------------------------------------------------
        if output_file in files:
            files.remove(output_file)
        for f in files:
            try:
                os.remove(f)
                print(f"Deleted source file: {f}")
            except OSError as e:
                print(f"Could not delete file {f}: {e}")
                
        print("All done.")
        
    except Exception as e:
        # If save fails, print error and DO NOT delete source files
        print(f"!!! Save failed !!! Source files were not deleted. Error: {e}")