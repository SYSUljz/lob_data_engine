import pandas as pd
import os
import subprocess
import shutil
import sys
from pathlib import Path
import argparse

# Allow importing from sibling modules
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) # src/
root_dir = os.path.dirname(parent_dir) # root
sys.path.append(root_dir)

try:
    from src.jobs.merge_parquet import merge_parquet
except ImportError:
    # Fallback if running from a different context
    sys.path.append(os.path.join(root_dir, 'src', 'jobs'))
    from merge_parquet import merge_parquet

try:
    from src.jobs.config import load_vps_config
except ImportError:
    sys.path.append(os.path.join(root_dir, 'src', 'jobs'))
    from config import load_vps_config

class TradeGapFiller:
    def __init__(self, data_path: str, remote_alias: str = None, remote_base_path: str = None):
        """
        data_path: Local path to the symbol folder, e.g., .../data/2026-01-15/BTCUSDT
        """
        # Load config if parameters are missing
        config = load_vps_config()
        self.remote_alias = remote_alias or config.get("remote_alias", "vps")
        self.remote_base_path = (remote_base_path or config.get("remote_base_path", "/root/lob_data_engine/data")).rstrip('/')

        self.data_path = Path(data_path)
        self.trade_path = self.data_path / "trade"
        
        # Infer relative path for remote construction
        # Expecting path structure like .../data/<date>/<symbol>
        parts = self.data_path.parts
        try:
            # Find the 'data' folder and take everything after it
            if 'data' in parts:
                idx = len(parts) - 1 - parts[::-1].index('data')
                self.rel_path = "/".join(parts[idx+1:])
            else:
                 # Fallback: just use the last two folders (Date/Symbol)
                 self.rel_path = f"{parts[-2]}/{parts[-1]}"
        except ValueError:
            self.rel_path = f"{parts[-2]}/{parts[-1]}"

    def run(self):
        if not self.trade_path.exists():
            print(f"❌ Path does not exist: {self.trade_path}")
            return

        # 1. Merge Local
        print(f"🔄 Merging local files in {self.trade_path}...")
        merged_file = self.trade_path / "merged.parquet"
        filled_file = self.trade_path / "filled.parquet"
        
        if list(self.trade_path.glob("*.parquet")):
            merge_parquet(str(self.trade_path), str(merged_file))
        else:
            print("⚠️ No parquet files found to merge.")
            return

        # 2. Analyze Gaps
        print(f"📖 Reading {merged_file}...")
        try:
            data = pd.read_parquet(merged_file)
            # Sort by trade ID 't'
            data = data.sort_values('t').reset_index(drop=True)
        except Exception as e:
            print(f"❌ Error reading merged file: {e}")
            return

        if 't' not in data.columns:
            print("⚠️ 't' column not found. Skipping gap check.")
            return

        # Continuity Logic (User provided)
        data['seq_diff'] = data['t'].diff()
        data['is_continuous'] = (data['seq_diff'] == 1.0)
        data.loc[0, 'is_continuous'] = True
        # data['segment_id'] = (~data['is_continuous']).cumsum() # Optional

        gaps = data[~data['is_continuous']].copy()

        if gaps.empty:
            print("✨ Data is continuous. No gaps found.")
            return

        gap_intervals = []
        for idx in gaps.index:
            # Gap is between idx-1 and idx
            # Use 'T' (timestamp) for file matching
            gap_intervals.append({
                'start_T': data.iloc[idx-1]['T'],
                'end_T': data.iloc[idx]['T'],
                'prev_t': data.iloc[idx-1]['t'],
                'curr_t': data.iloc[idx]['t']
            })

        print(f"🔍 Found {len(gap_intervals)} gaps.")

        # 3. List Remote Files
        remote_dir = f"{self.remote_base_path}/{self.rel_path}/trade/"
        print(f"📡 Listing remote files from {self.remote_alias}:{remote_dir}...")
        
        try:
            result = subprocess.run(
                ['rclone', 'lsf', f"{self.remote_alias}:{remote_dir}"], 
                capture_output=True, text=True, check=True
            )
            remote_files = result.stdout.strip().split('\n')
        except subprocess.CalledProcessError as e:
            print(f"❌ Rclone error: {e}")
            return

        # Parse remote files
        file_index = []
        for f in remote_files:
            if not f.endswith('.parquet'): continue
            try:
                # Expecting trade_<timestamp>.parquet or similar
                stem = Path(f).stem
                if '_' in stem:
                    ts_part = stem.split('_')[-1]
                    start_ts = int(ts_part)
                else:
                    start_ts = int(stem)
                file_index.append({'start_ts': start_ts, 'filename': f})
            except ValueError:
                continue
        
        if not file_index:
            print("⚠️ No valid timestamped files found on remote.")
            return

        file_index = sorted(file_index, key=lambda x: x['start_ts'])
        remote_df = pd.DataFrame(file_index)

        # 4. Match Files to Gaps
        files_to_download = set()
        for gap in gap_intervals:
            # Look for files that overlap with the gap [start_T, end_T]
            # Since files store a batch starting at start_ts, we want files where:
            # file_start_ts <= gap_end_T AND next_file_start_ts > gap_start_T
            
            # Find last file that started before or at gap start (to catch the segment containing the gap)
            # Actually, we want any file that *might* contain data between start_T and end_T.
            # Start checking from the file that starts before gap starts.
            candidates = remote_df[remote_df['start_ts'] <= gap['start_T']]
            start_idx = candidates.index.max() if not candidates.empty else 0
            
            # End checking at the file that starts after gap ends
            candidates_end = remote_df[remote_df['start_ts'] > gap['end_T']]
            end_idx = candidates_end.index.min() if not candidates_end.empty else len(remote_df) - 1
            
            # Convert to int to be safe
            start_idx = int(start_idx) if not pd.isna(start_idx) else 0
            end_idx = int(end_idx) if not pd.isna(end_idx) else len(remote_df) - 1

            subset = remote_df.loc[start_idx : end_idx]
            for _, row in subset.iterrows():
                files_to_download.add(row['filename'])

        if not files_to_download:
            print("⚠️ No remote files matched the gaps.")
            return

        print(f"📦 Downloading {len(files_to_download)} files...")

        # 5. Download and Patch
        temp_dir = self.trade_path / "temp_fill_gaps"
        temp_dir.mkdir(exist_ok=True)
        
        new_data_chunks = []
        
        try:
            for fname in files_to_download:
                remote_file_path = f"{self.remote_alias}:{remote_dir}{fname}"
                local_temp_path = temp_dir / fname
                
                # Check if already exists (optimization)
                if not local_temp_path.exists():
                     print(f"Downloading {fname}...")
                     subprocess.run(
                         ['rclone', 'copyto', remote_file_path, str(local_temp_path)],
                         check=True
                     )
                
                # Read
                try:
                    chunk = pd.read_parquet(local_temp_path)
                    new_data_chunks.append(chunk)
                except Exception as e:
                    print(f"⚠️ Failed to read downloaded file {fname}: {e}")

            # Merge
            if new_data_chunks:
                print("🧩 Merging new data...")
                new_data = pd.concat(new_data_chunks)
                final_data = pd.concat([data, new_data], ignore_index=True)
                
                # Sort and Deduplicate
                final_data = final_data.sort_values('t').reset_index(drop=True)
                final_data = final_data.drop_duplicates(subset=['t']).reset_index(drop=True)
                
                print(f"✨ Added {len(final_data) - len(data)} new records.")
                
                # Re-Verify Gaps
                final_data['prev_t'] = final_data['t'].shift(1)
                final_data['seq_diff'] = final_data['t'] - final_data['prev_t']
                final_data['is_continuous'] = (final_data['seq_diff'] == 1.0)
                final_data.loc[0, 'is_continuous'] = True
                
                remaining = final_data[~final_data['is_continuous']]
                
                total_missing = 0
                if not remaining.empty:
                    # e.g. prev=10, curr=15. diff=5. missing=4 (11,12,13,14)
                    missing_counts = remaining['seq_diff'] - 1
                    total_missing = int(missing_counts.sum())

                print(f"📉 Remaining gaps: {len(remaining)}")
                print(f"📉 Total missing trades: {total_missing}")
                
                # Save
                # Cleanup aux columns
                cols_to_drop = ['prev_t', 'seq_diff', 'is_continuous', 'segment_id']
                final_data.drop(columns=[c for c in cols_to_drop if c in final_data.columns], inplace=True)
                
                final_data.to_parquet(filled_file)
                print(f"✅ Saved updated merged file to {filled_file}")
                
                # 6. Save Report
                os.makedirs(self.trade_path / "filled/", exist_ok=True)
                report_file = self.trade_path / "filled/gap_report.txt"
                with open(report_file, "w") as f:
                    f.write("📊 Trade Gap Fill Report\n")
                    f.write("="*30 + "\n")
                    f.write(f"Timestamp: {pd.Timestamp.now()}\n")
                    f.write(f"Initial records: {len(data)}\n")
                    f.write(f"Added records: {len(final_data) - len(data)}\n")
                    f.write(f"Final records: {len(final_data)}\n")
                    f.write(f"Remaining gaps: {len(remaining)}\n")
                    f.write(f"Total missing trades: {total_missing}\n")
                    if not remaining.empty:
                        f.write("\nRemaining Gap Samples (t sequence):\n")
                        f.write(remaining[['T', 't', 'prev_t']].head().to_string())
                print(f"📝 Report saved to {report_file}")
                
            else:
                print("⚠️ No valid data extracted from downloaded files.")

        except subprocess.CalledProcessError as e:
             print(f"❌ Error during download: {e}")
        finally:
            # Cleanup temp dir
            shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    # Load default config for help text
    default_config = load_vps_config()

    parser = argparse.ArgumentParser(description="Merge local trade parquet files and fill gaps from remote VPS.")
    parser.add_argument("data_path", help="Path to the data folder (e.g., .../data/2026-01-15/BTCUSDT)")
    parser.add_argument("--remote_alias", default=None, help=f"Rclone remote alias (default: {default_config.get('remote_alias')})")
    parser.add_argument("--remote_base", default=None, help=f"Remote base path (default: {default_config.get('remote_base_path')})")
    
    args = parser.parse_args()
    
    filler = TradeGapFiller(args.data_path, args.remote_alias, args.remote_base)
    filler.run()
