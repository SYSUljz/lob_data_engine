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

class GapFiller:
    def __init__(self, data_path: str, remote_alias: str = None, remote_base_path: str = None):
        """
        data_path: Local path to the symbol folder, e.g., .../data/2026-01-15/BTCUSDT
        """
        # Load config if parameters are missing
        config = load_vps_config()
        self.remote_alias = remote_alias or config.get("remote_alias", "vps")
        self.remote_base_path = (remote_base_path or config.get("remote_base_path", "/root/lob_data_engine/data")).rstrip('/')

        self.data_path = Path(data_path)
        self.depth_path = self.data_path / "depthUpdate"
        
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
        if not self.depth_path.exists():
            print(f"❌ Path does not exist: {self.depth_path}")
            return

        # 1. Merge Local
        print(f"🔄 Merging local files in {self.depth_path}...")
        merged_file = self.depth_path / "merged.parquet"
        filled_file = self.depth_path / "filled.parquet"
        # Check if there are parquets to merge
        if list(self.depth_path.glob("*.parquet")):
            merge_parquet(str(self.depth_path), str(merged_file))
        else:
            print("⚠️ No parquet files found to merge.")
            return

        # 2. Analyze Gaps
        print(f"📖 Reading {merged_file}...")
        try:
            data = pd.read_parquet(merged_file)

            data = data.sort_values('E').reset_index(drop=True)
        except Exception as e:
            print(f"❌ Error reading merged file: {e}")
            return

        # Check continuity
        # Assuming binance format with 'u' (update ID)
        if 'u' not in data.columns or 'U' not in data.columns:
            print("⚠️ 'u' or 'U' columns not found. Skipping gap check (not depthUpdate data?).")
            return

        data['prev_u'] = data['u'].shift(1)
        data['is_continuous'] = (data['U'] == data['prev_u'] + 1)
        data.loc[0, 'is_continuous'] = True

        gaps = data[~data['is_continuous']].copy()

        if gaps.empty:
            print("✨ Data is continuous. No gaps found.")
            return

        gap_intervals = []
        for idx in gaps.index:
            gap_intervals.append({
                'start_e': data.iloc[idx-1]['E'],
                'end_e': data.iloc[idx]['E'],
                'prev_u': data.iloc[idx-1]['u'],
                'curr_u': data.iloc[idx]['U']
            })

        print(f"🔍 Found {len(gap_intervals)} gaps.")

        # 3. List Remote Files
        remote_dir = f"{self.remote_base_path}/{self.rel_path}/depthUpdate/"
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
                # Try simple integer stem first (old script logic)
                stem = Path(f).stem
                if '_' in stem:
                    ts_part = stem.split('_')[-1]
                    start_e = int(ts_part)
                else:
                    start_e = int(stem)
                file_index.append({'start_e': start_e, 'filename': f})
            except ValueError:
                continue
        
        if not file_index:
            print("⚠️ No valid timestamped files found on remote.")
            return

        file_index = sorted(file_index, key=lambda x: x['start_e'])
        remote_df = pd.DataFrame(file_index)

        # 4. Match Files to Gaps
        files_to_download = set()
        for gap in gap_intervals:
            # Find files that might cover the gap
            # Range: files starting before gap ends, and next file starts after gap starts
            
            # Find start index (last file starting before gap start)
            candidates = remote_df[remote_df['start_e'] <= gap['start_e']]
            start_idx = candidates.index.max() if not candidates.empty else 0
            
            # Find end index (first file starting after gap end)
            candidates_end = remote_df[remote_df['start_e'] > gap['end_e']]
            end_idx = candidates_end.index.min() if not candidates_end.empty else len(remote_df) - 1
            
            # Ensure valid range
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
        temp_dir = self.depth_path / "temp_fill_gaps"
        temp_dir.mkdir(exist_ok=True)
        
        new_data_chunks = []
        
        try:
            for fname in files_to_download:
                remote_file_path = f"{self.remote_alias}:{remote_dir}{fname}"
                local_temp_path = temp_dir / fname
                
                # Check if already exists (optimization)
                if not local_temp_path.exists():
                     # rclone copyto
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
                # Filter useful data (optional, but good for cleanliness)
                # data = data.append(new_data) -> deprecated, use concat
                final_data = pd.concat([data, new_data], ignore_index=True)
                
                # Sort and Deduplicate
                # Sort by 'U' (Start Update ID) to ensure strict sequence, as 'E' (timestamp) might have jitter
                final_data = final_data.sort_values('E').reset_index(drop=True)
                final_data = final_data.drop_duplicates(subset=['U', 'u']).reset_index(drop=True)
                
                print(f"✨ Added {len(final_data) - len(data)} new records.")
                
                # 1. 获取上一行的 E 值
                final_data['prev_u'] = final_data['u'].shift(1)
                final_data['prev_E'] = final_data['E'].shift(1)

                final_data['is_cont_check'] = (final_data['U'] == final_data['prev_u'] + 1)

                # 3. 第一行强制设为连续，避免 shift 产生的 NaN 触发误报
                final_data.loc[final_data.index[0], 'is_cont_check'] = True

                # 4. 筛选出不连续的行并统计缺失的“E长度”
                remaining = final_data[~final_data['is_cont_check']]

                total_missing = 0
                if not remaining.empty:
                    # missing length = (当前 E) - (前一个 E)
                    # 例如：前一个 E=10，当前 E=15，中间缺失了 11,12,13,14 共 4 个 ID
                    missing_counts = remaining['E'] - remaining['prev_E'] 
                    total_missing = int(missing_counts.sum())

                print(f"Total missing E-sequence length: {total_missing}")

                print(f"📉 Remaining gaps: {len(remaining)}")
                print(f"📉 Total missing updates: {int(total_missing)}")
                
                # Save
                # Cleanup aux columns
                cols_to_drop = ['prev_u', 'is_continuous', 'prev_u_check', 'is_cont_check']
                final_data.drop(columns=[c for c in cols_to_drop if c in final_data.columns], inplace=True)
                
                final_data.to_parquet(filled_file)
                print(f"✅ Saved updated merged file to {filled_file}")
                
                # 6. Save Report
                os.mkdir(self.depth_path / "filled/")
                report_file = self.depth_path / "filled/gap_report.txt"
                with open(report_file, "w") as f:
                    f.write("📊 Gap Fill Report\n")
                    f.write("="*30 + "\n")
                    f.write(f"Timestamp: {pd.Timestamp.now()}\n")
                    f.write(f"Initial records: {len(data)}\n")
                    f.write(f"Added records: {len(final_data) - len(data)}\n")
                    f.write(f"Final records: {len(final_data)}\n")
                    f.write(f"Remaining gaps: {len(remaining)}\n")
                    f.write(f"Total missing updates: {int(total_missing)}\n")
                    if not remaining.empty:
                        f.write("\nRemaining Gap Samples (U sequence):\n")
                        f.write(remaining[['E', 'U', 'prev_u_check']].head().to_string())
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

    parser = argparse.ArgumentParser(description="Merge local parquet files and fill gaps from remote VPS.")
    parser.add_argument("data_path", help="Path to the data folder (e.g., .../data/2026-01-15/BTCUSDT)")
    parser.add_argument("--remote_alias", default=None, help=f"Rclone remote alias (default: {default_config.get('remote_alias')})")
    parser.add_argument("--remote_base", default=None, help=f"Remote base path (default: {default_config.get('remote_base_path')})")
    
    args = parser.parse_args()
    
    filler = GapFiller(args.data_path, args.remote_alias, args.remote_base)
    filler.run()
