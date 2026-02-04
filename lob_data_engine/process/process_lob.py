#合并下载数据
import pandas as pd
import os
import glob
import argparse
import subprocess
from datetime import datetime, timedelta

def merge_parquet(folder, output_file):
    files = glob.glob(os.path.join(folder, "*.parquet"))
    
    if len(files) == 1 and os.path.basename(files[0])=='merged':
        print(f'{folder} 只有一个 merged。')

        
        return
    if not files:
        print(f'{folder}未找到Parquet文件。')
        return

    dfs = []
    # 如果输出文件也在这个文件夹里，需要避免把它自己也读进去（取决于你的文件名规则）
    # 建议先过滤掉 output_file
    files = sorted(files)
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"跳过损坏文件: {f}, 错误: {e}")
    
    if not dfs:
        print("没有有效的数据被读取。")
        return

    # 合并数据
    df_merged = pd.concat(dfs, axis=0, ignore_index=True)
    df_merged = df_merged.dropna(axis=1, how='all')

    # ---------------------------------------------------------
    # 关键修改：步骤 1 - 先尝试保存文件
    # ---------------------------------------------------------
    try:
        df_merged.to_parquet(output_file)
        print(f"合并成功，文件已保存至: {output_file}")
        
        # -----------------------------------------------------
        # 关键修改：步骤 2 - 保存成功后，不删除源文件
        # -----------------------------------------------------
        if output_file in files:
            files.remove(output_file)
        for f in files:
            try:
                os.remove(f)
                print(f"已删除源文件: {f}")
            except OSError as e:
                print(f"无法删除文件 {f}: {e}")
                
        print("全部完成。")
        
    except Exception as e:
        # 如果保存失败，打印错误，并且绝对不要删除源文件
        print(f"!!! 保存失败 !!! 源文件未被删除。错误信息: {e}")




#dataset preparing 
import pandas as pd
import numpy as np
import re
import os

def parse_first_price_vectorized(series):
    """
    向量化解析价格字符串提取 Level 1 价格。
    如果数据本身已经是列表/数组格式，直接取第一个元素；如果是字符串则进行解析。
    """
    # 尝试直接获取第一个元素（假设是列表/数组）
    try:
        # 如果是 list/array 列，直接提取
        return series.str[0]
    except:
        pass

    # 如果是字符串格式 "['100', '200']"，使用正则解析
    def _parse(x):
        try:
            clean_str = re.sub(r"[\[\]']", "", str(x))
            parts = clean_str.split()
            if parts:
                return float(parts[0])
            return np.nan
        except:
            return np.nan
    
    return series.apply(_parse)

def process_lob_parquet(input_path, output_path, start_session_id=0):
    """
    处理 LOB 数据，计算 Label，展开 Feature，并输出到 Parquet。
    
    参数:
        input_path: 输入 parquet 路径
        output_path: 输出 parquet 路径
        start_session_id: 当前文件 session_id 的起始值 (用于跨文件递增)
        
    返回:
        next_session_id: 下一个文件应该使用的起始 session_id
    """
    
    print(f"--- 处理文件: {input_path} ---")
    
    # 1. 读取数据
    if not os.path.exists(input_path):
        print(f"错误: 文件不存在 {input_path}")
        return start_session_id

    df = pd.read_parquet(input_path)
    if df.empty:
        print("警告: 数据为空，跳过")
        return start_session_id

    # 按时间排序
    df = df.sort_values(by="exchange_time").reset_index(drop=True)

    # ==========================================
    # 2. 数据预处理：计算 Mid-Price
    # ==========================================
    print("计算 Mid-Price...")
    # 提取最优买卖价 (兼容字符串或列表格式)
    df['bid_px_1'] = parse_first_price_vectorized(df['bids_px']).astype(float)
    df['asks_px_1'] = parse_first_price_vectorized(df['asks_px']).astype(float)
    df['mid_price'] = (df['bid_px_1'] + df['asks_px_1']) / 2

    # ==========================================
    # 3. Session ID 切分 (递增处理)
    # ==========================================
    GAP_THRESHOLD = 1000  # 毫秒
    
    # 计算时间差
    df['time_diff'] = df['exchange_time'].diff()
    
    # 标记断点 (大于阈值或第一行)
    is_gap = (df['time_diff'] > GAP_THRESHOLD).fillna(False)
    
    # 生成 session_id，基础值加上 start_session_id
    # cumsum 从 0 或 1 开始，加上外部传入的 start_session_id
    df['session_id'] = is_gap.cumsum() + start_session_id
    
    current_max_session_id = df['session_id'].max()
    print(f"Session ID 范围: {df['session_id'].min()} -> {current_max_session_id}")
    
    if df.empty:
        print("警告: 清洗后数据为空")
        return current_max_session_id + 1

    # ==========================================
    # 5. 特征展开 (Flattening) - 性能优化版
    # ==========================================
    print("展开 Feature 列 (Top 10)...")
    
    def expand_column(col_name, prefix, dtype=float):
        """将包含列表的列快速展开为多列"""
        # 假设列中已经是 list/array。如果是 string 格式的 list，需要先 eval (会变慢)
        # 这里假设 input parquet 读取出来已经是 array/list 结构
        # 取前 10 个元素
        expanded = pd.DataFrame(df[col_name].tolist()).iloc[:, :10]
        expanded.columns = [f"{prefix}{i}" for i in range(expanded.shape[1])]
        return expanded.astype(dtype)

    # 注意：如果 parquet 读入的是 string 形式的 "[1,2]"，这里会报错。
    # 鉴于你之前代码用了 .astype(float)，假设这里已经是数值型的 list
    try:
        # 尝试快速展开
        df_ask_px = expand_column('asks_px', 'ask_px_', float).astype(int) # 保持 int 
        df_bid_px = expand_column('bids_px', 'bid_px_', float).astype(int)
        df_ask_sz = expand_column('asks_sz', 'ask_sz_', float)
        df_bid_sz = expand_column('bids_sz', 'bid_sz_', float)
    except Exception as e:
        print(f"快速展开失败，尝试兼容模式 (可能数据是String): {e}")
        # 慢速兼容模式：如果数据是字符串
        import ast
        def safe_parse(x): 
            try: return ast.literal_eval(str(x))[:10]
            except: return [0]*10
        
        df_ask_px = pd.DataFrame(df['asks_px'].apply(safe_parse).tolist()).astype(float).astype(int).add_prefix('ask_px_')
        df_bid_px = pd.DataFrame(df['bids_px'].apply(safe_parse).tolist()).astype(float).astype(int).add_prefix('bid_px_')
        df_ask_sz = pd.DataFrame(df['asks_sz'].apply(safe_parse).tolist()).astype(float).add_prefix('ask_sz_')
        df_bid_sz = pd.DataFrame(df['bids_sz'].apply(safe_parse).tolist()).astype(float).add_prefix('bid_sz_')

    # ==========================================
    # 6. 合并输出
    # ==========================================
    # 确保索引对齐
    df_out = pd.concat([
        df[['exchange_time']].reset_index(drop=True),
        df_ask_px,
        df_bid_px,
        df_ask_sz,
        df_bid_sz,
        df[['session_id']].reset_index(drop=True)
    ], axis=1)

    # 再次清洗可能的空值
    df_out = df_out.dropna()
    
    print(f"写入输出文件: {output_path}, Shape: {df_out.shape}")
    
    # 检查文件是否存在
    file_exists = os.path.exists(output_path)
    
    # 使用 fastparquet 引擎，如果文件存在则 append=True，否则 append=False
    df_out.to_parquet(
        output_path, 
        compression='snappy', 
        engine='fastparquet',  # 必须指定引擎
        append=os.path.exists(output_path)     # 文件存在时追加，不存在时新建
    )
    
    # 返回下一个可用的 session_id
    return current_max_session_id + 1

def process_date(date_str, data_dir="data", sync=False):
    """
    Process all data for a specific date:
    1. Merge parquet files in all subdirectories.
    2. Sync to cloud storage via rclone.
    """
    day_dir = os.path.join(data_dir, date_str)
    if not os.path.exists(day_dir):
        print(f"Directory {day_dir} does not exist. Nothing to process.")
        return

    print(f"Starting processing for date: {date_str} in {day_dir}")

    # 1. Merge Parquet Files
    # Walk the directory tree: data/date/coin/channel/...
    for root, dirs, files in os.walk(day_dir):
        # We only care about directories that contain parquet files
        # Filter for .parquet files
        parquet_files = [f for f in files if f.endswith(".parquet")]
        
        # Skip if no parquet files
        if not parquet_files:
            continue
            
        # Skip if only merged.parquet exists (already processed)
        if len(parquet_files) == 1 and parquet_files[0] == 'merged.parquet':
            continue

        output_file = os.path.join(root, "merged.parquet")
        print(f"Merging files in: {root}")
        try:
            merge_parquet(root, output_file)
        except Exception as e:
            print(f"Error merging in {root}: {e}")

    # 2. Sync via Rclone
    if sync:
        print("Starting rclone sync...")
        # Use absolute path for source
        abs_source = os.path.abspath(day_dir)
        # Construct destination path: gdrive:backup/fetch_data/{date_str}
        # Note: The user requested "gdrive:backup/fetch_data/..."
        dest = f"gdrive:backup/fetch_data/{date_str}"
        
        cmd = [
            "rclone", "copy", abs_source, dest,
            "--ignore-existing", "-P"
        ]
        
        print(f"Executing: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
            print("Rclone sync completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Rclone sync failed with exit code {e.returncode}.")
        except FileNotFoundError:
            print("Error: 'rclone' command not found. Please install rclone.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge LOB data and sync to cloud.")
    parser.add_argument("--date", type=str, help="Date to process (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--data-dir", type=str, default="data", help="Root data directory (default: ./data).")
    parser.add_argument("--sync", action="store_true", help="Enable rclone sync after merging.")
    
    args = parser.parse_args()
    
    target_date = args.date
    if not target_date:
        # Default to yesterday (UTC based usually, or local)
        # Using local time here as files are written with local/utc mix but folders are dates.
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y-%m-%d")
        print(f"No date specified. Defaulting to yesterday: {target_date}")
    
    process_date(target_date, data_dir=args.data_dir, sync=args.sync)
