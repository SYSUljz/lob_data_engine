import pandas as pd
import os
import glob
import argparse
import subprocess
from datetime import datetime, timedelta
def merge_parquet(folder, output_file):
    files = glob.glob(os.path.join(folder, "*.parquet"))
    
    if len(files) == 1 and os.path.basename(files[0])=='merged.parquet':
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