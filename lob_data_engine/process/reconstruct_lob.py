import pandas as pd
import numpy as np

def reconstruct_lob(df_snapshot, df_diff, depth=10):
    # 1. 预处理
    df_diff = df_diff.sort_values('U').reset_index(drop=True)
    # 预先计算断裂点 (diff_gap_mask[i] 为 True 表示 i 与 i-1 不连续)
    # u 是上一条的结尾，U 是这一条的开始
    diff_gap_mask = df_diff['U'].values[1:] != df_diff['u'].values[:-1] + 1
    diff_gap_mask = np.insert(diff_gap_mask, 0, False) # 第一条设为 False，由快照逻辑接管
    
    # 2. 识别所有连续片段 (Segments)
    # 我们利用 cumsum 给每个连续的 diff 块打上 Group ID
    df_diff['group_id'] = diff_gap_mask.cumsum()
    
    segments = []
    processed_groups = set()

    # 3. 遍历 Snapshot，寻找每个片段的起点
    # 按照时间或 ID 排序快照
    df_snapshot = df_snapshot.sort_values('lastUpdateId')
    
    for _, snap in df_snapshot.iterrows():
        last_id = snap['lastUpdateId']
        
        # 寻找该快照在 diff 中的第一条：U <= last_id + 1 <= u
        # 这是交易所（如 Binance）推荐的对齐逻辑
        match_cond = (df_diff['U'] <= last_id + 1) & (df_diff['u'] >= last_id + 1)
        start_indices = df_diff.index[match_cond]
        
        if len(start_indices) > 0:
            start_idx = start_indices[0]
            group_id = df_diff.loc[start_idx, 'group_id']
            
            # 如果这个连续片段已经处理过，则跳过（避免多个快照指向同一个片段）
            if group_id in processed_groups:
                continue
                
            # 找到该 group 的结束位置
            group_end_idx = df_diff[df_diff['group_id'] == group_id].index[-1]
            
            segments.append({
                'snap': snap,
                'start_idx': start_idx,
                'end_idx': group_end_idx,
                'diff':group_end_idx-start_idx
            })
            processed_groups.add(group_id)

    # 4. 迭代处理每个片段
    results = []
    for seg in segments:
        snap = seg['snap']
        # 初始化订单簿
        current_bids = {float(x[0]): float(x[1]) for x in snap['bids'] if float(x[1]) > 0}
        current_asks = {float(x[0]): float(x[1]) for x in snap['asks'] if float(x[1]) > 0}
        
        # 提取该片段对应的 diff 数据
        df_seg = df_diff.loc[seg['start_idx'] : seg['end_idx']]
        
        for idx, row in df_seg.iterrows():
            # 更新逻辑 (同之前)
            for p_str, q_str in row['b']:
                p, q = float(p_str), float(q_str)
                if q == 0: current_bids.pop(p, None)
                else: current_bids[p] = q
            
            for p_str, q_str in row['a']:
                p, q = float(p_str), float(q_str)
                if q == 0: current_asks.pop(p, None)
                else: current_asks[p] = q
                
            # 提取 Depth
            best_bids = sorted(current_bids.items(), key=lambda x: x[0], reverse=True)[:depth]
            best_asks = sorted(current_asks.items(), key=lambda x: x[0], reverse=False)[:depth]
            
            results.append({
                'time': row['time'],
                'U': row['U'],
                'bids_10': best_bids,
                'asks_10': best_asks,
                'segment_id': row['group_id']
            })

    return pd.DataFrame(results)
import pandas as pd
import numpy as np
from sortedcontainers import SortedDict

def reconstruct_lob_optimized(df_snapshot, df_diff, depth=10):
    # --- 1. 预处理 (保持你的逻辑，但做向量化加速) ---
    df_diff = df_diff.sort_values('U').reset_index(drop=True)
    
    # 向量化计算 gap mask
    u_values = df_diff['u'].values
    U_values = df_diff['U'].values
    diff_gap_mask = np.zeros(len(df_diff), dtype=bool)
    if len(df_diff) > 0:
        # U[i] != u[i-1] + 1
        diff_gap_mask[1:] = U_values[1:] != (u_values[:-1] + 1)
    
    df_diff['group_id'] = diff_gap_mask.cumsum()
    
    # 关键优化：预先将 b 和 a 列里的 string 转 float
    # 这里假设数据量很大，使用 apply 可能会慢，但在循环外做一次好过循环内做千万次
    # 如果追求极致，可以在读取 parquet/csv 时就指定类型，或者使用 ast.literal_eval
    # 这里演示逻辑：
    def parse_updates(update_list):
        if update_list is None or len(update_list) == 0:
            return []

        return [(float(p), float(q)) for p, q in update_list]

    df_diff['b_parsed'] = df_diff['b'].apply(parse_updates)
    df_diff['a_parsed'] = df_diff['a'].apply(parse_updates)

    # --- 2. 识别片段逻辑 (保持你的逻辑) ---
    # ... (省略这部分代码，假设 segments 已经生成好) ...
    # 为演示方便，我们假设 segments 列表已经有了，并且结构一致
    
    # 临时模拟 segments 生成以便代码跑通 (实际请保留你的原始逻辑)
    # ... 你的 segments 生成代码 ...
    processed_groups = set()

    # 3. 遍历 Snapshot，寻找每个片段的起点
    # 按照时间或 ID 排序快照
    df_snapshot = df_snapshot.sort_values('lastUpdateId')
    segments=[]
    for _, snap in df_snapshot.iterrows():
        last_id = snap['lastUpdateId']
        
        # 寻找该快照在 diff 中的第一条：U <= last_id + 1 <= u
        # 这是交易所（如 Binance）推荐的对齐逻辑
        match_cond = (df_diff['U'] <= last_id + 1) & (df_diff['u'] >= last_id + 1)
        start_indices = df_diff.index[match_cond]
        
        if len(start_indices) > 0:
            start_idx = start_indices[0]
            group_id = df_diff.loc[start_idx, 'group_id']
            
            # 如果这个连续片段已经处理过，则跳过（避免多个快照指向同一个片段）
            if group_id in processed_groups:
                continue
                
            # 找到该 group 的结束位置
            group_end_idx = df_diff[df_diff['group_id'] == group_id].index[-1]
            
            segments.append({
                'snap': snap,
                'start_idx': start_idx,
                'end_idx': group_end_idx,
                'diff':group_end_idx-start_idx
            })
            processed_groups.add(group_id)

    results = []
    
    # --- 3. 核心循环优化 ---
    for seg in segments:
        snap = seg['snap']
        
        # 优化点 A: 使用 SortedDict
        # Bids: 价格从高到低 (SortedDict 默认从小到大，取最后 depth 个)
        # Asks: 价格从低到高
        current_bids = SortedDict({float(x[0]): float(x[1]) for x in snap['bids'] if float(x[1]) > 0})
        current_asks = SortedDict({float(x[0]): float(x[1]) for x in snap['asks'] if float(x[1]) > 0})
        
        # 提取数据，转换为 Numpy 数组或者原生 List 迭代，避开 iterrows
        # 使用 itertuples 比 iterrows 快 10-20 倍
        subset_cols = ['time', 'U', 'b_parsed', 'a_parsed', 'group_id']
        seg_data = df_diff.loc[seg['start_idx'] : seg['end_idx'], subset_cols].itertuples(index=False)
        
        for row in seg_data:
            # row[2] is b_parsed, row[3] is a_parsed
            
            # Bids 更新
            for p, q in row[2]:
                if q == 0:
                    # 安全删除，避免 key 不存在报错
                    if p in current_bids: del current_bids[p]
                else:
                    current_bids[p] = q
            
            # Asks 更新
            for p, q in row[3]:
                if q == 0:
                    if p in current_asks: del current_asks[p]
                else:
                    current_asks[p] = q
            
            # 优化点 B: O(K) 提取 Depth
            # Bids: 取最后 10 个 (最大的价格)，并反转顺序
            # SortedDict.items() 返回的是迭代器，islice 或负索引切片
            # 注意：SortedDict 切片返回的是 key，我们需要 items
            
            # 技巧：直接利用 SortedDict 的高效索引
            # limit depth to available length
            bid_len = len(current_bids)
            ask_len = len(current_asks)
            
            # 取最大的 depth 个 bids (尾部切片)
            bids_10 = []
            if bid_len > 0:
                # iter items in reverse order efficiently
                start = max(0, bid_len - depth)
                # islice 或者直接用 range 索引 (SortedDict 支持 logN 的索引访问)
                # 为了极致性能，SortedDict 的 iloc 是神器
                # items = current_bids.items() -> but getting last 10
                # keys = current_bids.keys()[-depth:] -> reversed
                
                # SortedDict 这里的性能权衡：
                # 如果 depth 很小 (10)，直接用负数索引取 keys 很快
                keys = current_bids.keys()[-depth:]
                # 必须反转，因为 Orderbook Bids 是从高到低
                bids_10 = [(k, current_bids[k]) for k in reversed(keys)]

            # 取最小的 depth 个 asks (头部切片)
            asks_10 = []
            if ask_len > 0:
                keys = current_asks.keys()[:depth]
                asks_10 = [(k, current_asks[k]) for k in keys]

            results.append({
                'time': row[0],
                'U': row[1],
                'bids_10': bids_10,
                'asks_10': asks_10,
                'segment_id': row[4]
            })

    return pd.DataFrame(results)
if __name__ == "__main__":
# 运行恢复
    print('start')
    df_snapshot = pd.read_parquet('/home/jack_li/python/LOB_research/data/2026-01-01/BTCUSDT/depthSnapshot/merged.parquet')
    df_diff = pd.read_parquet('/home/jack_li/python/LOB_research/data/2026-01-01/BTCUSDT/depthUpdate/merged.parquet')


    df_snapshot['time']=pd.to_datetime(df_snapshot['E'],unit='ms')
    df_diff['time']=pd.to_datetime(df_diff['E'],unit='ms')
    result = reconstruct_lob_optimized(df_snapshot, df_diff)
    print('end')