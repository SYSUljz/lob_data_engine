import pandas as pd
import numpy as np

def apply_triple_barrier_labeling(df, 
                                  price_col='mid_price', 
                                  vol_col='volatility',
                                  vertical_barrier_n=20, 
                                  pt=1.0, 
                                  sl=1.0,
                                  min_ret=0.0005):
    """
    针对 Dollar Bars 的三势垒标记法 (Triple Barrier Method)。
    增加输出：
    - t0：信号 / 观察点的 bar index（当前行 index）
    - t1：最先触碰 barrier 的 bar index；若未触碰，则为垂直势垒 t0 + N
    """

    df = df.sort_values(['session_id', 'datetime']).reset_index(drop=True)

    # 价格不存在就自动构造 mid_price
    if price_col not in df.columns:
        ap = 'ask_px_0' if 'ask_px_0' in df.columns else 'ask_px_1'
        bp = 'bid_px_0' if 'bid_px_0' in df.columns else 'bid_px_1'
        df[price_col] = (df[ap] + df[bp]) / 2.0

    use_dynamic_vol = vol_col in df.columns

    out_labels = []
    out_t0 = []
    out_t1 = []

    # 按 session 处理
    for session, group in df.groupby('session_id'):
        group = group.copy()

        # --- 构造未来价格矩阵 ---
        future_prices = pd.DataFrame({
            i: group[price_col].shift(-i)
            for i in range(1, vertical_barrier_n + 1)
        })

        current_price = group[price_col].values
        returns_matrix = future_prices.sub(current_price, axis=0).div(current_price, axis=0)

        # --- 阈值 ---
        if use_dynamic_vol:
            target = group[vol_col].values
            upper_bound = np.maximum(target * pt, min_ret)
            lower_bound = np.minimum(-target * sl, -min_ret)
        else:
            upper_bound = np.full(len(group), min_ret * pt)
            lower_bound = np.full(len(group), -min_ret * sl)

        # --- 命中矩阵 ---
        hit_upper = returns_matrix.ge(upper_bound[:, None])
        hit_lower = returns_matrix.le(lower_bound[:, None])

        has_hit_upper = hit_upper.any(axis=1)
        has_hit_lower = hit_lower.any(axis=1)

        # idxmax → 首次 True 的列名（1..N）
        first_upper_idx = hit_upper.idxmax(axis=1)
        first_lower_idx = hit_lower.idxmax(axis=1)

        # --- label 计算 ---
        session_labels = pd.Series(0, index=group.index)

        both_hit = has_hit_upper & has_hit_lower
        up_wins = both_hit & (first_upper_idx <= first_lower_idx)
        down_wins = both_hit & (first_upper_idx > first_lower_idx)

        session_labels[up_wins] = 1
        session_labels[down_wins] = -1
        session_labels[has_hit_upper & (~has_hit_lower)] = 1
        session_labels[has_hit_lower & (~has_hit_upper)] = -1

        # --- t0：当前 bar index ---
        t0_index = group.index.to_series()

        # --- t1：barrier 触发位置 ---
        # 如果 label = 1，则 t1 = t0 + first_upper_idx
        # 如果 label = -1，则 t1 = t0 + first_lower_idx
        # 如果 label = 0，则 t1 = t0 + vertical_barrier_n

        t1 = pd.Series(index=group.index, dtype=float)

        # label = 1
        mask_up = session_labels == 1
        t1[mask_up] = group.index[mask_up] + first_upper_idx[mask_up]

        # label = -1
        mask_down = session_labels == -1
        t1[mask_down] = group.index[mask_down] + first_lower_idx[mask_down]

        # label = 0 → 垂直 barrier
        mask_neutral = session_labels == 0
        t1[mask_neutral] = group.index[mask_neutral] + vertical_barrier_n

        out_labels.append(session_labels)
        out_t0.append(t0_index)
        out_t1.append(t1)

    # 合并
    df["label"] = pd.concat(out_labels).sort_index()
    df["t0"] = pd.concat(out_t0).sort_index()
    df["t1"] = pd.concat(out_t1).sort_index()

    # 删除没有未来数据的尾部（可选）
    df = df[df["t1"] < len(df)].copy()

    return df


def get_volatility(df, price_col='mid_price', span=100):
    """
    计算基于 Dollar Bars 的波动率 (作为动态阈值的基础)
    通常使用 EWM 标准差
    """
    # 计算对数收益率
    df['log_ret'] = np.log(df[price_col] / df[price_col].shift(1))
    # EWM Std Dev
    return df['log_ret'].ewm(span=span).std()