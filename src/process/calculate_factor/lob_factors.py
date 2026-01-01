import pandas as pd
import numpy as np

def calculate_obi(lob_df, levels=5):
    """
    Calculate Order Book Imbalance (OBI).
    
    Logic:
    Static pressure indicator. If bid wall is thicker than ask wall, price tends to rise.
    Weighted calculation of top 'levels' depths.
    
    Formula:
    OBI = (Sum(Bid_Qty_i * w_i) - Sum(Ask_Qty_i * w_i)) / (Sum(Bid_Qty_i * w_i) + Sum(Ask_Qty_i * w_i))
    """
    total_bid_qty = 0
    total_ask_qty = 0
    
    # Weights decay: 1.0, 0.85, 0.70...
    weights = [1.0 - (i * 0.15) for i in range(levels)]
    
    # Adjust for 0-based indexing (0 to levels-1)
    for i in range(levels):
        if i >= len(weights):
            w = 0.1 # Minimum weight fallback
        else:
            w = weights[i]
            
        # Using project specific column names: bid_sz_0, ask_sz_0, etc.
        bid_col = f'bid_sz_{i}'
        ask_col = f'ask_sz_{i}'
        
        if bid_col in lob_df.columns and ask_col in lob_df.columns:
            total_bid_qty += lob_df[bid_col] * w
            total_ask_qty += lob_df[ask_col] * w
        
    # Generate factor
    # Add 1e-8 to prevent division by zero
    obi = (total_bid_qty - total_ask_qty) / (total_bid_qty + total_ask_qty + 1e-8)
    
    return obi

def calculate_ofi(lob_df):
    """
    Calculate Order Flow Imbalance (OFI) - Cont et al. 2014.
    
    Logic:
    Dynamic intent indicator. Measures changes in the order book (Level 1).
    OFI = Bid Inflow - Ask Outflow
    """
    df = lob_df.copy()
    
    # Level 1 columns
    bid_p1_col = 'bid_px_0'
    bid_q1_col = 'bid_sz_0'
    ask_p1_col = 'ask_px_0'
    ask_q1_col = 'ask_sz_0'
    
    # Get t-1 data
    df['bid_p1_prev'] = df[bid_p1_col].shift(1)
    df['bid_q1_prev'] = df[bid_q1_col].shift(1)
    df['ask_p1_prev'] = df[ask_p1_col].shift(1)
    df['ask_q1_prev'] = df[ask_q1_col].shift(1)
    
    # --- Bid Inflow ---
    # Bid Price Up -> Entire new volume is inflow (+q_t)
    # Bid Price Down -> Old volume removed/eaten (-q_prev)
    # Bid Price Same -> Change in volume (q_t - q_prev)
    bid_inflow = np.where(df[bid_p1_col] > df['bid_p1_prev'], df[bid_q1_col],
                 np.where(df[bid_p1_col] < df['bid_p1_prev'], -df['bid_q1_prev'],
                          df[bid_q1_col] - df['bid_q1_prev']))
                          
    # --- Ask Outflow ---
    # Ask Price Down -> Selling pressure increases -> Outflow
    # Ask Price Up -> Old volume removed -> Inflow (negative outflow)
    # Ask Price Same -> Change in volume
    
    ask_outflow = np.where(df[ask_p1_col] > df['ask_p1_prev'], -df['ask_q1_prev'],
                  np.where(df[ask_p1_col] < df['ask_p1_prev'], df[ask_q1_col],
                           df[ask_q1_col] - df['ask_q1_prev']))
                           
    # OFI = Bid Inflow - Ask Outflow
    ofi = pd.Series(bid_inflow - ask_outflow, index=df.index)
    
    # Handle the first NaN row (caused by shift)
    ofi.iloc[0] = 0
    
    return ofi

import pandas as pd
import numpy as np

def calculate_depth_structure(lob_df, levels=10):
    """
    计算订单簿的分布结构特征：重心 (Center of Gravity) 和 头部集中度 (Concentration).
    
    Returns:
    - bid_cog: 买单重心 (0 表示重心在买一，数值越大重心越靠后)
    - ask_cog: 卖单重心
    - bid_concentration: 买单头部集中度 (买一量占前N档总量的比例)
    - ask_concentration: 卖单头部集中度
    """
    df = pd.DataFrame(index=lob_df.index)
    
    # 初始化累加器
    total_bid_vol = 0
    total_ask_vol = 0
    weighted_bid_index = 0
    weighted_ask_index = 0
    
    # 提取 Level 0 的量用于计算集中度
    bid_vol_0 = lob_df['bid_sz_0']
    ask_vol_0 = lob_df['ask_sz_0']
    
    for i in range(levels):
        bid_col = f'bid_sz_{i}'
        ask_col = f'ask_sz_{i}'
        
        if bid_col in lob_df.columns and ask_col in lob_df.columns:
            # 获取当前档位的量
            b_vol = lob_df[bid_col]
            a_vol = lob_df[ask_col]
            
            # 累加总量
            total_bid_vol += b_vol
            total_ask_vol += a_vol
            
            # 累加“力矩” (量 * 距离)
            # 距离就是 index i (0, 1, 2...)
            weighted_bid_index += b_vol * i
            weighted_ask_index += a_vol * i
            
    # 防止除以零
    total_bid_vol = total_bid_vol.replace(0, 1e-8)
    total_ask_vol = total_ask_vol.replace(0, 1e-8)
    
    # 1. 计算重心 (Center of Gravity)
    # 范围: 0 ~ (levels-1)
    # 越小说明资金越倾向于挂在盘口（Front-heavy）
    # 越大说明资金越倾向于挂在深处（Back-heavy）
    df['bid_cog'] = weighted_bid_index / total_bid_vol
    df['ask_cog'] = weighted_ask_index / total_ask_vol
    
    # 2. 计算头部集中度 (Concentration)
    # 范围: 0 ~ 1
    # 1.0 表示所有量都在 Level 0
    # 低值表示量分散在深处
    df['bid_concentration'] = bid_vol_0 / total_bid_vol
    df['ask_concentration'] = ask_vol_0 / total_ask_vol
    
    # 3. (可选) 扩展指标：重心差 (COG Imbalance)
    # 如果 bid_cog 很小 (0.5) 而 ask_cog 很大 (5.0)
    # 说明买方急于成交（挂在前面），卖方在防守（挂在后面），通常利好价格上涨
    df['cog_diff'] = df['ask_cog'] - df['bid_cog']
    
    return df

# --- 使用示例 ---
# 假设你已经有了 lob_df
# df_structure = calculate_depth_structure(lob_df, levels=10)
# print(df_structure.head())