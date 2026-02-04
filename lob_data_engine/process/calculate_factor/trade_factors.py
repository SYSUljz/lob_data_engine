import pandas as pd
import numpy as np
from .registry import trade_registry

@trade_registry.register(agg_rule={
    'qty': 'sum',
    'signed_vol': 'sum',
    'amount': 'sum'
})
def prepare_trade_features(trades_df):
    """
    Prepare trade features for aggregation.
    
    Adds:
    - 'side': -1 for aggressor sell (maker buy), 1 for aggressor buy (maker sell)
    - 'signed_vol': qty * side
    - 'amount': price * qty
    """
    df = trades_df.copy()
    
    # 1. Determine Aggressor Side
    # side = a -> Buyer is Maker -> Seller is Aggressor -> Price moves down -> Side = -1
    # side = b -> Buyer is Taker -> Buyer is Aggressor -> Price moves up -> Side = 1
    # Note: Ensure 'is_buyer_maker' is boolean
    if 'side' not in df.columns:
        raise KeyError("Column 'side' is missing in trades_df. Cannot compute aggressor direction.")
    valid_side = {"A", "B"}
    if not df["side"].isin(valid_side).all():
        bad_vals = df.loc[~df["side"].isin(valid_side), "side"].unique()
        raise ValueError(f"Invalid side values encountered: {bad_vals}. "
                         f"Allowed: {valid_side}")
    df["side"] = df["side"].map({"A": -1, "B": 1})
    
    # 2. Signed Volume
    # Ensure 'qty' is float. In some datasets it might be 'sz'
    if 'qty' not in df.columns and 'sz' in df.columns:
        df['qty'] = df['sz'].astype(float)
    if 'price' not in df.columns and 'px' in df.columns:
        df['price'] = df['px'].astype(float)
        
    df['signed_vol'] = df['qty'] * df['side']
    
    # 3. Dollar Amount
    df['amount'] = df['price'] * df['qty']
    
    return df



import pandas as pd
import numpy as np

# 定义 Hyperliquid 的清算 Hash (全0)
LIQ_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000"

@trade_registry.register_group()
def calculate_advanced_microstructure(group):
    """
    该函数设计用于 groupby('bar_id').apply() 中，
    输入的是一个时间切片内的所有 trades (DataFrame)。
    """
    # ---------------------------------
    # 1. 数据预处理
    # ---------------------------------
    # 区分 正常交易(Normal) 和 强平交易(Liquidation)
    is_liq = group['hash'] == LIQ_HASH
    
    # 提取正常交易的数据
    normal_trades = group.loc[~is_liq]
    
    # 提取强平交易的数据
    liq_trades = group.loc[is_liq]
    
    # ---------------------------------
    # 2. 因子：清算密度 (Liquidation Density)
    # ---------------------------------
    # 指标 A: 强平总金额 (Total Liquidation Value)
    # 反映了多大资金盘子爆仓了，是绝对值的痛感
    liq_vol_usd = liq_trades['amount'].sum() if not liq_trades.empty else 0.0
    
    # 指标 B: 强平参与度 (Liquidation Ratio)
    # 强平金额占该时间段总成交额的比例
    total_vol_usd = group['amount'].sum()
    liq_ratio = liq_vol_usd / total_vol_usd if total_vol_usd > 1e-8 else 0.0
    
    # 指标 C: 强平频次 (Liquidation Frequency)
    # 虽然无法区分是一个大户被斩仓还是多个小户，但 fill 的数量反映了对盘口的冲击次数
    liq_count = len(liq_trades)

    # ---------------------------------
    # 3. 因子：Taker 参与者复杂度 (Taker Complexity)
    # 仅针对正常交易 (Normal Trades) 计算，排除清算干扰
    # ---------------------------------
    
    if normal_trades.empty:
        taker_count = 0
        taker_hhi = 1.0 # 没交易或者是全清算，视为极端情况
        max_taker_share = 0.0
    else:
        # A. 活跃 Taker 数量 (Unique Takers)
        # 也就是有多少个不同的 hash
        taker_count = normal_trades['hash'].nunique()
        
        # B. Taker 集中度 (HHI - Herfindahl-Hirschman Index)
        # 逻辑：先按 Hash 聚合算出每个 Taker 的成交量，然后算 HHI
        # 1. 每个 Hash 的成交量
        vol_per_hash = normal_trades.groupby('hash')['amount'].sum()
        total_normal_vol = vol_per_hash.sum()
        
        if total_normal_vol > 1e-8:
            # 2. 计算每个 Hash 的市场份额 (Market Share)
            shares = vol_per_hash / total_normal_vol
            # 3. HHI = Sum(Share^2)
            # HHI 越接近 1，说明成交量集中在 1 个 Hash 手里（巨鲸）
            # HHI 越接近 0，说明成交量很分散（散户）
            taker_hhi = (shares ** 2).sum()
            
            # C. 最大单一 Taker 占比 (Dominance)
            # 也可以作为一个辅助因子：最大的那个人买/卖了多少比例
            max_taker_share = shares.max()
        else:
            taker_hhi = 0.0
            max_taker_share = 0.0

    return pd.Series({
        'liq_usd_vol': liq_vol_usd,       # 清算总金额
        'liq_ratio': liq_ratio,           # 清算金额占比
        'liq_freq': liq_count,            # 清算成交笔数
        'taker_uniq_cnt': taker_count,    # 独立 Taker 数量
        'taker_hhi': taker_hhi,           # Taker 集中度 (0=分散, 1=集中)
        'whale_dominance': max_taker_share # 最大单一 Taker 占比
    })

# --- 使用示例 ---
# 假设 trades_df 已经有了 bar_id (例如按1分钟对齐的时间戳) 和 amount (price * qty)
# 
# features_df = trades_df.groupby('bar_id').apply(calculate_advanced_microstructure)
# print(features_df.head())



def aggregate_trade_factors(grouped_trades):
    """
    Calculate aggregated trade factors from grouped data.
    Expected to be used with a groupby object, e.g. df.groupby('bar_id').apply(aggregate_trade_factors)
    
    However, for performance, we should use dictionary aggregation first, then compute derived columns.
    
    Returns:
        DataFrame with 'nav' (Net Aggressor Volume) and 'vwap' (Volume Weighted Average Price)
    """
    # This function might not be directly used if we do optimized aggregation in the main flow.
    # It serves as a logic reference.
    pass
