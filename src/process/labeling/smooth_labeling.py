import pandas as pd
import numpy as np

def apply_smooth_labeling(df: pd.DataFrame, k_values: list = None) -> pd.DataFrame:
    """
    Generate labels for the dataframe based on mid_price changes.
    
    Args:
        df: DataFrame containing 'mid_price' and 'session_id' columns.
        k_values: List of look-ahead windows. Defaults to [5, 10, 30, 50].
        
    Returns:
        DataFrame with added label columns.
    """
    if k_values is None:
        k_values = [5, 10, 30, 50]
        
    print("计算 Label...")
    
    # 这里的技巧是利用 groupby().transform 保持索引对齐，避免 apply 的低效
    grouped = df.groupby('session_id')['mid_price']
    
    for k in k_values:
        # 过去 k 个的均值
        m_minus = grouped.transform(lambda x: x.rolling(window=k).mean())
        
        # 未来 k 个的均值 (shift(-k) 获取未来数据，再 rolling)
        # 注意：user 原逻辑是 rolling(k).mean().shift(-k)
        m_plus = grouped.transform(lambda x: x.rolling(window=k).mean().shift(-k))
        
        # 计算变化率
        raw_change = (m_plus - m_minus) / m_minus
        
        # === 动态阈值 Labeling ===
        # 获取有效数据的 33% 和 66% 分位数
        valid_changes = raw_change.dropna()
        if valid_changes.empty:
            df[f'label_{k}'] = np.nan
            continue
            
        th_down = valid_changes.quantile(0.3333)
        th_up = valid_changes.quantile(0.6667)
        
        # 生成标签
        labels = pd.Series(0, index=df.index) # 默认为 0
        labels[raw_change > th_up] = 1
        labels[raw_change < th_down] = -1
        labels[raw_change.isna()] = np.nan # 保持无效值为 NaN
        
        df[f'label_{k}'] = labels
        
    return df
