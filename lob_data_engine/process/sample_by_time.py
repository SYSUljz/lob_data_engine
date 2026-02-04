import pandas as pd
import glob
import os
import numpy as np
import warnings
# Import registry and the factors module to trigger registration
from fetch_data.src.process.calculate_factor.registry import lob_registry, trade_registry
import fetch_data.src.process.calculate_factor.lob_factors 
import fetch_data.src.process.calculate_factor.trade_factors

def sample_lob_by_time(lob_df, trades_df, interval):
    """
    基于时间间隔对LOB数据进行采样，并计算核心因子 (OHLC, VWAP, Volatility, Spread/Imbalance Mean).
    
    Refactored to use FactorRegistry for Open-Closed Principle adherence.
    """
    # 1. Data Alignment & Sorting
    lob_df = lob_df.sort_index()
    trades_df = trades_df.sort_index()
    
    # 2. Validation
    lob_df = lob_registry.validate(lob_df)
    trades_df = trade_registry.validate(trades_df)
    
    # 3. LOB Processing
    # Base agg rules for metadata
    agg_rules = {
        'session_id': 'last'
    }
    
    # Apply LOB factors (includes mid_price, spread, OBI, etc.)
    # This modifies lob_df in-place adding feature columns
    factor_rules = lob_registry.apply_and_get_agg_rules(lob_df)
    agg_rules.update(factor_rules)
    
    # Resample LOB
    sampled_df = lob_df.resample(interval).agg(agg_rules)
    sampled_df['snapshot_count'] = lob_df.iloc[:, 0].resample(interval).count()
    
    # Drop empty bins (where no LOB state exists - check a core column like mid_price)
    if 'mid_price' in sampled_df.columns:
        sampled_df = sampled_df.dropna(subset=['mid_price'])
    
    # 4. Trades Processing
    # Apply Trade row-wise factors (includes tick_ret, side, amount, values)
    _ = trade_registry.apply_and_get_agg_rules(trades_df)
    
    # Resample Trades using Group Factors (OHLC, Volatility, Sums, Advanced)
    trade_resampler = trades_df.resample(interval)
    group_apply_func = trade_registry.get_group_apply_func()
    
    if group_apply_func:
        # Calculate all group factors in one pass
        trade_features = trade_resampler.apply(group_apply_func)
    else:
        # Should not happen if basic stats are registered
        trade_features = pd.DataFrame(index=sampled_df.index)

    # 5. Merge & Post-Process
    # Reindex trade_features to match sampled_df index (fill missing with appropriate defaults?)
    # Note: trade_features index might be slightly different if periods are missing, strict alignment to LOB is usually desired
    trade_features = trade_features.reindex(sampled_df.index)
    
    # Fill missing sums with 0 (for intervals with no trades)
    fill_zero_cols = ['signed_vol', 'qty', 'amount', 'values', 'liq_usd_vol', 'liq_freq']
    for col in fill_zero_cols:
        if col in trade_features.columns:
            trade_features[col] = trade_features[col].fillna(0)
            
    # Join
    sampled_df = sampled_df.join(trade_features)
    
    if 'values' in sampled_df.columns:
        sampled_df.rename(columns={'values': 'interval_volume'}, inplace=True)
    
    # Fill NaN values for derived factors if necessary
    # Note: trade_factors.py handles basic defaults, but if resampling created empty rows, they might be NaN.
    # We may want to fill 'vwap_divergence' and 'trade_imbalance' with 0 for empty bins.
    if 'vwap_divergence' in sampled_df.columns:
        sampled_df['vwap_divergence'] = sampled_df['vwap_divergence'].fillna(0)
    if 'trade_imbalance' in sampled_df.columns:
        sampled_df['trade_imbalance'] = sampled_df['trade_imbalance'].fillna(0)

    return sampled_df

        