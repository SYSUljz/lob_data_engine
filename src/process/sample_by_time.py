import pandas as pd
import glob
import os
        
import pandas as pd
import numpy as np
import warnings
# Import registry and the factors module to trigger registration
from fetch_data.src.process.calculate_factor.registry import lob_registry,trade_registry
import fetch_data.src.process.calculate_factor.lob_factors 
from fetch_data.src.process.calculate_factor.trade_factors import prepare_trade_features, calculate_advanced_microstructure

def sample_lob_by_time(lob_df, trades_df, interval):
    """
    基于时间间隔对LOB数据进行采样，并计算核心因子 (OHLC, VWAP, Volatility, Spread/Imbalance Mean)。
    
    参数:
    lob_df (pd.DataFrame): LOB数据，Index为datetime
    trades_df (pd.DataFrame): 成交数据，Index为datetime，必须包含 'values' 列
    interval (str): 时间间隔 (Time Bar Interval), e.g., '1m', '5s'
    
    返回:
    pd.DataFrame: 采样后的LOB数据
    """
    # 1. 数据对齐与预处理
    lob_df = lob_df.sort_index()
    trades_df = trades_df.sort_index()
    
    # --- Feature Pre-calculation ---
    trades_df = prepare_trade_features(trades_df)
      
    # Prepare LOB Factors
    if 'mid_price' not in lob_df.columns:
        lob_df['mid_price'] = (lob_df['bid_px_0'] + lob_df['ask_px_0']) / 2
    
    lob_df['spread'] = lob_df['ask_px_0'] - lob_df['bid_px_0']

    # 2. Resample LOB
    # Base aggregation rules
    agg_rules = {
        'mid_price': 'last',
        'spread': 'mean',
        'session_id':'last'
    }
    
    # Use registry to calculate all registered factors and get their agg rules
    factor_rules = lob_registry.apply_and_get_agg_rules(lob_df)
    agg_rules.update(factor_rules)
    
    sampled_df = lob_df.resample(interval).agg(agg_rules)
    sampled_df['snapshot_count'] = lob_df['mid_price'].resample(interval).count()
    
    # Drop empty bins (where no LOB state exists)
    sampled_df = sampled_df.dropna(subset=['mid_price'])
    
    # 3. Aggregate Trades
    if 'values' not in trades_df.columns:
         trades_df['values'] = trades_df['px'] * trades_df['sz']

    trades_df['px']=trades_df['px'].astype(float)
    
    # Calculate Tick Returns for Realized Volatility
    trades_df['tick_ret'] = trades_df['px'].pct_change()

    trade_resampler = trades_df.resample(interval)

    # OHLC
    ohlc = trade_resampler['px'].ohlc()
    
    # Realized Volatility (Std of tick returns)
    realized_vol = trade_resampler['tick_ret'].std().rename('realized_volatility')

    # Sums
    trade_sums = trade_resampler.agg({
        'signed_vol': 'sum',
        'qty': 'sum',
        'amount': 'sum',
        'values': 'sum'
    })

    # Combine Trade Features
    trade_features = pd.concat([ohlc, trade_sums, realized_vol], axis=1)
    
    # --- Apply Group Factors (if any) ---
    group_apply_func = trade_registry.get_group_apply_func()
    if group_apply_func:
        advanced_features = trade_resampler.apply(group_apply_func)
        # advanced_features index is DatetimeIndex, matches trade_features
        trade_features = pd.concat([trade_features, advanced_features], axis=1)
    
    # 4. Merge Trades info into Sampled LOB
    # Reindex trade_features to match sampled_df index
    trade_features = trade_features.reindex(sampled_df.index)
    
    # Fill missing sums with 0
    trade_features[['signed_vol', 'qty', 'amount', 'values']] = \
        trade_features[['signed_vol', 'qty', 'amount', 'values']].fillna(0)
    
    # Join
    sampled_df = sampled_df.join(trade_features)
    sampled_df.rename(columns={'values': 'interval_volume'}, inplace=True)
    
    # Calculate Factors
    qty = sampled_df['qty'] + 1e-8
    sampled_df['trade_imbalance'] = sampled_df['signed_vol'] / qty
    
    sampled_df['vwap'] = sampled_df['amount'] / qty
    
    # Mid Price for divergence (Use Trade Close if available, else Mid Price)
    close_prices = sampled_df['close'].combine_first(sampled_df['mid_price'])
        
    sampled_df['vwap_divergence'] = (close_prices - sampled_df['vwap']) / (close_prices + 1e-8)
    
    # Handle no-trade cases for Divergence
    mask_no_trade = sampled_df['qty'] < 1e-7
    sampled_df.loc[mask_no_trade, 'vwap_divergence'] = 0
    
    return sampled_df

        