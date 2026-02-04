def sample_lob_by_dollar_volume(lob_df, trades_df, threshold):
    """
    基于成交额阈值对LOB数据进行采样，并计算核心因子 (NAV, VWAP Divergence, OBI, OFI)。
    
    参数:
    lob_df (pd.DataFrame): LOB数据，Index为datetime
    trades_df (pd.DataFrame): 成交数据，Index为datetime，必须包含 'values' 列
    threshold (float): 美元成交额阈值 (Dollar Bar Threshold)
    
    返回:
    pd.DataFrame: 采样后的LOB数据，包含:
                  - 'interval_volume', 'skipped_snapshots'
                  - 'obi' (Order Book Imbalance)
                  - 'ofi_sum' (Order Flow Imbalance Sum per Bar)
                  - 'trade_imbalance' (Net Aggressor Volume)
                  - 'vwap_divergence' (Price vs VWAP)
    """
    # 1. 数据对齐与预处理
    lob_df = lob_df.sort_index()
    trades_df = trades_df.sort_index()
    
    # --- Feature Pre-calculation (Vectorized) ---
    # Use registry to calculate all registered factors
    # We ignore the agg_rules here as dollar bars use custom aggregation
    _ = lob_registry.apply_and_get_agg_rules(lob_df)
    
    # Calculate Cumsum of OFI for fast interval aggregation
    # Prepend 0 to align with 0-based indexing strategy or just use .cumsum()
    lob_ofi_cumsum = lob_df['ofi'].cumsum()
    
    # Trade Features Prep
    # Use trade registry to calculate features (e.g. signed_vol, amount)
    trade_agg_rules = trade_registry.apply_and_get_agg_rules(trades_df)
    
    # Ensure 'values' column exists for dollar bar sampling logic
    if 'values' not in trades_df.columns:
        # If amount is calculated, use it, otherwise calc manually
        if 'amount' in trades_df.columns:
            trades_df['values'] = trades_df['amount']
        else:
             trades_df['values'] = trades_df['px'] * trades_df['sz']
    
    # --------------------------------------------
    
    # 提取numpy数组以加速计算
    lob_times = lob_df.index.values
    trade_times = trades_df.index.values
    trade_values = trades_df['values'].values
    
    # 2. 计算成交额的累积和 (Cumulative Sum)
    trade_cum_vol = np.concatenate(([0], np.cumsum(trade_values)))
    
    # 3. 将 LOB 的时间戳映射到 Trade 的累积成交额索引上
    idx_map = np.searchsorted(trade_times, lob_times, side='right')
    lob_snapshot_cum_vols = trade_cum_vol[idx_map]
    
    # 4. 核心逻辑：遍历并判定阈值
    keep_indices = []       # 保留的 LOB 行号索引
    interval_volumes = []   # 该区间的实际成交额
    skipped_counts = []     # 跳过的切片数量
    
    # 保存上一次切片的索引，用于后续聚合计算
    prev_indices = [0] 
    
    last_accepted_vol = lob_snapshot_cum_vols[0]
    current_skipped = 0
    
    for i in range(1, len(lob_times)):
        current_vol = lob_snapshot_cum_vols[i]
        vol_diff = current_vol - last_accepted_vol
        
        if vol_diff >= threshold:
            keep_indices.append(i)
            prev_indices.append(i) # Record the end of this bar (start of next)
            interval_volumes.append(vol_diff)
            skipped_counts.append(current_skipped)
            
            last_accepted_vol = current_vol
            current_skipped = 0
        else:
            current_skipped += 1
            
    # 5. 构建输出 DataFrame
    if not keep_indices:
        return pd.DataFrame() 
        
    sampled_df = lob_df.iloc[keep_indices].copy()
    sampled_df['interval_volume'] = interval_volumes
    sampled_df['skipped_snapshots'] = skipped_counts
    
    # --- Aggregate Features by Bar ---
    
    # A. Aggregate OFI (Flow)
    # OFI Sum = Cumsum[End] - Cumsum[Start]
    # Start indices correspond to prev_indices[:-1] (0, i1, i2...)
    # End indices correspond to keep_indices (i1, i2, i3...)
    # Note: keep_indices are integers into lob_df
    
    starts = prev_indices[:-1]
    ends = keep_indices
    
    # Use .values to ignore index alignment issues during subtraction
    ofi_sums = lob_ofi_cumsum.iloc[ends].values - lob_ofi_cumsum.iloc[starts].values
    sampled_df['ofi_sum'] = ofi_sums
    
    # B. Aggregate Trade Factors (NAV, VWAP)
    # We need to group trades into the bars defined by (Start Time, End Time]
    # Bar i: (lob_times[starts[i]], lob_times[ends[i]]]
    
    # Define bins using the timestamps of the cut points
    # Start time is the time of the 0-th element (or previous cut)
    # End times are the times of keep_indices
    
    # Bins edges: [time[0], time[k1], time[k2]...]
    # Note: Trade timestamps must be compared to these edges.
    bin_edges = np.concatenate(([lob_times[0]], lob_times[keep_indices]))
    
    # Assign Bar ID to each trade
    # pd.cut: (edge[i], edge[i+1]] -> returns interval or bin code
    # labels=False returns integer codes 0, 1, 2...
    trade_bins = pd.cut(trades_df.index, bins=bin_edges, labels=False, include_lowest=True)
    
    # Filter trades that fall outside (if any, though edges cover full range usually)
    # Group trades
    # We only care about bins 0 to len(keep_indices)-1
    
    if len(keep_indices) > 0:
        trades_df['bar_id'] = trade_bins
        
        # GroupBy Bar ID - High Performance Aggregation
        # Only aggregate trades that fall into valid bins (0 to N-1)
        valid_trades = trades_df[trades_df['bar_id'].notna()]
        
        # Use aggregation rules from registry, plus defaults if not present
        agg_dict = {k: v for k, v in trade_agg_rules.items() if k in valid_trades.columns}
        
        # Ensure we have what we need for legacy calculations if not in registry
        if 'signed_vol' not in agg_dict and 'signed_vol' in valid_trades.columns: agg_dict['signed_vol'] = 'sum'
        if 'qty' not in agg_dict and 'qty' in valid_trades.columns: agg_dict['qty'] = 'sum'
        if 'amount' not in agg_dict and 'amount' in valid_trades.columns: agg_dict['amount'] = 'sum'
            
        trade_agg = valid_trades.groupby('bar_id').agg(agg_dict)
        
        # --- Apply Group Factors (if any) ---
        group_apply_func = trade_registry.get_group_apply_func()
        if group_apply_func:
            # Note: groupby().apply() returns a DataFrame with index 'bar_id' if func returns Series
            advanced_features = valid_trades.groupby('bar_id').apply(group_apply_func)
            # Join with trade_agg (both indexed by bar_id)
            trade_agg = trade_agg.join(advanced_features)
        
        # Calculate Factors
        # 1. Trade Imbalance (NAV)
        # Avoid division by zero
        if 'signed_vol' in trade_agg.columns and 'qty' in trade_agg.columns:
            trade_agg['trade_imbalance'] = trade_agg['signed_vol'] / (trade_agg['qty'] + 1e-8)
        else:
            trade_agg['trade_imbalance'] = np.nan
        
        # 2. VWAP Divergence
        # VWAP = Sum(Amount) / Sum(Qty)
        if 'amount' in trade_agg.columns and 'qty' in trade_agg.columns:
            trade_agg['vwap'] = trade_agg['amount'] / (trade_agg['qty'] + 1e-8)
        else:
            trade_agg['vwap'] = np.nan
        
        # Map aggregated factors back to sampled_df
        # sampled_df has length N. trade_agg should have index 0..N-1
        # However, some bins might have no trades (empty bars?), producing missing indices in trade_agg
        
        # Reindex to ensure alignment
        trade_agg = trade_agg.reindex(range(len(keep_indices)))
        
        # Align: sampled_df is index-based (Time), trade_agg is integer-index based (Bar ID 0..N)
        # We can assign array directly
        sampled_df['trade_imbalance'] = trade_agg['trade_imbalance'].values
        
        # Calculate Divergence: (Close Price - VWAP) / Close Price
        # Close Price is the Mid Price at the sample moment (from sampled_df)
        # If 'mid_price' exists (it should from process_lob)
        if 'mid_price' in sampled_df.columns:
            close_prices = sampled_df['mid_price'].values
        else:
            # Fallback to calculation
            # Assuming bid_px_0 and ask_px_0 exist
            close_prices = (sampled_df['bid_px_0'] + sampled_df['ask_px_0']) / 2
            
        sampled_df['vwap_divergence'] = (close_prices - trade_agg['vwap'].values) / (close_prices + 1e-8)
        
    else:
        # No samples
        sampled_df['trade_imbalance'] = np.nan
        sampled_df['vwap_divergence'] = np.nan

    return sampled_df