import pandas as pd
import numpy as np

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
