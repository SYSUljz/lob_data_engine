
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

def plot_feature_correlations(df, target_col, top_n=20, output_path=None):
    """
    Calculates and plots the correlation of features with the target variable.
    """
    if target_col not in df.columns:
        print(f"Target column '{target_col}' not found in DataFrame.")
        return

    # specific: only calculate numeric correlations
    metric_df = df.select_dtypes(include=[np.number])
    
    if target_col not in metric_df.columns:
         pass

    # Compute correlations
    corr = metric_df.corrwith(df[target_col]).sort_values(ascending=False)
    
    # Drop the target itself
    corr = corr.drop(target_col, errors='ignore')
    
    # Select top N (abs value)
    corr_abs = corr.abs().sort_values(ascending=False).head(top_n)
    top_features = corr.loc[corr_abs.index]
    # Sort for better plotting
    top_features = top_features.sort_values(ascending=True)

    plt.figure(figsize=(10, 8))
    # Use matplotlib barh
    # Color bars based on sign?
    colors = ['#d62728' if x < 0 else '#1f77b4' for x in top_features.values]
    
    plt.barh(top_features.index, top_features.values, color=colors)
    plt.title(f"Top {top_n} Feature Correlations with {target_col}")
    plt.xlabel("Correlation Coefficient")
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path)
        print(f"Saved feature correlation plot to {output_path}")
    else:
        plt.show()
    plt.close()

def plot_hmm_states(df, price_col, state_col, subset_n=500, output_path=None):
    """
    Plots price series (or other metric) color-coded by HMM state.
    Optimized for larger datasets using scatter/line segments.
    """
    if price_col not in df.columns or state_col not in df.columns:
        print(f"Columns {price_col} or {state_col} not found.")
        return

    # Use a subset of data for clear visualization
    plot_df = df.tail(subset_n).copy().reset_index(drop=True)
    
    unique_states = sorted(plot_df[state_col].unique())
    prop_cycle = plt.rcParams['axes.prop_cycle']
    colors = prop_cycle.by_key()['color']
    state_colors = {s: colors[i % len(colors)] for i, s in enumerate(unique_states)}
    
    plt.figure(figsize=(14, 7))
    
    x_vals = plot_df.index
    y_vals = plot_df[price_col]
    states = plot_df[state_col]
    
    # Plot the price line faintly
    plt.plot(x_vals, y_vals, color='gray', alpha=0.5, linewidth=1, label='Price')
    
    # For large N, bar charts are slow. Use scatter or coloring segments.
    # Scatter is reasonably fast for 20k points.
    if subset_n > 2000:
        # Use scatter for large datasets
        for state in unique_states:
            mask = (states == state)
            if mask.any():
                plt.scatter(x_vals[mask], y_vals[mask], s=2, color=state_colors[state], label=f"State {state}", zorder=2)
    else:
        # Use bar for smaller (original style)
        for state in unique_states:
            mask = (states == state)
            if mask.any():
                plt.bar(x_vals[mask], y_vals[mask], width=1.0, color=state_colors[state], label=f"State {state}", align='center')

    plt.title(f"Market Regime (HMM States) - Last {subset_n} Bars")
    plt.xlabel("Bar Index")
    plt.ylabel(price_col)
    plt.legend(markerscale=3 if subset_n > 2000 else 1)
    
    if not y_vals.empty:
        plt.ylim(y_vals.min() * 0.999, y_vals.max() * 1.001)
    
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path)
        print(f"Saved HMM state plot to {output_path}")
    else:
        plt.show()
    plt.close()
