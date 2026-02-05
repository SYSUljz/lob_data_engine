# LOB Data Modeling Pipeline

This directory contains the end-to-end pipeline for **Labeling, Feature Engineering, and Training** machine learning models on Limit Order Book (LOB) data.

## Overview

The pipeline transforms raw tick-level trade data into aggregated Volume Bars, extracts advanced features (including Signature Transforms), generates labels based on future returns, and trains LightGBM classifiers.

### Key Features
- **Volume Bar Aggregation**: Adapts to market activity by aggregating trades based on volume thresholds.
- **Advanced Feature Engineering**:
    - **Micro-structure Features**: Imbalance (Z-score), CVD, VWAP Distortion.
    - **Fractional Differentiation**: Preserves memory while achieving stationarity (`Price_FFD`).
    - **Signature Transform (Intra-Bar)**: Captures the shape of the Price-CVD path *within* each bar using `iisignature`.
    - **Rolling Signature Transform (Inter-Bar)**: Captures temporal dynamics across a sliding window of bars.
- **Clustering-based Labeling**: Uses K-Means to identify distinct market regimes (Strong/Weak Buy/Sell) dynamically.
- **HMM Market Regime Classification**: Uses Hidden Markov Models to classify market states (e.g., Low/High Volatility) based on Returns, Volatility, and Volume Imbalance.

## Directory Structure

```text
modeling/
├── config.py           # Central configuration (Paths, Hyperparams, Features)
├── run_etl.py          # Entry point for the ETL pipeline
├── train_model.py      # Entry point for Model Training
├── main.py             # Orchestrator (Optional)
└── src/
    ├── processing.py       # Core ETL logic (Aggregation, Signature Calc)
    ├── features.py         # Feature engineering (FFD, Lags, Rolling Sig)
    ├── labeling.py         # K-Means clustering for labeling
    ├── training.py         # LightGBM training wrapper
    ├── hmm.py              # HMM Logic and Feature Engineering
    └── utils.py            # Helpers
```

## Workflow

### 1. ETL Pipeline (`run_etl.py`)
Processes raw Parquet data into training-ready datasets.

**Usage:**
```bash
# Run on full dataset (all row groups)
python notebook/modeling/run_etl.py --groups 100

# Run in test mode (small subset)
python notebook/modeling/run_etl.py --test
```

**What it does:**
1.  **Load**: Reads raw trade ticks from `data/`.
2.  **Aggregate**: Groups trades into Volume Bars (threshold defined in `config.py`).
3.  **Featurize**:
    *   **Intra-Bar Signature**: Computes the signature of the `[log_price, cum_cvd]` path (Min-Max scaled) for every bar.
    *   **Rolling Signature**: Computes the signature of `[Price_FFD, CVD, Time]` over a sliding window.
    *   Calculates standard features (Volatility, Lags, etc.).
4.  **Label**: Generates labels for future horizons using Clustering.
5.  **Save**: Outputs `train.parquet` and `val.parquet` to `data/`.

### 2. Model Training (`train_model.py`)
Trains LightGBM models for each labeled class (One-vs-Rest).

**Usage:**
```bash
python notebook/modeling/train_model.py
```

**What it does:**
1.  Loads processed `train`/`val` data.
2.  Identifies all target classes (e.g., Strong Buy, Weak Sell).
3.  Trains a separate Binary Classifier (LightGBM) for each non-neutral class.
4.  Logs performance metrics (AUC, Precision, Recall, Feature Importance).

### 3. HMM Regime Classification (`train_hmm.py`)
Trains a Hidden Markov Model to decode market regimes.

**Usage:**
```bash
python modeling/train_hmm.py
```

**What it does:**
1.  Loads `train.parquet`.
2.  Extracts HMM-specific features:
    *   **Log Returns**: Direction.
    *   **Realized Volatility**: Risk (Multi-timeframe).
    *   **Volume Deviation**: Activity level.
    *   **Trade Imbalance**: Buy/Sell pressure.
3.  Trains a Gaussian HMM (default 3 states).
4.  Outputs state interpretation (Mean/Std of features per state) and saves model to `data/hmm_model.pkl`.

## Configuration
Adjust parameters in `config.py`:
- `VOLUME_BAR_THRESHOLD`: Size of volume bars.
- `SIG_LEVEL`: Order of the signature transform.
- `SIG_ROLLING_WINDOW`: Window size for inter-bar signatures.
- `HORIZON`: Prediction horizon for labeling.

## Dependencies
- `pandas`, `numpy`
- `iisignature` (Vital for signature calculation)
- `lightgbm`
- `hmmlearn` (For HMM Regime Classification)
- `scikit-learn`
