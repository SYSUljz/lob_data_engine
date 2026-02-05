
import os
import pandas as pd
import joblib
import config
from src import processing, features, visualization

def main():
    # 1. Load Data
    train_path = os.path.join(config.PROCESSED_DATA_DIR, "train.parquet")
    if not os.path.exists(train_path):
        print(f"Data not found at {train_path}. Run pipeline first.")
        return
    
    print(f"Loading data from {train_path}...")
    df = pd.read_parquet(train_path)
    
    # 2. Visualize Feature Correlations
    # Adjust target column based on labeling.py. 
    # Assuming 'meta_label' or similar serves as the main target, or 'ret_future' for regression
    # Based on README: "Clustering-based Labeling" -> likely 'label' or 'cluster'
    # Let's check columns quickly or assume 'label' exists. 
    # If not, we fall back to 'log_ret_close' (future return) or similar.
    
    target_var = 'label'
    if target_var not in df.columns:
        print(f"Warning: '{target_var}' not in columns. Using 'log_ret_close' as proxy for correlations.")
        target_var = 'log_ret_close'
    
    output_corr = os.path.join(config.PROCESSED_DATA_DIR, "plots", "feature_correlations.png")
    visualization.plot_feature_correlations(df, target_var, top_n=20, output_path=output_corr)

    # 3. Visualize HMM States
    # We need to predict states first if they aren't in the dataframe
    # Or load the model and predict
    model_path = os.path.join(config.PROCESSED_DATA_DIR, "hmm_model.pkl")
    if os.path.exists(model_path):
        print(f"Loading HMM model from {model_path}...")
        model_data = joblib.load(model_path)
        if isinstance(model_data, dict) and 'model' in model_data:
            hmm_model = model_data['model']
            print("Loaded HMM model from dictionary packet.")
        else:
            hmm_model = model_data
            print("Loaded HMM model directly.")
        
        # Predict
        # Note: We must ensure features match what HMM was trained on.
        # train_hmm.py uses specific features inside data preparation? 
        # Let's look at train_hmm.py again or import the Class and let it handle transformation if it does.
        # The stored object is 'MarketRegimeHMM' instance from src.hmm
        
        print("Predicting states...")
        df_hmm = hmm_model.predict(df)
        
        # Plot
        output_hmm = os.path.join(config.PROCESSED_DATA_DIR, "plots", "hmm_regimes.png")
        # Assuming 'price_ffd' or 'close' (if available). 
        # 'price_ffd' is FFD stationarized price, might look weird as bars.
        # Let's try to find a raw price col, usually 'close' or 'price'.
        price_col = 'price' if 'price' in df_hmm.columns else 'close'
        if price_col not in df_hmm.columns:
             # Fallback to cumulative sum of returns or just first numeric col
             price_col = 'price_ffd' 
        
        visualization.plot_hmm_states(df_hmm, price_col, 'state', subset_n=20000, output_path=output_hmm)
    else:
        print("HMM model not found. Skipping HMM visualization.")

if __name__ == "__main__":
    main()
