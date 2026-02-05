import pandas as pd
import os
import sys
from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report, confusion_matrix

# Add local directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config

def load_data(data_dir):
    train_path = os.path.join(data_dir, "train.parquet")
    val_path = os.path.join(data_dir, "val.parquet")
    
    if not os.path.exists(train_path) or not os.path.exists(val_path):
        raise FileNotFoundError(f"Data not found in {data_dir}. Please run run_etl.py first.")
        
    print(f"Loading train from {train_path}...")
    train = pd.read_parquet(train_path)
    
    print(f"Loading val from {val_path}...")
    val = pd.read_parquet(val_path)
    
    return train, val

def train_binary_model(X_train, y_train, X_val, y_val, target_class, class_name):
    """Train a binary classifier for a specific class vs Rest."""
    print(f"\n=== Training Model for Class {target_class} ({class_name}) ===")
    
    # Create Binary Targets
    y_train_bin = (y_train == target_class).astype(int)
    y_val_bin = (y_val == target_class).astype(int)
    
    # Check counts
    pos_count = y_train_bin.sum()
    print(f"Positive Samples in Train: {pos_count} / {len(y_train)} ({pos_count/len(y_train):.2%})")
    
    # Initialize Model with Balancing
    # class_weight='balanced' automatically adjusts weights inversely proportional to frequencies
    model = LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.03,
        num_leaves=20,               # Reduced from 31 to prevent overfitting on small positive class
        max_depth=5,                 # Reduced depth
        min_split_gain=0.01,         # Stop splitting if gain is small (Fixes "No further splits" warning cause)
        min_child_samples=50,        # Ensure robust leaves
        objective='binary',
        class_weight='balanced',
        random_state=42,
        importance_type='gain',
        n_jobs=-1,
        verbosity=-1                 # Suppress warnings
    )
    
    # Train
    from lightgbm import early_stopping, log_evaluation
    
    model.fit(
        X_train, y_train_bin,
        eval_set=[(X_val, y_val_bin)],
        eval_metric='auc',
        callbacks=[
            early_stopping(stopping_rounds=50),
            log_evaluation(period=100)
        ]
    )
    
    # Evaluate
    print(f"\n--- Evaluation for {class_name} Model ---")
    preds_proba = model.predict_proba(X_val)[:, 1]
    preds_bin = model.predict(X_val)
    
    print(classification_report(y_val_bin, preds_bin, target_names=['Rest', class_name]))
    print("Confusion Matrix:")
    print(confusion_matrix(y_val_bin, preds_bin))
    
    # Feature Importance
    print(f"\n--- Feature Importance ({class_name}) ---")
    imp = pd.Series(model.feature_importances_, index=config.FEATURE_COLS).sort_values(ascending=False).head(10)
    print(imp)
    
    return model

def train():
    # Load Data
    train_df, val_df = load_data(config.PROCESSED_DATA_DIR)
    
    X_train = train_df[config.FEATURE_COLS]
    y_train = train_df['label']
    
    X_val = val_df[config.FEATURE_COLS]
    y_val = val_df['label']
    
    print(f"Training shapes: X_train={X_train.shape}, X_val={X_val.shape}")
    
    # Define Class Names for Reporting
    class_names = {
        1: "STRONG_SELL (Informed)",
        2: "WEAK_SELL (Noise)",
        3: "WEAK_BUY (Noise)",
        4: "STRONG_BUY (Informed)"
    }
    
    # Detect available labels
    unique_labels = sorted(y_train.unique())
    print(f"Found unique labels in training data: {unique_labels}")
    
    # Train Binary Model for each non-neutral class
    models = {}
    for label in unique_labels:
        if label == 0:
            continue # Skip Neutral
            
        name = class_names.get(label, f"CLASS_{label}")
        print(f"\n>>> Starting Training for {name} <<<")
        model = train_binary_model(X_train, y_train, X_val, y_val, target_class=label, class_name=name)
        models[label] = model

if __name__ == "__main__":
    train()
