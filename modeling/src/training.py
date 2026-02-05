import pandas as pd
import numpy as np
import os
import lightgbm as lgb
from sklearn.metrics import classification_report, confusion_matrix
from typing import Dict, Any

# Local
import config
from .utils import setup_logger, timer

class LOBTrainer:
    """
    Trainer for LOB Models.
    Supports One-vs-Rest Binary classification for multiple regimes.
    """
    
    CLASS_NAMES = {
        1: "STRONG_SELL (Informed)",
        2: "WEAK_SELL (Noise)",
        3: "WEAK_BUY (Noise)",
        4: "STRONG_BUY (Informed)"
    }
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.logger = setup_logger("LOBTrainer")
        self.models = {}

    def load_data(self):
        train_path = os.path.join(self.data_dir, "train.parquet")
        val_path = os.path.join(self.data_dir, "val.parquet")
        
        if not os.path.exists(train_path):
            raise FileNotFoundError(f"Data not found: {train_path}")
            
        self.logger.info("Loading datasets...")
        self.train_df = pd.read_parquet(train_path)
        self.val_df = pd.read_parquet(val_path)
        
        self.X_train = self.train_df[config.FEATURE_COLS]
        self.y_train = self.train_df['label']
        self.X_val = self.val_df[config.FEATURE_COLS]
        self.y_val = self.val_df['label']
        
        self.logger.info(f"Loaded: Train={self.X_train.shape}, Val={self.X_val.shape}")

    def train_binary_model(self, target_class: int, class_name: str) -> lgb.LGBMClassifier:
        """Train a single binary classifier."""
        self.logger.info(f"\n>>> Training Model for Class {target_class}: {class_name} <<<")
        
        # Binary Targets
        y_train_bin = (self.y_train == target_class).astype(int)
        y_val_bin = (self.y_val == target_class).astype(int)
        
        pos_count = y_train_bin.sum()
        self.logger.info(f"Positive Samples: {pos_count} / {len(self.y_train)} ({pos_count/len(self.y_train):.2%})")
        
        model = lgb.LGBMClassifier(
            n_estimators=1000,
            learning_rate=0.03,
            num_leaves=20,
            max_depth=5,
            min_split_gain=0.01,
            min_child_samples=50,
            objective='binary',
            class_weight='balanced',
            random_state=42,
            importance_type='gain',
            n_jobs=-1,
            verbosity=-1
        )
        
        callbacks = [
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(period=100)
        ]
        
        model.fit(
            self.X_train, y_train_bin,
            eval_set=[(self.X_val, y_val_bin)],
            eval_metric='auc',
            callbacks=callbacks
        )
        
        # Evaluate
        self._evaluate_model(model, self.X_val, y_val_bin, class_name)
        return model

    def _evaluate_model(self, model, X, y_true, class_name):
        preds = model.predict(X)
        report = classification_report(y_true, preds, target_names=['Rest', class_name])
        cm = confusion_matrix(y_true, preds)
        
        self.logger.info(f"\n--- Evaluation ({class_name}) ---\n{report}")
        self.logger.info(f"Confusion Matrix:\n{cm}")
        
        # Feature Imp
        imp = pd.Series(model.feature_importances_, index=config.FEATURE_COLS).sort_values(ascending=False).head(10)
        self.logger.info(f"\nTop Features:\n{imp.to_string()}")

    def run(self):
        """Detect classes and train all models."""
        self.load_data()
        
        unique_labels = sorted(self.y_train.unique())
        self.logger.info(f"Found labels: {unique_labels}")
        
        for label in unique_labels:
            if label == 0: continue # Skip Neutral
            
            name = self.CLASS_NAMES.get(label, f"CLASS_{label}")
            model = self.train_binary_model(label, name)
            self.models[label] = model
