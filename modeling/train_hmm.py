import pandas as pd
import numpy as np
import os
import joblib
import config
from src.hmm import MarketRegimeHMM

def interpret_regimes(stats):
    """
    根据统计数据自动给状态打标签 (Heuristic Logic)
    """
    labels = {}
    # 找到收益率最高和最低的状态
    bull_state = stats['log_ret']['mean'].idxmax()
    bear_state = stats['log_ret']['mean'].idxmin()
    
    # 在剩下的状态中寻找特征，或者细分
    for state in stats.index:
        mean_ret = stats.loc[state, ('log_ret', 'mean')]
        std_vol = stats.loc[state, ('vol_long', 'mean')]
        dur_dev = stats.loc[state, ('duration_deviation', 'mean')]
        
        # 基础标签
        if state == bull_state:
            label = "Bull"
        elif state == bear_state:
            label = "Bear"
        else:
            label = "Sideways/Neutral"
            
        # 附加特征描述
        # 如果波动率极高，标记为 High Vol
        if std_vol > stats[('vol_long', 'mean')].quantile(0.7):
            label = f"High-Vol {label}"
        # 如果 Duration Deviation 很负（值很小），说明 Bar 产生极快，交易极其活跃
        if dur_dev < 0:
            label = f"Active {label}"
            
        labels[state] = label
    return labels

def run_hmm_training():
    print("🚀 Loading Dollar Bar training data...")
    train_path = os.path.join(config.PROCESSED_DATA_DIR, "train.parquet")
    
    if not os.path.exists(train_path):
        print(f"❌ Error: {train_path} not found. Ensure ETL process is complete.")
        return

    df = pd.read_parquet(train_path)
    print(f"📊 Loaded {len(df)} bars.")

    # 1. 初始化模型
    # n_states=3 通常是 Dollar Bar 下最稳健的选择
    n_states = 3
    print(f"🤖 Initializing HMM with {n_states} states (diag covariance)...")
    hmm_model = MarketRegimeHMM(n_components=n_states, n_iter=100)

    # 2. 训练模型 (内部已包含 StandardScaler 和 State Remapping)
    print("⚙️  Fitting model and optimizing state transitions...")
    hmm_model.train(df)

    # 3. 预测并平滑状态
    # 使用 smoothing=True 解决你担心的“类别不连续”问题
    print("📈 Predicting and smoothing market regimes...")
    df_labeled = hmm_model.predict(df, smoothing=True, kernel_size=5)

    # 4. 分析状态并自动解释
    print("\n🧐 State Statistical Analysis:")
    stats = hmm_model.get_state_stats(df_labeled)
    print(stats)
    
    regime_labels = interpret_regimes(stats)
    print("\n🏷️  Identified Regimes:")
    for state, name in regime_labels.items():
        print(f"State {state}: {name}")
    
    # 5. 保存模型及元数据
    # 将标签映射一起保存，方便回测调用
    model_data = {
        'model': hmm_model,
        'regime_labels': regime_labels,
        'feature_stats': stats
    }
    
    model_path = os.path.join(config.PROCESSED_DATA_DIR, "hmm_model.pkl")
    print(f"\n💾 Saving comprehensive model packet to {model_path}...")
    joblib.dump(model_data, model_path)
    
    print("✅ Done! Regime classifier is ready for backtesting.")

if __name__ == "__main__":
    run_hmm_training()