import numpy as np
import pandas as pd
from hmmlearn import hmm
from sklearn.preprocessing import StandardScaler
from scipy.signal import medfilt # 用于中值滤波

class MarketRegimeHMM:
    def __init__(self, n_components=3, n_iter=100, random_state=42, covariance_type='diag'):
        self.n_components = n_components
        self.n_iter = n_iter
        self.random_state = random_state
        # 改为 'diag' 通常能减少状态闪烁，增强鲁棒性
        self.covariance_type = covariance_type 
        self.model = None
        self.scaler = StandardScaler()
        self.is_fitted = False
        self.state_map = {} # 用于存储排序后的状态标签
    def get_state_stats(self, df_with_states):
        """
        计算每个状态的统计指标，用于解释不同 Regime 的含义。
        针对 Dollar Bar 特别关注：收益率、长/短波动率、持续时间偏离度和失衡度。
        """
        # 定义需要统计的列
        # 注意：这里统计的是原始值（未标准化的值），这样才有物理意义
        cols_to_stats = ['log_ret', 'vol_long', 'vol_short', 'duration_deviation', 'imbalance']
        
        # 确保这些列都在 DataFrame 中
        available_cols = [c for c in cols_to_stats if c in df_with_states.columns]
        
        # 按 state 分组计算均值和标准差
        stats = df_with_states.groupby('state')[available_cols].agg(['mean', 'std'])
        
        # 为了方便阅读，我们可以对索引排序（虽然 train 里已经排过序了，这里双保险）
        stats = stats.sort_index()
        
        return stats
    def prepare_features(self, df, fit_scaler=False):
        data = df.copy()
        
        # 1. Log Returns
        data['log_ret'] = np.log(data['price'] / data['price'].shift(1))
        
        # 2. Volatility (加入平滑处理减少噪声)
        data['vol_short'] = data['log_ret'].rolling(window=20).std()
        data['vol_long'] = data['log_ret'].rolling(window=100).std()
        
        # 3. Bar Duration (Dollar Bar 的核心特征)
        if not np.issubdtype(data['tradetime'].dtype, np.datetime64):
            data['tradetime'] = pd.to_datetime(data['tradetime'])
        data['duration'] = (data['tradetime'] - data['tradetime'].shift(1)).dt.total_seconds()
        
        # 取 Log 处理长尾分布，使之更接近正态分布
        data['log_duration'] = np.log1p(data['duration'].fillna(data['duration'].median()))
        data['duration_deviation'] = (data['log_duration'] - data['log_duration'].rolling(100).mean()) / \
                                     (data['log_duration'].rolling(100).std() + 1e-8)
        
        # 4. Trade Imbalance
        data['imbalance'] = (data['buyer_pv'] - data['seller_pv']) / (data['pv'] + 1e-8)
        
        feature_cols = ['log_ret', 'vol_short', 'vol_long', 'duration_deviation', 'imbalance']
        data_clean = data.dropna(subset=feature_cols).copy()
        
        # --- 核心改进：标准化 ---
        if fit_scaler:
            self.scaler.fit(data_clean[feature_cols])
        
        data_clean[feature_cols] = self.scaler.transform(data_clean[feature_cols])
        
        return data_clean, feature_cols

    def train(self, df):
        data, feature_cols = self.prepare_features(df, fit_scaler=True)
        X = data[feature_cols].values
        
        self.model = hmm.GaussianHMM(
            n_components=self.n_components, 
            covariance_type=self.covariance_type, 
            n_iter=self.n_iter,
            random_state=self.random_state
        )
        
        self.model.fit(X)
        self.is_fitted = True
        
        # --- 核心改进：状态重排序 (Remap) ---
        # 预测原始状态
        raw_states = self.model.predict(X)
        # 按平均 log_ret 从低到高排序：0(熊), 1(震荡), 2(牛)
        state_means = [data.iloc[raw_states == i]['log_ret'].mean() for i in range(self.n_components)]
        self.state_map = {old: new for new, old in enumerate(np.argsort(state_means))}
        
        return self.model

    def predict(self, df, smoothing=True, kernel_size=5):
        if not self.is_fitted:
            raise ValueError("Model not fitted.")
            
        data, feature_cols = self.prepare_features(df, fit_scaler=False)
        X = data[feature_cols].values
        
        # 1. 原始预测
        raw_states = self.model.predict(X)
        
        # 2. 映射到有序状态
        ordered_states = np.array([self.state_map[s] for s in raw_states])
        
        # 3. --- 核心改进：中值滤波平滑 ---
        if smoothing:
            # kernel_size 必须是奇数，越大越平滑，滞后也越高
            ordered_states = medfilt(ordered_states, kernel_size=kernel_size)
            
        data['state'] = ordered_states
        return data

