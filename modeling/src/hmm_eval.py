
import numpy as np
import pandas as pd
from scipy.spatial.distance import mahalanobis
from scipy.stats import entropy
from sklearn.metrics import mutual_info_score
from sklearn.feature_selection import mutual_info_regression

class HMMEvaluator:
    def __init__(self, hmm_model, data, feature_cols):
        """
        Args:
            hmm_model: Trained MarketRegimeHMM instance (or internal GaussianHMM)
            data: DataFrame containing features and state labels (if available)
            feature_cols: List of feature column names used for training
        """
        self.hmm_wrapper = hmm_model
        self.model = hmm_model.model if hasattr(hmm_model, 'model') else hmm_model
        self.data = data
        self.feature_cols = feature_cols
        
        # Ensure we have state predictions
        if 'state' not in self.data.columns:
            raise ValueError("Data must have a 'state' column for evaluation.")
            
    def evaluate_all(self):
        """Run all evaluation metrics"""
        results = {}
        
        print("   [Evaluator] Calculating Separation Metrics...")
        results['separation'] = self.separation_metrics()
        
        print("   [Evaluator] Calculating Stability Metrics...")
        results['stability'] = self.stability_metrics()
        
        print("   [Evaluator] Calculating Predictive Power...")
        results['predictive'] = self.predictive_power_metrics()
        
        return results

    # ==========================
    # 1. Separation Metrics
    # ==========================
    def separation_metrics(self):
        metrics = {}
        
        # 1.1 Log Likelihood per Sample
        X = self.data[self.feature_cols].values
        # Note: self.model.score returns total log likelihood
        log_likelihood = self.model.score(X)
        metrics['log_likelihood_per_sample'] = log_likelihood / len(X)
        
        # 1.2 Mahalanobis Distance (Pairwise between states)
        # Formula: D_M = sqrt( (u_i - u_j)^T * S^-1 * (u_i - u_j) )
        # Using pooled covariance (average of the two states) for symmetry
        n_components = self.model.n_components
        means = self.model.means_
        covars = self.model.covars_
        
        dist_matrix = np.zeros((n_components, n_components))
        
        for i in range(n_components):
            for j in range(i + 1, n_components):
                mu_i = means[i]
                mu_j = means[j]
                delta = mu_i - mu_j
                
                # Robust Mahalanobis Calculation
                cov_type = getattr(self.model, 'covariance_type', 'diag')
                
                if cov_type == 'diag':
                    # covars_ is (n_components, n_features)
                    # We average the variances
                    avg_var = 0.5 * (covars[i] + covars[j])
                    # Distance^2 = sum( delta^2 / variance )
                    # Add small epsilon to avoid division by zero
                    dist_sq = np.sum((delta ** 2) / (avg_var + 1e-8))
                    
                else:
                    # covars_ is (n_components, n_features, n_features)
                    # We average the covariance matrices
                    avg_cov = 0.5 * (covars[i] + covars[j])
                    try:
                        inv_cov = np.linalg.inv(avg_cov + np.eye(len(delta)) * 1e-8)
                        dist_sq = mult = np.dot(np.dot(delta, inv_cov), delta.T)
                    except np.linalg.LinAlgError:
                        # Fallback to identity if singular
                        dist_sq = np.dot(delta, delta.T)

                # Ensure it's scalar
                if isinstance(dist_sq, np.ndarray):
                    dist_sq = dist_sq.item()
                    
                dist = np.sqrt(max(0, dist_sq))
                
                dist_matrix[i, j] = dist
                dist_matrix[j, i] = dist # Symmetric
                
        metrics['mahalanobis_distance_mean'] = np.mean(dist_matrix[dist_matrix > 0])
        metrics['mahalanobis_distance_matrix'] = dist_matrix
        
        return metrics

    # ==========================
    # 2. Stability Metrics
    # ==========================
    def stability_metrics(self):
        metrics = {}
        states = self.data['state'].values
        
        # 2.1 Mean Regime Duration
        # Run-length encoding
        # Find indices where value changes
        # e.g., [0, 0, 1, 1, 1, 0] -> diff gives [0, 1, 0, 0, -1]
        # simpler: loop or itertools.groupby
        import itertools
        durations = [sum(1 for _ in group) for _, group in itertools.groupby(states)]
        metrics['mean_regime_duration'] = np.mean(durations)
        metrics['min_regime_duration'] = np.min(durations)
        metrics['max_regime_duration'] = np.max(durations)
        
        # 2.2 Transition Matrix Entropy
        # H = - sum(p_ij * log(p_ij))
        transmat = self.model.transmat_
        # Row entropy (uncertainty of where to go next from state i)
        # We can average the row entropies weighted by stationary distribution, 
        # or just take the global entropy as requested by the formula sum sum ...
        
        # Formula given: sum_i sum_j p_ij * log(p_ij). Actually H is usually -sum..
        # Using scipy.stats.entropy for each row (base e)
        # Note: 0 * log(0) is 0. entropy function handles this.
        
        # Calculating global matrix entropy as requested: H = - sum sum p_ij log p_ij
        # This is strictly the entropy of the transition distribution conditioned on starting state,
        # summed over all transitions.
        # However, usually we care about the entropy rate or the average uncertainty.
        # The user formula: H = - sum_i sum_j p_ij log(p_ij)
        
        p = transmat.flatten()
        p = p[p > 0] # Avoid log(0)
        matrix_entropy = -np.sum(p * np.log(p))
        metrics['transition_matrix_entropy'] = matrix_entropy
        
        # Also useful: Average determinism (avg diagonal)
        metrics['avg_diagonal_prob'] = np.mean(np.diag(transmat))
        
        return metrics

    # ==========================
    # 3. Predictive Power
    # ==========================
    def predictive_power_metrics(self, horizon=5):
        metrics = {}
        
        # Target: Future N bar Log Returns
        # We need to calculate it if not present
        if 'log_ret' in self.data.columns:
            # Shift log_ret backwards to get future returns
            # log_ret is usually close-to-close. 
            # Rolling sum of next N log returns = log(P_{t+N} / P_t)
            future_ret = self.data['log_ret'].rolling(window=horizon).sum().shift(-horizon)
            
            # Align data
            valid_mask = ~np.isnan(future_ret)
            states_valid = self.data['state'][valid_mask].values.reshape(-1, 1) # reshape for mutual_info_regression
            future_ret_valid = future_ret[valid_mask].values
            
            # 3.1 Mutual Information
            # Continuous target, discrete feature.
            # sklearn mutual_info_regression works for continuous Y and discrete X if labeled.
            # discrete_features=[0] means column 0 of X is discrete.
            mi_score = mutual_info_regression(states_valid, future_ret_valid, discrete_features=[0], random_state=42)[0]
            metrics['mutual_information'] = mi_score
            
            # 3.2 Conditional Variance Reduction
            # Global Variance
            global_var = np.var(future_ret_valid)
            
            # Weighted Conditional Variance
            # sum P(s) * Var(R|s)
            state_counts = pd.Series(states_valid.flatten()).value_counts(normalize=True)
            weighted_cond_var = 0
            
            unique_states = np.unique(states_valid)
            for s in unique_states:
                state_ret = future_ret_valid[states_valid.flatten() == s]
                s_var = np.var(state_ret)
                p_s = state_counts.get(s, 0)
                weighted_cond_var += p_s * s_var
                
            variance_reduction = 1 - (weighted_cond_var / (global_var + 1e-10))
            metrics['variance_reduction'] = variance_reduction
            
        else:
            print("Warning: 'log_ret' column missing, skipping predictive metrics.")
            metrics['mutual_information'] = np.nan
            metrics['variance_reduction'] = np.nan
            
        return metrics

    # ==========================
    # 4. Feature Importance
    # ==========================
    def feature_importance(self):
        """
        Calculate feature importance using two methods:
        1. Permutation Importance: Decrease in Log-Likelihood when feature is shuffled.
        2. State Signatures: Z-scores showing how different a feature is in a state compared to global mean.
        """
        results = {}
        
        # 4.1 Permutation Importance (Global)
        # -----------------------------------
        X = self.data[self.feature_cols].values
        baseline_score = self.model.score(X) # Total Log Likelihood
        
        perm_importance = {}
        
        for i, col in enumerate(self.feature_cols):
            X_perm = X.copy()
            np.random.shuffle(X_perm[:, i])
            
            perm_score = self.model.score(X_perm)
            
            # Importance = Drop in Likelihood (Positive is good)
            # Log likelihood is negative, so if it drops from -100 to -150, importance is 50.
            importance = baseline_score - perm_score
            perm_importance[col] = max(0, importance) # features shouldn't help by being random
            
        # Normalize to sum to 1 (or 100%)
        total_imp = sum(perm_importance.values())
        if total_imp > 0:
            perm_importance = {k: v / total_imp for k, v in perm_importance.items()}
            
        results['permutation_importance'] = perm_importance
        
        # 4.2 State Signatures (Z-Scores)
        # -------------------------------
        # Which features make State K unique?
        # Z = (Mean_State - Mean_Global) / Std_Global
        
        global_mean = self.data[self.feature_cols].mean()
        global_std = self.data[self.feature_cols].std()
        
        state_z_scores = {}
        unique_states = sorted(self.data['state'].unique())
        
        for s in unique_states:
            state_data = self.data[self.data['state'] == s][self.feature_cols]
            state_mean = state_data.mean()
            
            # Z-score for this state
            z_scores = (state_mean - global_mean) / (global_std + 1e-8)
            state_z_scores[s] = z_scores.to_dict()
            
        results['state_z_scores'] = state_z_scores
        
        return results

