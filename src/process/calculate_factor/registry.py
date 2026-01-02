import functools
import pandas as pd

class FactorRegistry:
    def __init__(self):
        # 存储注册的特征：(函数对象, 聚合规则)
        self._factors = []
        # Store group-wise factors: list of functions
        self._group_factors = []

    def register(self, agg_rule='last'):
        """
        装饰器：用于注册特征计算函数及其聚合规则。
        
        参数:
        agg_rule: 字符串 (如 'mean', 'sum') 或 字典 (如 {'col_name': 'mean'})
                  定义该特征在 resample 时的聚合方式。
        """
        def decorator(func):
            # 将函数和配置添加到注册表
            self._factors.append({
                'func': func,
                'agg_rule': agg_rule
            })
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def register_group(self):
        """
        Decorator to register a function that calculates factors on a DataFrame group.
        The function should take a DataFrame (the group) and return a Series (features).
        """
        def decorator(func):
            self._group_factors.append(func)
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def apply_and_get_agg_rules(self, df):
        """
        执行所有注册的特征计算，并返回对应的聚合规则字典。
        
        参数:
        df: 原始 DataFrame (会被原地修改或返回新的)
        
        返回:
        agg_rules: 适用于 resample().agg(agg_rules) 的字典
        """
        agg_rules = {}
        
        for item in self._factors:
            func = item['func']
            rule_config = item['agg_rule']
            
            # 1. 计算特征
            result = func(df)
            
            # 2. 处理返回结果 (Series 或 DataFrame) 并生成聚合规则
            if isinstance(result, pd.Series):
                # 如果返回 Series，列名默认为函数名或 Series.name
                col_name = result.name if result.name else func.__name__
                df[col_name] = result
                
                # 设置聚合规则
                rule = rule_config if isinstance(rule_config, str) else 'last'
                agg_rules[col_name] = rule
                
            elif isinstance(result, pd.DataFrame):
                # 如果返回 DataFrame，合并所有列
                df[result.columns] = result
                
                for col in result.columns:
                    # 解析该列的聚合规则
                    if isinstance(rule_config, dict):
                        rule = rule_config.get(col, 'last')
                    elif isinstance(rule_config, str):
                        rule = rule_config
                    else:
                        rule = 'last'
                    agg_rules[col] = rule
                    
        return agg_rules

    def get_group_apply_func(self):
        """
        Returns a single function that executes all registered group factors.
        Suitable for groupby().apply() or resample().apply().
        """
        if not self._group_factors:
            return None
            
        def wrapper(group):
            results = {}
            for func in self._group_factors:
                res = func(group)
                if isinstance(res, pd.Series):
                    results.update(res.to_dict())
                elif isinstance(res, dict):
                    results.update(res)
                else:
                    # Scalar or single value with function name
                    results[func.__name__] = res
            return pd.Series(results)
        return wrapper

# 全局实例
lob_registry = FactorRegistry()
trade_registry = FactorRegistry()
