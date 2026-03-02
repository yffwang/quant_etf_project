# -*- coding: utf-8 -*-
"""
动量因子分析模块
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class MomentumAnalyzer:
    """动量因子分析器"""
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化分析器
        
        Args:
            df: 包含OHLCV数据的DataFrame，需按日期升序排列
        """
        self.df = df.copy()
        self.df = self.df.sort_values("date")
    
    def calculate_returns(self, periods: List[int] = None) -> pd.DataFrame:
        """
        计算不同周期的收益率
        
        Args:
            periods: 周期列表（天数）
        """
        if periods is None:
            periods = [1, 5, 10, 20, 60, 120]
        
        df = self.df
        
        for period in periods:
            df[f"return_{period}d"] = df["close"].pct_change(period)
        
        return df
    
    def calculate_momentum(
        self, 
        short: int = 20, 
        medium: int = 60, 
        long: int = 120
    ) -> pd.DataFrame:
        """
        计算动量因子
        
        动量效应: 过去表现好的资产未来表现也倾向好
        反转效应: 过去表现差的资产未来可能反弹
        
        Args:
            short: 短期动量周期
            medium: 中期动量周期
            long: 长期动量周期
        """
        df = self.df
        
        # 简单动量 (收益率)
        df["momentum_short"] = df["close"].pct_change(short)
        df["momentum_medium"] = df["close"].pct_change(medium)
        df["momentum_long"] = df["close"].pct_change(long)
        
        # 相对动量 (相对于基准)
        # 可以传入benchmark参数计算相对强弱
        
        # 累积动量 (带权重的多周期动量)
        df["momentum_composite"] = (
            df["momentum_short"] * 0.5 + 
            df["momentum_medium"] * 0.3 + 
            df["momentum_long"] * 0.2
        )
        
        # 动量加速 (短期动量相对于长期动量)
        df["momentum_acceleration"] = df["momentum_short"] - df["momentum_medium"]
        
        return df
    
    def calculate_volatility(self, periods: List[int] = None) -> pd.DataFrame:
        """
        计算波动率因子
        
        低波动率效应: 低波动股票长期表现往往好于高波动股票
        """
        if periods is None:
            periods = [20, 60]
        
        df = self.df
        
        for period in periods:
            # 日收益率标准差年化
            df[f"volatility_{period}d"] = df["close"].pct_change().rolling(window=period).std() * np.sqrt(252)
        
        return df
    
    def calculate_sharpe_ratio(self, period: int = 60) -> pd.DataFrame:
        """
        计算夏普比率因子
        """
        df = self.df
        
        # 日收益率
        daily_returns = df["close"].pct_change()
        
        # 滚动夏普比率
        mean_return = daily_returns.rolling(window=period).mean()
        std_return = daily_returns.rolling(window=period).std()

        sharpe = (mean_return / std_return.where(std_return != 0, np.nan)) * np.sqrt(252)
        df["sharpe_ratio"] = sharpe.fillna(0)
        
        return df
    
    def calculate_max_drawdown(self, period: int = 60) -> pd.DataFrame:
        """
        计算最大回撤
        """
        df = self.df
        
        # 滚动最高价
        df["rolling_high"] = df["close"].rolling(window=period, min_periods=1).max()
        
        # 回撤
        df["drawdown"] = (df["close"] - df["rolling_high"]) / df["rolling_high"]
        
        # 最大回撤
        df["max_drawdown"] = df["drawdown"].rolling(window=period).min()
        
        return df
    
    def calculate_volume_momentum(self, period: int = 20) -> pd.DataFrame:
        """
        计算成交量动量因子
        
        放量上涨往往是强势信号
        """
        df = self.df
        
        # 成交量变化率
        df["volume_change"] = df["volume"].pct_change(period)
        
        # 成交额变化率
        df["amount_change"] = df["amount"].pct_change(period)
        
        # 量价相关性
        df["volume_price_corr"] = df["close"].rolling(window=period).corr(df["volume"])
        
        return df
    
    def calculate_all(self) -> pd.DataFrame:
        """
        计算所有动量因子
        """
        df = self.df
        
        # 收益率
        df = self.calculate_returns([1, 5, 10, 20, 60])
        
        # 动量
        df = self.calculate_momentum(short=20, medium=60, long=120)
        
        # 波动率
        df = self.calculate_volatility([20, 60])
        
        # 夏普比率
        df = self.calculate_sharpe_ratio()
        
        # 最大回撤
        df = self.calculate_max_drawdown()
        
        # 成交量动量
        df = self.calculate_volume_momentum()
        
        return df
    
    def get_latest_momentum(self) -> Dict:
        """
        获取最新的动量因子值
        """
        df = self.calculate_all()
        
        if df.empty:
            return {}
        
        latest = df.iloc[-1]
        
        return {
            "return_1d": latest.get("return_1d"),
            "return_5d": latest.get("return_5d"),
            "return_20d": latest.get("return_20d"),
            "return_60d": latest.get("return_60d"),
            "momentum_short": latest.get("momentum_short"),
            "momentum_medium": latest.get("momentum_medium"),
            "momentum_long": latest.get("momentum_long"),
            "momentum_composite": latest.get("momentum_composite"),
            "momentum_acceleration": latest.get("momentum_acceleration"),
            "volatility_20d": latest.get("volatility_20d"),
            "volatility_60d": latest.get("volatility_60d"),
            "sharpe_ratio": latest.get("sharpe_ratio"),
            "max_drawdown": latest.get("max_drawdown"),
            "volume_change": latest.get("volume_change"),
        }


def calculate_momentum_score(momentum: Dict) -> Tuple[float, List[str]]:
    """
    计算动量因子综合评分 (-1 到 1)
    
    正分: 强势动量
    负分: 弱势动量
    """
    score = 0.0
    signals = []
    
    # 短期动量评分 (-0.25 到 0.25)
    short = momentum.get("momentum_short", 0)
    if short > 0.1:
        short_score = 0.25
        signals.append(f"短期强劲上涨({short*100:.1f}%)")
    elif short > 0.05:
        short_score = 0.15
        signals.append(f"短期上涨({short*100:.1f}%)")
    elif short < -0.1:
        short_score = -0.25
        signals.append(f"短期下跌({short*100:.1f}%)")
    elif short < -0.05:
        short_score = -0.15
        signals.append(f"短期回调({short*100:.1f}%)")
    else:
        short_score = 0
    score += short_score
    
    # 中期动量评分 (-0.25 到 0.25)
    medium = momentum.get("momentum_medium", 0)
    if medium > 0.15:
        medium_score = 0.25
        signals.append(f"中期强势({medium*100:.1f}%)")
    elif medium > 0:
        medium_score = 0.1
    elif medium < -0.15:
        medium_score = -0.25
        signals.append(f"中期弱势({medium*100:.1f}%)")
    else:
        medium_score = -0.1
    score += medium_score
    
    # 动量一致性评分 (-0.25 到 0.25)
    acceleration = momentum.get("momentum_acceleration", 0)
    if acceleration > 0.05:
        accel_score = 0.25
        signals.append("动量加速")
    elif acceleration < -0.05:
        accel_score = -0.25
        signals.append("动量减速")
    else:
        accel_score = 0
    score += accel_score
    
    # 波动率调整 (-0.25 到 0.25)
    vol = momentum.get("volatility_20d", 0)
    if vol < 0.15:
        vol_score = 0.15  # 低波动加分
        signals.append(f"低波动({vol*100:.1f}%)")
    elif vol > 0.4:
        vol_score = -0.15  # 高波动减分
        signals.append(f"高波动({vol*100:.1f}%)")
    else:
        vol_score = 0
    score += vol_score
    
    # 夏普比率 (0 到 0.2)
    sharpe = momentum.get("sharpe_ratio", 0)
    if sharpe > 1:
        sharpe_score = 0.2
        signals.append(f"高夏普({sharpe:.2f})")
    elif sharpe > 0.5:
        sharpe_score = 0.1
    elif sharpe < 0:
        sharpe_score = -0.1
    else:
        sharpe_score = 0
    score += sharpe_score
    
    return score, signals


if __name__ == "__main__":
    import sys
    sys.path.append("..")
    
    from data.fetcher import ETFFetcher
    
    fetcher = ETFFetcher()
    df = fetcher.get_etf_historical("159990")
    
    if not df.empty:
        analyzer = MomentumAnalyzer(df)
        momentum = analyzer.get_latest_momentum()
        print("动量因子:", momentum)
        
        score, signals = calculate_momentum_score(momentum)
        print(f"动量评分: {score:.2f}")
        print("信号:", signals)
