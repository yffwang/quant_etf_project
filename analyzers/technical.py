# -*- coding: utf-8 -*-
"""
技术指标分析模块
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class TechnicalAnalyzer:
    """技术指标分析器"""
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化分析器
        
        Args:
            df: 包含OHLCV数据的DataFrame
        """
        self.df = df.copy()
        self.df = self.df.sort_values("date")
    
    def calculate_ma(self, periods: list = [5, 10, 20, 60]) -> pd.DataFrame:
        """
        计算移动平均线
        
        Args:
            periods: 均线周期列表
        """
        df = self.df
        
        for period in periods:
            df[f"ma{period}"] = df["close"].rolling(window=period).mean()
        
        # 计算均线方向
        df["ma5_direction"] = np.where(df["ma5"] > df["ma5"].shift(1), 1, -1)
        df["ma10_direction"] = np.where(df["ma10"] > df["ma10"].shift(1), 1, -1)
        df["ma20_direction"] = np.where(df["ma20"] > df["ma20"].shift(1), 1, -1)
        
        return df
    
    def calculate_macd(
        self, 
        fast: int = 12, 
        slow: int = 26, 
        signal: int = 9
    ) -> pd.DataFrame:
        """
        计算MACD指标
        
        Returns:
            df: 包含 macd, macd_signal, macd_hist 列
        """
        df = self.df
        
        # 计算EMA
        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        
        # DIF (MACD线)
        df["macd"] = ema_fast - ema_slow
        
        # DEA (信号线)
        df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
        
        # MACD柱状图
        df["macd_hist"] = (df["macd"] - df["macd_signal"]) * 2
        
        return df
    
    def calculate_rsi(self, period: int = 14) -> pd.DataFrame:
        """
        计算RSI相对强弱指标
        
        Args:
            period: RSI周期
        """
        df = self.df
        
        # 计算价格变化
        delta = df["close"].diff()
        
        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)
        
        # 计算平均涨跌幅
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # 计算RS和RSI
        rs = avg_gain / avg_loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # 填充NaN
        df["rsi"] = df["rsi"].fillna(50)
        
        return df
    
    def calculate_boll(self, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
        """
        计算布林带指标
        
        Args:
            period: 布林带周期
            std_dev: 标准差倍数
        """
        df = self.df
        
        # 中轨
        df["boll_middle"] = df["close"].rolling(window=period).mean()
        
        # 标准差
        std = df["close"].rolling(window=period).std()
        
        # 上轨和下轨
        df["boll_upper"] = df["boll_middle"] + std_dev * std
        df["boll_lower"] = df["boll_middle"] - std_dev * std
        
        # 位置百分比 (%B)
        boll_range = df["boll_upper"] - df["boll_lower"]
        df["boll_position"] = (df["close"] - df["boll_lower"]) / boll_range.where(boll_range != 0, 1)
        
        return df
    
    def calculate_kdj(self, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """
        计算KDJ指标
        """
        df = self.df
        
        # 计算RSV
        low_n = df["low"].rolling(window=n).min()
        high_n = df["high"].rolling(window=n).max()
        price_range = high_n - low_n
        rsv = (df["close"] - low_n) / price_range.where(price_range != 0, 1) * 100
        rsv = rsv.fillna(50)
        
        # 计算K、D、J
        df["kdj_k"] = rsv.ewm(com=m1-1, adjust=False).mean()
        df["kdj_d"] = df["kdj_k"].ewm(com=m2-1, adjust=False).mean()
        df["kdj_j"] = 3 * df["kdj_k"] - 2 * df["kdj_d"]
        
        return df
    
    def calculate_atr(self, period: int = 14) -> pd.DataFrame:
        """
        计算ATR平均真实波幅
        """
        df = self.df
        
        high_low = df["high"] - df["low"]
        high_close = np.abs(df["high"] - df["close"].shift())
        low_close = np.abs(df["low"] - df["close"].shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = true_range.rolling(window=period).mean()
        
        return df
    
    def calculate_all(self) -> pd.DataFrame:
        """
        计算所有技术指标
        """
        df = self.df
        
        # 移动平均
        df = self.calculate_ma([5, 10, 20, 60])
        
        # MACD
        df = self.calculate_macd()
        
        # RSI
        df = self.calculate_rsi()
        
        # 布林带
        df = self.calculate_boll()
        
        # KDJ
        df = self.calculate_kdj()
        
        # ATR
        df = self.calculate_atr()
        
        return df
    
    def get_latest_indicators(self) -> Dict:
        """
        获取最新的技术指标值
        """
        df = self.calculate_all()
        
        if df.empty:
            return {}
        
        latest = df.iloc[-1]
        
        return {
            "ma5": latest.get("ma5"),
            "ma10": latest.get("ma10"),
            "ma20": latest.get("ma20"),
            "ma60": latest.get("ma60"),
            "ma5_direction": latest.get("ma5_direction"),
            "macd": latest.get("macd"),
            "macd_signal": latest.get("macd_signal"),
            "macd_hist": latest.get("macd_hist"),
            "rsi": latest.get("rsi"),
            "boll_upper": latest.get("boll_upper"),
            "boll_middle": latest.get("boll_middle"),
            "boll_lower": latest.get("boll_lower"),
            "boll_position": latest.get("boll_position"),
            "kdj_k": latest.get("kdj_k"),
            "kdj_d": latest.get("kdj_d"),
            "kdj_j": latest.get("kdj_j"),
            "atr": latest.get("atr"),
            "close": latest.get("close"),
            "volume": latest.get("volume"),
        }


def calculate_technical_score(indicators: Dict) -> float:
    """
    计算技术指标综合评分 (-1 到 1)
    
    正分: 看多信号
    负分: 看空信号
    """
    score = 0.0
    signals = []
    
    # RSI评分 (-0.2 到 0.2)
    rsi = indicators.get("rsi", 50)
    if rsi < 30:
        rsi_score = 0.2
        signals.append("RSI超卖")
    elif rsi > 70:
        rsi_score = -0.2
        signals.append("RSI超买")
    else:
        rsi_score = (50 - rsi) / 100  # 偏离50的程度
    score += rsi_score
    
    # MACD评分 (-0.3 到 0.3)
    macd = indicators.get("macd", 0)
    macd_signal = indicators.get("macd_signal", 0)
    macd_hist = indicators.get("macd_hist", 0)
    
    if macd > macd_signal and macd_hist > 0:
        macd_score = 0.3
        signals.append("MACD金叉")
    elif macd < macd_signal and macd_hist < 0:
        macd_score = -0.3
        signals.append("MACD死叉")
    else:
        macd_score = 0
    score += macd_score
    
    # 均线评分 (-0.3 到 0.3)
    ma5 = indicators.get("ma5", 0)
    ma10 = indicators.get("ma10", 0)
    ma20 = indicators.get("ma20", 0)
    close = indicators.get("close", 0)
    
    if close > ma5 > ma10 > ma20:
        ma_score = 0.3
        signals.append("多头排列")
    elif close < ma5 < ma10 < ma20:
        ma_score = -0.3
        signals.append("空头排列")
    elif close > ma5:
        ma_score = 0.1
    elif close < ma5:
        ma_score = -0.1
    else:
        ma_score = 0
    score += ma_score
    
    # 布林带评分 (-0.2 到 0.2)
    boll_pos = indicators.get("boll_position", 0.5)
    if boll_pos < 0.2:
        boll_score = 0.2
        signals.append("触及布林下轨")
    elif boll_pos > 0.8:
        boll_score = -0.2
        signals.append("触及布林上轨")
    else:
        boll_score = (0.5 - abs(boll_pos - 0.5)) * 0.4
    score += boll_score
    
    return score, signals


if __name__ == "__main__":
    # 测试
    import sys
    sys.path.append("..")
    
    from data.fetcher import ETFFetcher
    
    fetcher = ETFFetcher()
    df = fetcher.get_etf_historical("159990")
    
    if not df.empty:
        analyzer = TechnicalAnalyzer(df)
        indicators = analyzer.get_latest_indicators()
        print("技术指标:", indicators)
        
        score, signals = calculate_technical_score(indicators)
        print(f"综合评分: {score:.2f}")
        print("信号:", signals)
