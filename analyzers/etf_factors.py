# -*- coding: utf-8 -*-
"""
ETF专用因子分析模块
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ETFFactorAnalyzer:
    """ETF专用因子分析器"""
    
    def __init__(self, etf_info: Dict = None, realtime: Dict = None, historical: pd.DataFrame = None):
        """
        初始化ETF因子分析器
        
        Args:
            etf_info: ETF基本信息 (从AkShare获取)
            realtime: 实时行情数据
            historical: 历史行情数据
        """
        self.etf_info = etf_info or {}
        self.realtime = realtime or {}
        self.historical = historical
        
        if historical is not None:
            self.historical = historical.sort_values("date")
    
    def calculate_premium_rate(self) -> float:
        """
        计算溢价率
        
        溢价率 = (ETF价格 - 净值) / 净值 * 100%
        
        溢价率高说明ETF价格高于内在价值，可能有回调风险
        溢价率低甚至为负，说明存在套利机会
        """
        # 从实时数据获取溢价率
        premium = self.realtime.get("pct_change")  # 注意：这里可能需要根据实际数据调整
        
        # 如果没有实时溢价率，从历史数据估算
        if premium is None and self.historical is not None and not self.historical.empty:
            # 使用涨跌幅作为近似
            premium = self.historical.iloc[-1].get("pct_change", 0)
        
        return premium if premium else 0
    
    def calculate_tracking_error(self, benchmark_return: float = None) -> float:
        """
        计算跟踪误差
        
        跟踪误差 = ETF收益率 - 基准收益率 的标准差
        
        跟踪误差小说明ETF复制指数能力强
        """
        if self.historical is None or len(self.historical) < 20:
            return 0
        
        # 计算ETF日收益率
        etf_returns = self.historical["close"].pct_change().dropna()
        
        if benchmark_return is None:
            # 如果没有基准，假设为0（相对无风险收益）
            benchmark_returns = pd.Series([0] * len(etf_returns))
        else:
            # 转换为日收益率
            daily_benchmark = (1 + benchmark_return) ** (1/252) - 1
            benchmark_returns = pd.Series([daily_benchmark] * len(etf_returns))
        
        # 计算跟踪误差（标准差）
        tracking_diff = etf_returns - benchmark_returns
        tracking_error = tracking_diff.std() * np.sqrt(252)  # 年化
        
        return tracking_error if not np.isnan(tracking_error) else 0
    
    def calculate规模因子(self) -> Dict:
        """
        计算规模因子
        
        规模太小可能有清盘风险，流动性差
        """
        amount = self.realtime.get("amount", 0)
        volume = self.realtime.get("volume", 0)
        
        # 估算规模（日成交额作为流动性的代理）
        size_factor = {
            "日成交额": amount,
            "日成交量": volume,
            "流动性评级": self._rate_liquidity(amount)
        }
        
        return size_factor
    
    def _rate_liquidity(self, amount: float) -> str:
        """评级流动性"""
        if amount > 10_000_000_000:  # 100亿
            return "极高"
        elif amount > 1_000_000_000:  # 10亿
            return "高"
        elif amount > 100_000_000:    # 1亿
            return "中"
        elif amount > 10_000_000:     # 1000万
            return "低"
        else:
            return "极低"
    
    def calculate_换手率因子(self) -> Dict:
        """
        计算换手率因子
        
        换手率高说明交易活跃，流动性好
        但换手率过高可能说明投机氛围重
        """
        turnover = self.realtime.get("turnover", 0)
        
        turnover_factor = {
            "换手率": turnover,
            "交易活跃度": self._rate_turnover(turnover)
        }
        
        return turnover_factor
    
    def _rate_turnover(self, turnover: float) -> str:
        """评级换手率"""
        if turnover > 20:
            return "极高"
        elif turnover > 10:
            return "高"
        elif turnover > 3:
            return "中"
        elif turnover > 1:
            return "低"
        else:
            return "极低"
    
    def calculate_波动率因子(self) -> Dict:
        """
        计算ETF波动率因子
        
        低波动ETF通常更适合长期持有
        """
        if self.historical is None or len(self.historical) < 20:
            return {"波动率": 0, "波动评级": "未知"}
        
        returns = self.historical["close"].pct_change().dropna()
        
        # 年化波动率
        volatility = returns.std() * np.sqrt(252)
        
        return {
            "波动率": volatility,
            "波动评级": self._rate_volatility(volatility)
        }
    
    def _rate_volatility(self, vol: float) -> str:
        """评级波动率"""
        if vol > 0.4:
            return "极高"
        elif vol > 0.25:
            return "高"
        elif vol > 0.15:
            return "中"
        elif vol > 0.08:
            return "低"
        else:
            return "极低"
    
    def calculate_收益因子(self) -> Dict:
        """
        计算收益因子
        
        不同周期的收益率
        """
        if self.historical is None or len(self.historical) < 5:
            return {}
        
        close = self.historical["close"]
        
        return {
            "近5日收益": (close.iloc[-1] / close.iloc[-6] - 1) if len(close) > 5 else 0,
            "近20日收益": (close.iloc[-1] / close.iloc[-21] - 1) if len(close) > 20 else 0,
            "近60日收益": (close.iloc[-1] / close.iloc[-61] - 1) if len(close) > 60 else 0,
            "年初至今收益": self._calculate_ytd_return(close),
        }
    
    def _calculate_ytd_return(self, close: pd.Series) -> float:
        """计算年初至今收益"""
        if self.historical is None or self.historical.empty:
            return 0
        
        # 找到今年第一个交易日
        first_day = self.historical[self.historical["date"].dt.month == 1]
        
        if first_day.empty:
            return 0
        
        first_price = first_day.iloc[0]["close"]
        current_price = close.iloc[-1]
        
        return (current_price / first_price - 1) if first_price > 0 else 0
    
    def calculate_all(self) -> Dict:
        """
        计算所有ETF因子
        """
        factors = {}
        
        # 溢价率
        factors["溢价率"] = self.calculate_premium_rate()
        
        # 跟踪误差
        factors["跟踪误差"] = self.calculate_tracking_error()
        
        # 规模因子
        factors.update(self.calculate规模因子())
        
        # 换手率因子
        factors.update(self.calculate_换手率因子())
        
        # 波动率因子
        factors.update(self.calculate_波动率因子())
        
        # 收益因子
        factors.update(self.calculate_收益因子())
        
        return factors


def calculate_etf_score(factors: Dict) -> tuple:
    """
    计算ETF综合评分 (-1 到 1)
    
    正分: 推荐持有
    负分: 建议谨慎
    """
    score = 0.0
    signals = []
    
    # 溢价率评分 (-0.2 到 0.2)
    premium = factors.get("溢价率", 0)
    if premium is None:
        premium = 0
    
    if premium < 0:
        premium_score = 0.15  # 负溢价（折价）是好事
        signals.append(f"折价({premium:.2f}%)")
    elif premium < 1:
        premium_score = 0.1
    elif premium < 3:
        premium_score = 0
    elif premium < 5:
        premium_score = -0.1
        signals.append(f"溢价较高({premium:.2f}%)")
    else:
        premium_score = -0.2
        signals.append(f"高溢价风险({premium:.2f}%)")
    score += premium_score
    
    # 流动性评分 (0 到 0.15)
    liquidity = factors.get("流动性评级", "低")
    liquidity_scores = {"极高": 0.15, "高": 0.1, "中": 0.05, "低": 0, "极低": -0.1}
    score += liquidity_scores.get(liquidity, 0)
    
    # 换手率评分 (-0.1 到 0.1)
    turnover = factors.get("换手率", 0)
    if 3 < turnover < 20:
        turnover_score = 0.1  # 活跃但不过高
        signals.append(f"换手适中({turnover:.1f}%)")
    elif turnover > 30:
        turnover_score = -0.1
        signals.append(f"换手过高({turnover:.1f}%)")
    else:
        turnover_score = 0.05  # 低换手说明长期持有
    score += turnover_score
    
    # 波动率评分 (-0.15 到 0.15)
    volatility = factors.get("波动率", 0)
    if volatility < 0.1:
        vol_score = 0.15
        signals.append(f"低波动({volatility*100:.1f}%)")
    elif volatility < 0.2:
        vol_score = 0.05
    elif volatility > 0.35:
        vol_score = -0.15
        signals.append(f"高波动({volatility*100:.1f}%)")
    else:
        vol_score = 0
    score += vol_score
    
    # 近期表现 (0 到 0.2)
    returns_5d = factors.get("近5日收益", 0) or 0
    returns_20d = factors.get("近20日收益", 0) or 0
    
    if returns_5d > 0.05:
        score += 0.1
        signals.append(f"5日大涨({returns_5d*100:.1f}%)")
    elif returns_5d < -0.05:
        score -= 0.1
        signals.append(f"5日大跌({returns_5d*100:.1f}%)")
    
    if returns_20d > 0.1:
        score += 0.1
    elif returns_20d < -0.1:
        score -= 0.1
    
    # 跟踪误差 (0 到 0.1)
    tracking_error = factors.get("跟踪误差", 0)
    if tracking_error < 0.02:
        score += 0.1
        signals.append(f"低跟踪误差({tracking_error*100:.1f}%)")
    elif tracking_error > 0.1:
        score -= 0.1
        signals.append(f"高跟踪误差({tracking_error*100:.1f}%)")
    
    # 限制分数范围
    score = max(-1, min(1, score))
    
    return score, signals


if __name__ == "__main__":
    # 测试
    import sys
    sys.path.append("..")
    
    from data.fetcher import ETFFetcher
    
    fetcher = ETFFetcher()
    historical = fetcher.get_etf_historical("159990")
    realtime = fetcher.get_etf_realtime("159990")
    
    if not historical.empty:
        analyzer = ETFFactorAnalyzer(
            etf_info={},
            realtime=realtime,
            historical=historical
        )
        factors = analyzer.calculate_all()
        print("ETF因子:", factors)
        
        score, signals = calculate_etf_score(factors)
        print(f"ETF评分: {score:.2f}")
        print("信号:", signals)
