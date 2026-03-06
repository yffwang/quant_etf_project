# -*- coding: utf-8 -*-
"""
交易信号生成模块
"""
from typing import Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
import pandas as pd

from analyzers.technical import TechnicalAnalyzer, calculate_technical_score
from analyzers.momentum import MomentumAnalyzer, calculate_momentum_score
from analyzers.etf_factors import ETFFactorAnalyzer, calculate_etf_score

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """信号类型"""
    STRONG_BUY = "强烈买入"
    BUY = "买入"
    HOLD = "持有"
    SELL = "卖出"
    STRONG_SELL = "强烈卖出"
    NO_SIGNAL = "无信号"


@dataclass
class TradingSignal:
    """交易信号"""
    symbol: str
    name: str
    signal: SignalType
    score: float
    
    # 各维度评分
    technical_score: float = 0
    momentum_score: float = 0
    etf_score: float = 0
    
    # 信号强度 (0-1)
    strength: float = 0.5
    
    # 信号原因
    reasons: List[str] = None
    
    # 实时数据
    price: float = 0
    change_pct: float = 0
    
    # 技术指标
    indicators: Dict = None
    
    def __post_init__(self):
        if self.reasons is None:
            self.reasons = []
        if self.indicators is None:
            self.indicators = {}


class SignalGenerator:
    """交易信号生成器"""
    
    def __init__(
        self,
        technical_weight: float = 0.4,
        momentum_weight: float = 0.35,
        etf_weight: float = 0.25
    ):
        """
        初始化信号生成器
        
        Args:
            technical_weight: 技术指标权重
            momentum_weight: 动量因子权重
            etf_weight: ETF因子权重
        """
        self.weights = {
            "technical": technical_weight,
            "momentum": momentum_weight,
            "etf": etf_weight
        }
    
    def analyze(
        self,
        symbol: str,
        name: str,
        historical_data,
        realtime_data: Dict = None
    ) -> TradingSignal:
        """
        分析并生成交易信号
        
        Args:
            symbol: ETF代码
            name: ETF名称
            historical_data: 历史行情数据
            realtime_data: 实时行情数据
        """
        # 1. 技术指标分析
        tech_analyzer = TechnicalAnalyzer(historical_data)
        tech_indicators = tech_analyzer.get_latest_indicators()
        tech_score, tech_signals = calculate_technical_score(tech_indicators)
        
        # 2. 动量因子分析
        mom_analyzer = MomentumAnalyzer(historical_data)
        mom_indicators = mom_analyzer.get_latest_momentum()
        mom_score, mom_signals = calculate_momentum_score(mom_indicators)
        
        # 3. ETF因子分析
        etf_analyzer = ETFFactorAnalyzer(
            etf_info={},
            realtime=realtime_data or {},
            historical=historical_data
        )
        etf_factors = etf_analyzer.calculate_all()
        etf_score, etf_signals = calculate_etf_score(etf_factors)
        
        # 4. 综合评分
        total_score = (
            tech_score * self.weights["technical"] +
            mom_score * self.weights["momentum"] +
            etf_score * self.weights["etf"]
        )
        
        # 5. 确定信号类型
        signal_type = self._score_to_signal(total_score)
        
        # 6. 汇总所有信号原因
        all_reasons = tech_signals + mom_signals + etf_signals
        
        # 7. 计算信号强度
        strength = abs(total_score)
        
        # 8. 获取价格信息
        price = 0
        change_pct = 0
        if realtime_data:
            price = realtime_data.get("price", 0)
            change_pct = realtime_data.get("pct_change", 0)
        elif not historical_data.empty:
            price = historical_data.iloc[-1].get("close", 0)
            change_pct = historical_data.iloc[-1].get("pct_change", 0)
        
        # 合并所有指标
        all_indicators = {**tech_indicators, **mom_indicators, **etf_factors}
        
        return TradingSignal(
            symbol=symbol,
            name=name,
            signal=signal_type,
            score=total_score,
            technical_score=tech_score,
            momentum_score=mom_score,
            etf_score=etf_score,
            strength=strength,
            reasons=all_reasons,
            price=price,
            change_pct=change_pct,
            indicators=all_indicators
        )
    
    def _score_to_signal(self, score: float) -> SignalType:
        """
        将综合评分转换为信号类型
        
        Args:
            score: 综合评分 (-1 到 1)
        """
        if score >= 0.6:
            return SignalType.STRONG_BUY
        elif score >= 0.3:
            return SignalType.BUY
        elif score >= -0.3:
            return SignalType.HOLD
        elif score >= -0.6:
            return SignalType.SELL
        else:
            return SignalType.STRONG_SELL
    
    def analyze_batch(
        self,
        etf_list: List[Dict],
        historical_data_dict: Dict[str, pd.DataFrame],
        realtime_data_dict: Dict[str, Dict] = None
    ) -> List[TradingSignal]:
        """
        批量分析ETF并生成信号
        """
        signals = []
        
        for etf in etf_list:
            symbol = etf.get("code") or etf.get("symbol")
            name = etf.get("name", "")
            
            if symbol not in historical_data_dict:
                logger.warning(f"没有找到 {symbol} 的历史数据")
                continue
            
            historical = historical_data_dict[symbol]
            realtime = None
            if realtime_data_dict and symbol in realtime_data_dict:
                realtime = realtime_data_dict[symbol]
            
            try:
                signal = self.analyze(symbol, name, historical, realtime)
                signals.append(signal)
            except Exception as e:
                logger.error(f"分析 {symbol} 失败: {e}")
        
        # 按评分排序
        signals.sort(key=lambda x: x.score, reverse=True)
        
        return signals
    
    def filter_signals(
        self,
        signals: List[TradingSignal],
        signal_type: SignalType = None,
        min_score: float = None,
        min_strength: float = None
    ) -> List[TradingSignal]:
        """
        过滤信号
        """
        filtered = signals
        
        if signal_type:
            filtered = [s for s in filtered if s.signal == signal_type]
        
        if min_score is not None:
            filtered = [s for s in filtered if s.score >= min_score]
        
        if min_strength is not None:
            filtered = [s for s in filtered if s.strength >= min_strength]
        
        return filtered
    
    def get_top_signals(
        self,
        signals: List[TradingSignal],
        n: int = 10,
        signal_types: List[SignalType] = None
    ) -> Dict[str, List[TradingSignal]]:
        """
        获取Top N信号，按类型分组
        """
        result = {
            "strong_buy": [],
            "buy": [],
            "hold": [],
            "sell": [],
            "strong_sell": []
        }
        
        for signal in signals:
            if signal_types and signal.signal not in signal_types:
                continue
            
            if signal.signal == SignalType.STRONG_BUY:
                result["strong_buy"].append(signal)
            elif signal.signal == SignalType.BUY:
                result["buy"].append(signal)
            elif signal.signal == SignalType.SELL:
                result["sell"].append(signal)
            elif signal.signal == SignalType.STRONG_SELL:
                result["strong_sell"].append(signal)
            else:
                result["hold"].append(signal)
        
        # 限制数量
        for key in result:
            result[key] = result[key][:n]
        
        return result


def format_signal_report(signals: List[TradingSignal]) -> str:
    """
    格式化信号报告
    """
    if not signals:
        return "暂无信号数据"
    
    lines = ["📊 ETF量化分析信号报告", "=" * 50, ""]
    
    # 分类统计
    signal_counts = {}
    for s in signals:
        key = s.signal.value
        signal_counts[key] = signal_counts.get(key, 0) + 1
    
    lines.append("📈 信号分布:")
    for sig, count in sorted(signal_counts.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"  • {sig}: {count}只")
    lines.append("")
    
    # ==================== 详细分析每只ETF ====================
    lines.append("📋 详细分析报告")
    lines.append("=" * 80)
    
    for s in signals:
        signal_emoji = "🟢" if s.score >= 0.3 else ("🔴" if s.score <= -0.3 else "🟡")
        price_str = f"{s.price:.3f}" if s.price else "N/A"
        change_str = f"{s.change_pct:+.2f}%" if s.change_pct is not None else "N/A"
        
        lines.append(f"\n{signal_emoji} {s.symbol} - {s.name}")
        lines.append(f"  信号: {s.signal.value} | 综合评分: {s.score:.2f} | 强度: {s.strength:.2f}")
        lines.append(f"  价格: {price_str} | 涨跌幅: {change_str}")
        
        # 技术指标
        ind = s.indicators
        lines.append(f"\n  📊 技术指标:")
        lines.append(f"    MA:    MA5={ind.get('ma5', 'N/A'):>8} MA10={ind.get('ma10', 'N/A'):>8} MA20={ind.get('ma20', 'N/A'):>8} MA60={ind.get('ma60', 'N/A'):>8}")
        lines.append(f"    MACD:  DIF={ind.get('macd', 'N/A'):>8} DEA={ind.get('macd_signal', 'N/A'):>8} BAR={ind.get('macd_hist', 'N/A'):>8}")
        lines.append(f"    RSI:   {ind.get('rsi', 'N/A'):.2f}")
        lines.append(f"    BOLL:  Upper={ind.get('boll_upper', 'N/A'):>8} Middle={ind.get('boll_middle', 'N/A'):>8} Lower={ind.get('boll_lower', 'N/A'):>8}")
        
        # 动量指标
        lines.append(f"\n  📈 动量指标:")
        lines.append(f"    5日动量: {ind.get('momentum_short', 'N/A'):>8.2%}")
        lines.append(f"   20日动量: {ind.get('momentum_medium', 'N/A'):>8.2%}")
        lines.append(f"   60日动量: {ind.get('momentum_long', 'N/A'):>8.2%}")
        
        # 评分详情
        lines.append(f"\n  🎯 评分详情:")
        lines.append(f"    技术评分: {s.technical_score:.2f} | 动量评分: {s.momentum_score:.2f} | ETF因子: {s.etf_score:.2f}")
        
        # 信号原因
        if s.reasons:
            lines.append(f"\n  💡 信号原因:")
            for i, reason in enumerate(s.reasons, 1):
                lines.append(f"    {i}. {reason}")
        
        lines.append("\n" + "-" * 80)
    
    # ==================== 简洁汇总 ====================
    lines.append("\n" + "=" * 80)
    lines.append("📊 信号汇总")
    lines.append("=" * 80)
    
    # 强烈买入
    strong_buy = [s for s in signals if s.signal == SignalType.STRONG_BUY]
    if strong_buy:
        lines.append("\n🟢 强烈买入 (Top 5):")
        for s in strong_buy[:5]:
            price_str = f"{s.price:.3f}" if s.price else "N/A"
            change_str = f"{s.change_pct:+.2f}%" if s.change_pct is not None else "N/A"
            lines.append(f"  {s.symbol} {s.name}")
            lines.append(f"    价格: {price_str} ({change_str})")
            lines.append(f"    评分: {s.score:.2f} | 强度: {s.strength:.2f}")
            if s.reasons:
                lines.append(f"    原因: {', '.join(s.reasons[:3])}")
            lines.append("")
    
    # 买入
    buy = [s for s in signals if s.signal == SignalType.BUY]
    if buy:
        lines.append("🟢 买入推荐 (Top 5):")
        for s in buy[:5]:
            price_str = f"{s.price:.3f}" if s.price else "N/A"
            change_str = f"{s.change_pct:+.2f}%" if s.change_pct is not None else "N/A"
            lines.append(f"  {s.symbol} {s.name}")
            lines.append(f"    价格: {price_str} ({change_str})")
            lines.append(f"    评分: {s.score:.2f}")
            lines.append("")
    
    # 卖出
    sell = [s for s in signals if s.signal == SignalType.SELL]
    if sell:
        lines.append("🔴 卖出建议 (Top 5):")
        for s in sell[:5]:
            price_str = f"{s.price:.3f}" if s.price else "N/A"
            change_str = f"{s.change_pct:+.2f}%" if s.change_pct is not None else "N/A"
            lines.append(f"  {s.symbol} {s.name}")
            lines.append(f"    价格: {price_str} ({change_str})")
            lines.append(f"    评分: {s.score:.2f}")
            lines.append("")
    
    # 强烈卖出
    strong_sell = [s for s in signals if s.signal == SignalType.STRONG_SELL]
    if strong_sell:
        lines.append("🔴 强烈卖出 (Top 5):")
        for s in strong_sell[:5]:
            price_str = f"{s.price:.3f}" if s.price else "N/A"
            change_str = f"{s.change_pct:+.2f}%" if s.change_pct is not None else "N/A"
            lines.append(f"  {s.symbol} {s.name}")
            lines.append(f"    价格: {price_str} ({change_str})")
            lines.append(f"    评分: {s.score:.2f}")
            lines.append("")
    
    lines.append("\n" + "=" * 80)
    lines.append("💡 提示: 本报告仅供参考，不构成投资建议")
    
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.path.append("..")
    
    from data.fetcher import ETFFetcher
    
    # 测试
    fetcher = ETFFetcher()
    
    symbols = ["159990", "159870", "159883", "159885"]
    signals = []
    
    for symbol in symbols:
        historical = fetcher.get_etf_historical(symbol)
        realtime = fetcher.get_etf_realtime(symbol)
        
        if not historical.empty:
            generator = SignalGenerator()
            signal = generator.analyze(symbol, symbol, historical, realtime)
            signals.append(signal)
            print(f"{symbol}: {signal.signal.value} (评分: {signal.score:.2f})")
