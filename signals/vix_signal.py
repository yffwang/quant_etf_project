# -*- coding: utf-8 -*-
"""
VIX-科技板块交易信号生成模块

核心逻辑：
1. VIX 处于极端高位（>35）或单日飙升（>10%）→ 清仓/大幅减仓
2. VIX 处于高波动区（30-35）且负相关性加剧 → 减仓/观望
3. VIX 从高位回落，相关性减弱 → 加仓/恢复仓位
4. VIX 处于低位（<20）且市场平静 → 正常持仓
"""
import pandas as pd
import numpy as np
from typing import Dict, List
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class VixAction(Enum):
    """持仓操作建议"""
    CLEARANCE = "清仓"          # 建议清空科技持仓
    REDUCE = "减仓"             # 建议减仓
    HOLD = "观望/持有"          # 建议维持现有仓位，不操作
    ADD = "加仓"                # 建议增加仓位
    FULL = "满仓"               # 建议满仓或高仓位


@dataclass
class VixSignal:
    """VIX 交易信号"""
    date: str
    action: VixAction
    score: float                # 综合信号分数 (-1 到 1, -1=强烈看空, 1=强烈看多)
    latest_vix: float
    vix_level: str
    vix_change_pct: float
    corr_20d: float
    beta_60d: float
    r2_60d: float
    reasons: List[str] = field(default_factory=list)
    position_suggestion: str = ""  # 仓位建议，如 "0%", "30%", "50%", "80%", "100%"


def generate_vix_signal(metrics: Dict) -> VixSignal:
    """
    根据 VIX 指标生成交易信号

    信号逻辑：
    - 极端恐慌 (VIX > 35 或 单日飙升 > 10%): 清仓
    - 恐慌区 (VIX 30-35 且 corr < -0.4): 减仓至 30%
    - 风险预警 (VIX  spike 或 corr 快速下降): 减仓至 50%
    - 观望 (VIX 20-30, 无明显方向): 维持仓位
    - 加仓 (VIX 从高位回落 > 15% 且 corr 回升): 加仓至 80%
    - 满仓 (VIX < 20 且稳定): 满仓
    """
    if not metrics:
        return VixSignal(date="", action=VixAction.HOLD, score=0,
                         latest_vix=0, vix_level="未知", vix_change_pct=0,
                         corr_20d=0, beta_60d=0, r2_60d=0,
                         reasons=["指标数据不足"], position_suggestion="维持现状")

    vix = metrics.get("latest_vix", 0) or 0
    vix_change = metrics.get("vix_change_pct", 0) or 0
    corr = metrics.get("corr_20d", 0) or 0
    beta = metrics.get("beta_60d", 0) or 0
    r2 = metrics.get("r2_60d", 0) or 0
    date = metrics.get("date", "")
    vix_level = metrics.get("vix_level", "未知")

    reasons = []
    score = 0.0  # 正分看多科技，负分看空科技

    # ========== 1. VIX 绝对水平判断 ==========
    if vix >= 35:
        score -= 0.8
        reasons.append(f"VIX处于极端恐慌区({vix:.1f})，全球风险偏好急剧下降")
    elif vix >= 30:
        score -= 0.5
        reasons.append(f"VIX处于恐慌区({vix:.1f})，美股波动剧烈")
    elif vix <= 20:
        score += 0.3
        reasons.append(f"VIX处于低波动区({vix:.1f})，市场情绪平稳")
    else:
        reasons.append(f"VIX处于正常波动区({vix:.1f})")

    # ========== 2. VIX 单日变化判断 ==========
    if vix_change >= 15:
        score -= 0.6
        reasons.append(f"VIX单日暴涨{vix_change:.1f}%，恐慌情绪急剧升温")
    elif vix_change >= 10:
        score -= 0.4
        reasons.append(f"VIX单日飙升{vix_change:.1f}%，短期风险剧增")
    elif vix_change <= -15:
        score += 0.4
        reasons.append(f"VIX单日大跌{vix_change:.1f}%，恐慌情绪显著消退")
    elif vix_change <= -10:
        score += 0.2
        reasons.append(f"VIX单日回落{vix_change:.1f}%，风险偏好回升")

    # ========== 3. 相关性判断 (负相关越强烈，越看空科技) ==========
    if corr <= -0.6:
        score -= 0.4
        reasons.append(f"20日相关系数极低({corr:.2f})，跨市场风险传染极强")
    elif corr <= -0.4:
        score -= 0.2
        reasons.append(f"20日相关系数较低({corr:.2f})，美股恐慌对A股科技压制明显")
    elif corr >= -0.1:
        score += 0.1
        reasons.append(f"20日相关系数接近0({corr:.2f})，关联性较弱")

    # ========== 4. Beta 敏感度判断 ==========
    if beta <= -0.2 and r2 >= 0.1:
        score -= 0.2
        reasons.append(f"Beta敏感度较高({beta:.3f})，VIX波动对科技持仓冲击大")
    elif beta >= -0.05:
        score += 0.1
        reasons.append(f"Beta敏感度低({beta:.3f})，科技持仓受VIX影响有限")

    # ========== 5. 综合信号映射到操作 ==========
    score = max(-1.0, min(1.0, score))

    if score <= -0.7:
        action = VixAction.CLEARANCE
        position = "0% (清仓)"
    elif score <= -0.4:
        action = VixAction.REDUCE
        position = "30% (轻仓)"
    elif score <= -0.15:
        action = VixAction.REDUCE
        position = "50% (半仓以下)"
    elif score >= 0.5:
        action = VixAction.FULL
        position = "100% (满仓)"
    elif score >= 0.2:
        action = VixAction.ADD
        position = "80% (重仓)"
    else:
        action = VixAction.HOLD
        if vix >= 25:
            position = "50% (观望)"
        else:
            position = "维持当前仓位"

    return VixSignal(
        date=date,
        action=action,
        score=round(score, 2),
        latest_vix=round(vix, 2),
        vix_level=vix_level,
        vix_change_pct=round(vix_change, 2),
        corr_20d=round(corr, 3) if corr is not None else None,
        beta_60d=round(beta, 3) if beta is not None else None,
        r2_60d=round(r2, 3) if r2 is not None else None,
        reasons=reasons,
        position_suggestion=position,
    )


def format_vix_report(signal: VixSignal, tech_holdings: Dict[str, float] = None) -> str:
    """
    格式化 VIX 监控报告 (文本版，用于飞书/控制台)

    Args:
        signal: VixSignal 对象
        tech_holdings: 可选，持仓标的今日涨跌幅 {名称: 涨跌幅%}
    """
    if not signal:
        return "暂无VIX信号数据"

    action_emoji = {
        VixAction.CLEARANCE: "🔴",
        VixAction.REDUCE: "🟠",
        VixAction.HOLD: "🟡",
        VixAction.ADD: "🟢",
        VixAction.FULL: "🟢",
    }.get(signal.action, "⚪")

    lines = []
    lines.append("=" * 55)
    lines.append(f"📊 VIX-科技板块关联监控报告  {signal.date}")
    lines.append("=" * 55)
    lines.append("")

    # VIX 状态
    vix_emoji = "🔴" if signal.latest_vix >= 30 else ("🟠" if signal.latest_vix >= 25 else "🟢")
    lines.append(f"{vix_emoji} VIX 状态: {signal.latest_vix:.2f} ({signal.vix_level})")
    change_str = f"{signal.vix_change_pct:+.2f}%"
    lines.append(f"   单日变化: {change_str}")
    lines.append("")

    # 关联指标
    lines.append("📈 关联指标:")
    corr_str = f"{signal.corr_20d:.3f}" if signal.corr_20d is not None else "N/A"
    beta_str = f"{signal.beta_60d:.3f}" if signal.beta_60d is not None else "N/A"
    r2_str = f"{signal.r2_60d:.3f}" if signal.r2_60d is not None else "N/A"
    lines.append(f"   20日相关系数: {corr_str}")
    lines.append(f"   60日Beta敏感度: {beta_str}")
    lines.append(f"   60日R²解释力: {r2_str}")
    lines.append("")

    # 交易信号
    lines.append("=" * 55)
    lines.append(f"{action_emoji} 【操作建议】{signal.action.value}")
    lines.append(f"   信号分数: {signal.score:+.2f} (-1=强烈看空, +1=强烈看多)")
    lines.append(f"   建议仓位: {signal.position_suggestion}")
    lines.append("")

    # 原因
    if signal.reasons:
        lines.append("💡 信号原因:")
        for i, r in enumerate(signal.reasons, 1):
            lines.append(f"   {i}. {r}")
        lines.append("")

    # 持仓表现
    if tech_holdings:
        lines.append("📉 科技持仓今日表现:")
        for name, pct in sorted(tech_holdings.items(), key=lambda x: x[1]):
            emoji = "🔴" if pct < -1 else ("🟠" if pct < 0 else "🟢")
            lines.append(f"   {emoji} {name}: {pct:+.2f}%")
        lines.append("")

    lines.append("=" * 55)
    lines.append("⚠️ 提示: 本信号基于历史相关性统计，不构成投资建议。")
    lines.append("   请结合基本面、技术面及自身风险承受能力决策。")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.path.append("..")

    # 测试
    test_metrics = {
        "date": "2026-06-11",
        "latest_vix": 32.5,
        "vix_level": "高波动/恐慌",
        "vix_change_pct": 12.3,
        "corr_20d": -0.55,
        "beta_60d": -0.18,
        "r2_60d": 0.25,
    }
    sig = generate_vix_signal(test_metrics)
    print(format_vix_report(sig, tech_holdings={
        "科创50": -2.1,
        "半导体": -3.5,
        "工业富联": -1.8,
    }))
