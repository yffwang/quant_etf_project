# -*- coding: utf-8 -*-
"""
多周期趋势共振分析模块（方案三）

核心思路：
  对日线 / 周线 / 月线 三个时间框架独立评分，然后通过共振矩阵计算综合趋势评分。
  - 月线决定大方向（仓位决定器）
  - 周线确认中周期趋势
  - 日线提供入场时机参考

当三个周期同时看多 → 最强信号；月线看空但日线看多 → 仅视为反弹，降级处理。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

from config import TREND_PARAMS

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """
    多周期趋势共振分析器

    对日/周/月三个时间框架分别计算：
      - MA 斜率方向（均线是向上还是向下）
      - MA 多头排列稳定性（短均线在长均线上方的时间占比）
      - MACD 状态（金叉/死叉/柱状图方向）
      - 价格相对均线位置（允许轻微跌破的占比统计）
      - ADX 趋势强度（用作置信度乘数，月线不适用）

    最后通过共振矩阵综合三个周期的评分。
    """

    def __init__(
        self,
        df: pd.DataFrame,
        cfg: Optional[Dict] = None,
        min_daily_bars: int = 60,
        min_weekly_bars: int = 20,
        min_monthly_bars: int = 12,
    ):
        """
        Args:
            df: 日线 OHLCV DataFrame（需含 date/open/high/low/close/volume）
            cfg: 趋势参数配置，默认使用 TREND_PARAMS
            min_daily_bars: 日线最少 bar 数
            min_weekly_bars: 周线最少 bar 数
            min_monthly_bars: 月线最少 bar 数
        """
        self.cfg = cfg or TREND_PARAMS
        self.min_daily = min_daily_bars
        self.min_weekly = min_weekly_bars
        self.min_monthly = min_monthly_bars

        # ---- 准备数据 ----
        raw = df.sort_values("date").reset_index(drop=True).copy()

        # 确保必要的数值列存在
        for col in ["open", "high", "low", "close"]:
            if col in raw.columns:
                raw[col] = pd.to_numeric(raw[col], errors="coerce")
        if "volume" in raw.columns:
            raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce")

        self.daily_df = raw
        self.weekly_df = self._resample_ohlcv(raw, "W-FRI")
        self.monthly_df = self._resample_ohlcv(raw, "ME")

        # 可用性标记
        self.has_daily = len(self.daily_df) >= self.min_daily
        self.has_weekly = len(self.weekly_df) >= self.min_weekly
        self.has_monthly = len(self.monthly_df) >= self.min_monthly

    # ================================================================
    # 公开 API
    # ================================================================

    def analyze(self) -> Dict:
        """
        执行完整的多周期趋势分析。

        Returns:
            {
                "trend_score": float,       # 共振趋势评分 [-1, 1]
                "trend_phase": str,         # 趋势阶段描述
                "daily": Dict | None,       # 日线分析详情
                "weekly": Dict | None,      # 周线分析详情
                "monthly": Dict | None,     # 月线分析详情
                "available_timeframes": [], # 有效的时间框架列表
            }
        """
        result: Dict = {
            "trend_score": 0.0,
            "trend_phase": "数据不足",
            "daily": None,
            "weekly": None,
            "monthly": None,
            "available_timeframes": [],
        }

        # --- 日线 ---
        if self.has_daily:
            daily = self._analyze_timeframe(self.daily_df, self.cfg["daily"])
        else:
            daily = {"score": 0.0, "adx": 0, "signals": ["日线数据不足"]}
        result["daily"] = daily
        result["available_timeframes"].append("daily")

        # --- 周线 ---
        if self.has_weekly:
            weekly = self._analyze_timeframe(self.weekly_df, self.cfg["weekly"])
        else:
            weekly = None
        result["weekly"] = weekly
        if weekly:
            result["available_timeframes"].append("weekly")

        # --- 月线 ---
        if self.has_monthly:
            monthly = self._analyze_timeframe(self.monthly_df, self.cfg["monthly"])
        else:
            monthly = None
        result["monthly"] = monthly
        if monthly:
            result["available_timeframes"].append("monthly")

        # --- 共振计算 ---
        resonance_score, phase = self._compute_resonance(daily, weekly, monthly)
        result["trend_score"] = round(resonance_score, 4)
        result["trend_phase"] = phase

        return result

    # ================================================================
    # 数据重采样
    # ================================================================

    @staticmethod
    def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
        """将日线 OHLCV 重采样为周线或月线。"""
        if df.empty or "date" not in df.columns:
            return pd.DataFrame()

        work = df[["date", "open", "high", "low", "close", "volume"]].copy()
        work = work.set_index("date")

        ohlcv = work.resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna()

        return ohlcv.reset_index()

    # ================================================================
    # 单时间框架分析
    # ================================================================

    def _analyze_timeframe(self, df: pd.DataFrame, cfg: Dict) -> Dict:
        """
        对单个时间框架计算所有趋势子指标并加权合成评分。

        Returns:
            {
                "score": float,              # 该周期趋势评分 [-1, 1]
                "adx": float,                # ADX 值（月线为 0）
                "confidence": float,         # 置信度 [0, 1]
                "ma_slope_score": float,
                "ma_alignment_score": float,
                "macd_score": float,
                "price_position_score": float,
                "signals": [str],            # 该周期的文字信号
                # 详细指标（供报告用）
                "ma_slopes": {period: slope},
                "alignment_ratio": float,
                "price_above_ma_ratio": float,
            }
        """
        df = df.copy()

        # ---- 计算所有 MA ----
        for p in cfg["ma_periods"]:
            df[f"ma{p}"] = df["close"].rolling(window=p).mean()
        df = df.dropna(subset=[f"ma{max(cfg['ma_periods'])}"]).reset_index(drop=True)

        if df.empty:
            return {"score": 0.0, "adx": 0, "confidence": 0,
                    "ma_slope_score": 0, "ma_alignment_score": 0,
                    "macd_score": 0, "price_position_score": 0,
                    "signals": ["计算数据不足"]}

        # ---- 子维度评分 ----
        ma_slope_score, ma_slopes = self._ma_slope_score(df, cfg)
        ma_align_score, align_ratio = self._ma_alignment_score(df, cfg)
        macd_score, macd_bullish = self._macd_score(df, cfg)
        price_pos_score, price_ratio = self._price_position_score(df, cfg)

        # ---- ADX 置信度 ----
        adx_val = 0.0
        confidence = 0.5  # 默认中等置信度
        if cfg.get("adx_period"):
            adx_val = self._compute_adx(df, cfg["adx_period"])
            confidence = self._adx_confidence(adx_val)

        # ---- 子维度合成 ----
        # 权重：斜率 0.30 / 排列 0.30 / MACD 0.25 / 价格位置 0.15
        raw_score = (
            ma_slope_score * 0.30 +
            ma_align_score * 0.30 +
            macd_score * 0.25 +
            price_pos_score * 0.15
        )

        # ADX 置信度乘数：趋势明确时保持原分，震荡时往 0 拉
        score = raw_score * (0.5 + 0.5 * confidence)
        score = max(-1.0, min(1.0, score))

        # ---- 信号文字 ----
        signals = []
        if score > 0.3:
            signals.append("趋势向上")
        elif score < -0.3:
            signals.append("趋势向下")
        else:
            signals.append("趋势震荡")

        if adx_val >= self.cfg.get("adx_strong", 25):
            signals.append(f"趋势明确(ADX={adx_val:.1f})")
        elif adx_val >= self.cfg.get("adx_moderate", 20):
            signals.append(f"趋势中等(ADX={adx_val:.1f})")

        for p in cfg["slope_periods"]:
            slope = ma_slopes.get(p, 0)
            if slope > 0.003:
                signals.append(f"MA{p}向上")
            elif slope < -0.003:
                signals.append(f"MA{p}向下")

        return {
            "score": round(score, 4),
            "adx": round(adx_val, 2),
            "confidence": round(confidence, 2),
            "ma_slope_score": round(ma_slope_score, 4),
            "ma_alignment_score": round(ma_align_score, 4),
            "macd_score": round(macd_score, 4),
            "price_position_score": round(price_pos_score, 4),
            "signals": signals,
            "ma_slopes": {str(p): round(s, 6) for p, s in ma_slopes.items()},
            "alignment_ratio": round(align_ratio, 4),
            "price_above_ma_ratio": round(price_ratio, 4),
            "macd_bullish": macd_bullish,
        }

    # ================================================================
    # 子维度：MA 斜率
    # ================================================================

    def _ma_slope_score(self, df: pd.DataFrame, cfg: Dict) -> Tuple[float, Dict[int, float]]:
        """
        MA 斜率评分。

        对指定的均线计算归一化斜率（线性回归斜率 / 均价），
        斜率为正 → 均线向上 → 看多；斜率为负 → 看空。

        Returns:
            (slope_score [-1, 1], {period: normalized_slope})
        """
        lookback = cfg["slope_lookback"]
        slopes = {}

        for p in cfg["slope_periods"]:
            col = f"ma{p}"
            if col not in df.columns:
                slopes[p] = 0.0
                continue
            series = df[col].tail(lookback).values
            if len(series) < lookback or np.isnan(series).any():
                slopes[p] = 0.0
                continue

            x = np.arange(lookback)
            slope, _ = np.polyfit(x, series, 1)
            mean_val = np.mean(series)
            if abs(mean_val) < 1e-8:
                slopes[p] = 0.0
            else:
                slopes[p] = slope / mean_val  # 归一化：比如 0.01 = 每 bar 涨 1%

        if not slopes:
            return 0.0, {}

        # 取各均线斜率的平均值，映射到 [-1, 1]
        avg_slope = np.mean(list(slopes.values()))
        # 斜率 0.5% 以上视为强力趋势，映射到 +1
        normalized = np.clip(avg_slope / 0.005, -1.0, 1.0)
        return float(normalized), slopes

    # ================================================================
    # 子维度：MA 多头排列
    # ================================================================

    def _ma_alignment_score(self, df: pd.DataFrame, cfg: Dict) -> Tuple[float, float]:
        """
        MA 多头排列评分。

        检查各均线对之间的排序关系（短 > 长 = 多头），
        统计最近 lookback 根 bar 中多头排列的稳定性。

        Returns:
            (alignment_score [-1, 1], alignment_ratio [0, 1])
        """
        pairs = cfg.get("alignment_pairs", [])
        if not pairs:
            return 0.0, 0.0

        lookback = cfg.get("price_lookback", 20)
        recent = df.tail(lookback)

        # 对每一根 bar 统计有多少对均线满足多头排列
        pair_count = len(pairs)
        aligned_counts = []

        for _, row in recent.iterrows():
            aligned = 0
            for short_p, long_p in pairs:
                short_col = f"ma{short_p}"
                long_col = f"ma{long_p}"
                if short_col in row.index and long_col in row.index:
                    if pd.notna(row[short_col]) and pd.notna(row[long_col]):
                        if row[short_col] > row[long_col]:
                            aligned += 1
            aligned_counts.append(aligned / pair_count if pair_count > 0 else 0)

        avg_alignment = np.mean(aligned_counts) if aligned_counts else 0.5

        # 最新一根 bar 的对齐情况
        latest = recent.iloc[-1]
        latest_aligned = 0
        for short_p, long_p in pairs:
            short_col = f"ma{short_p}"
            long_col = f"ma{long_p}"
            if short_col in latest.index and long_col in latest.index:
                if pd.notna(latest[short_col]) and pd.notna(latest[long_col]):
                    if latest[short_col] > latest[long_col]:
                        latest_aligned += 1
        latest_ratio = latest_aligned / pair_count if pair_count > 0 else 0.5

        # 综合评分：稳定性 60% + 最新状态 40%
        ratio = avg_alignment * 0.6 + latest_ratio * 0.4
        # 映射到 [-1, 1]
        score = (ratio - 0.5) * 2.0

        return float(max(-1.0, min(1.0, score))), float(ratio)

    # ================================================================
    # 子维度：MACD 状态
    # ================================================================

    def _macd_score(self, df: pd.DataFrame, cfg: Dict) -> Tuple[float, bool]:
        """
        MACD 状态评分。

        综合判断 DIF-DEA 关系、柱状图方向。

        Returns:
            (macd_score [-1, 1], is_bullish)
        """
        fast = cfg.get("macd_fast", 12)
        slow = cfg.get("macd_slow", 26)
        signal = cfg.get("macd_signal", 9)

        close = df["close"].values
        if len(close) < slow + signal:
            return 0.0, False

        # 计算 EMA
        ema_fast = self._ema(close, fast)
        ema_slow = self._ema(close, slow)
        dif = ema_fast - ema_slow
        dea = self._ema(dif, signal)
        hist = (dif - dea) * 2

        latest_dif = float(dif[-1])
        latest_dea = float(dea[-1])
        latest_hist = float(hist[-1])
        prev_hist = float(hist[-2]) if len(hist) >= 2 else 0.0

        # 金叉/死叉
        if len(dif) >= 2 and len(dea) >= 2:
            prev_dif = float(dif[-2])
            prev_dea = float(dea[-2])
            golden_cross = prev_dif <= prev_dea and latest_dif > latest_dea
            death_cross = prev_dif >= prev_dea and latest_dif < latest_dea
        else:
            golden_cross = False
            death_cross = False

        # 评分
        score = 0.0
        is_bullish = False

        if golden_cross:
            score = 0.7
            is_bullish = True
        elif death_cross:
            score = -0.7
            is_bullish = False
        elif latest_dif > latest_dea:
            # DIF 在 DEA 上方
            score = 0.3
            is_bullish = True
            if latest_hist > 0 and latest_hist > prev_hist:
                score = 0.5  # 柱状图在零轴上方且放大
            elif latest_hist > 0 and latest_hist <= prev_hist:
                score = 0.2  # 柱状图在零轴上方但缩小（动能减弱）
        else:
            # DIF 在 DEA 下方
            score = -0.3
            is_bullish = False
            if latest_hist < 0 and latest_hist < prev_hist:
                score = -0.5  # 柱状图在零轴下方且放大
            elif latest_hist < 0 and latest_hist >= prev_hist:
                score = -0.15  # 柱状图在零轴下方但缩小（动能回升）

        return float(max(-1.0, min(1.0, score))), is_bullish

    # ================================================================
    # 子维度：价格相对均线位置（含回撤容忍）
    # ================================================================

    def _price_position_score(self, df: pd.DataFrame, cfg: Dict) -> Tuple[float, float]:
        """
        价格相对均线的位置评分。

        统计最近 N 根 bar 中收盘价在均线上方的占比，
        允许一定比例的轻微跌破（方案三的回撤容忍）。

        Returns:
            (position_score [-1, 1], above_ratio)
        """
        ma_p = cfg.get("price_vs_ma", 20)
        lookback = cfg.get("price_lookback", 20)
        ma_col = f"ma{ma_p}"

        if ma_col not in df.columns:
            return 0.0, 0.5

        recent = df.tail(lookback)
        above = (recent["close"] >= recent[ma_col]).sum()
        total = len(recent)
        ratio = above / total if total > 0 else 0.5

        # 映射到 [-1, 1]：0.7+ → +1, 0.3- → -1
        score = (ratio - 0.5) * 2.5
        return float(max(-1.0, min(1.0, score))), float(ratio)

    # ================================================================
    # ADX 计算与置信度
    # ================================================================

    @staticmethod
    def _compute_adx(df: pd.DataFrame, period: int = 14) -> float:
        """
        计算 ADX（平均趋向指数）。

        标准 Wilder 算法：
          TR  = max(H-L, |H-C_prev|, |L-C_prev|)
          +DM = H - H_prev  if H-H_prev > L_prev-L and H-H_prev > 0 else 0
          -DM = L_prev - L  if L_prev-L > H-H_prev and L_prev-L > 0 else 0
          +DI = 100 * EMA(+DM, period) / ATR(period)
          -DI = 100 * EMA(-DM, period) / ATR(period)
          DX  = 100 * |+DI - -DI| / (+DI + -DI)
          ADX = EMA(DX, period)
        """
        high = df["high"].values
        low = df["low"].values
        close = df["close"].values

        if len(high) < period + 1:
            return 0.0

        tr = np.zeros(len(high))
        plus_dm = np.zeros(len(high))
        minus_dm = np.zeros(len(high))

        for i in range(1, len(high)):
            tr[i] = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i] - close[i - 1]),
            )
            up_move = high[i] - high[i - 1]
            down_move = low[i - 1] - low[i]

            if up_move > down_move and up_move > 0:
                plus_dm[i] = up_move
            else:
                plus_dm[i] = 0.0

            if down_move > up_move and down_move > 0:
                minus_dm[i] = down_move
            else:
                minus_dm[i] = 0.0

        # Wilder smoothing (EMA with alpha = 1/period)
        atr = TrendAnalyzer._wilders_ema(tr, period)
        plus_di = 100.0 * TrendAnalyzer._wilders_ema(plus_dm, period) / np.where(atr > 0, atr, 1)
        minus_di = 100.0 * TrendAnalyzer._wilders_ema(minus_dm, period) / np.where(atr > 0, atr, 1)

        denom = plus_di + minus_di
        dx = 100.0 * np.abs(plus_di - minus_di) / np.where(denom > 0, denom, 1)
        adx = TrendAnalyzer._wilders_ema(dx, period)

        return float(adx[-1]) if not np.isnan(adx[-1]) else 0.0

    @staticmethod
    def _wilders_ema(series: np.ndarray, period: int) -> np.ndarray:
        """Wilder 平滑（EMA with alpha = 1/period）。"""
        result = np.zeros_like(series)
        result[0] = series[0]
        alpha = 1.0 / period
        for i in range(1, len(series)):
            result[i] = alpha * series[i] + (1 - alpha) * result[i - 1]
        return result

    def _adx_confidence(self, adx: float) -> float:
        """
        ADX → 置信度映射。

        ADX > 25  → 置信度 1.0（趋势明确）
        ADX 20-25 → 置信度 0.7（趋势中等）
        ADX < 20  → 置信度 0.4（震荡市，趋势信号不可靠）
        """
        strong = self.cfg.get("adx_strong", 25)
        moderate = self.cfg.get("adx_moderate", 20)

        if adx >= strong:
            return 1.0
        elif adx >= moderate:
            return 0.5 + 0.5 * (adx - moderate) / (strong - moderate)
        else:
            return max(0.2, 0.5 * adx / moderate)

    # ================================================================
    # EMA 工具
    # ================================================================

    @staticmethod
    def _ema(series: np.ndarray, period: int) -> np.ndarray:
        """指数移动平均。"""
        if len(series) == 0:
            return np.array([])
        result = np.zeros_like(series)
        result[0] = series[0]
        alpha = 2.0 / (period + 1)
        for i in range(1, len(series)):
            result[i] = alpha * series[i] + (1 - alpha) * result[i - 1]
        return result

    # ================================================================
    # 多周期共振计算
    # ================================================================

    def _compute_resonance(
        self,
        daily: Optional[Dict],
        weekly: Optional[Dict],
        monthly: Optional[Dict],
    ) -> Tuple[float, str]:
        """
        多周期共振评分。

        核心逻辑：
        - 月线看多 + 周线看多 + 日线看多 → 共振加成，最强信号
        - 月线看多 + 周线看多 + 日线回调 → 回调买入机会（中强信号）
        - 月线看多 + 周线看空 + 日线看多 → 反弹减仓（弱信号）
        - 月线看空 + … → 降级惩罚，视为逆大势反弹
        - 所有周期看空 → 共振加强看空

        月线权重 > 周线 > 日线，月线决定大方向。
        """
        ds = daily["score"] if daily else 0.0
        ws = weekly["score"] if weekly else None
        ms = monthly["score"] if monthly else None

        # --- 确定每个周期的方向 ---
        def _direction(score: Optional[float]) -> int:
            """返回 1(看多) / 0(中性) / -1(看空)。"""
            if score is None:
                return 0
            if score > 0.15:
                return 1
            if score < -0.15:
                return -1
            return 0

        d_dir = _direction(ds)
        w_dir = _direction(ws)
        m_dir = _direction(ms) if ms is not None else None

        # 计算加权基础分
        weights = []
        scores = []
        if ms is not None:
            weights.append(self.cfg.get("monthly_weight", 0.35))
            scores.append(ms)
        if ws is not None:
            weights.append(self.cfg.get("weekly_weight", 0.35))
            scores.append(ws)
        # daily always included
        weights.append(self.cfg.get("daily_weight", 0.30))
        scores.append(ds)
        total_w = sum(weights)
        base = sum(w * s for w, s in zip(weights, scores)) / total_w

        # --- 共振乘数 ---
        resonance = 1.0
        phase = "趋势不明"

        if m_dir is not None and w_dir is not None:
            # 三月度时间框架可用
            if m_dir == 1 and w_dir == 1 and d_dir == 1:
                resonance = 1.30
                phase = "多周期共振看多 ⬆⬆⬆"
            elif m_dir == 1 and w_dir == 1 and d_dir == 0:
                resonance = 1.10
                phase = "上升趋势中的日线整理"
            elif m_dir == 1 and w_dir == 1 and d_dir == -1:
                resonance = 0.85
                phase = "上升趋势中的日线回调（关注买点）"
            elif m_dir == 1 and w_dir == 0 and d_dir == 1:
                resonance = 0.70
                phase = "月线看多，日线反弹（等待周线确认）"
            elif m_dir == 1 and w_dir == -1 and d_dir == 1:
                resonance = 0.45
                phase = "⚠️ 月线看多但周线走弱，日线仅为反弹"
            elif m_dir == 1 and w_dir == -1 and d_dir == -1:
                resonance = 0.60
                phase = "月线看多，中短期调整中"
            elif m_dir == -1 and w_dir == 1 and d_dir == 1:
                resonance = 0.25
                phase = "⚠️ 月线空头下的短期反弹，不宜追高"
            elif m_dir == -1 and w_dir == -1 and d_dir == 1:
                resonance = 0.15
                phase = "⚠️⚠️ 逆大势反弹，大概率是下跌中继"
            elif m_dir == -1 and w_dir == -1 and d_dir == -1:
                resonance = 1.30
                phase = "多周期共振看空 ⬇⬇⬇"
            elif m_dir == -1 and w_dir == 1 and d_dir == -1:
                resonance = 0.40
                phase = "月线压制，周线反弹受阻"
            elif m_dir == 0 and w_dir == 1 and d_dir == 1:
                resonance = 0.85
                phase = "月线震荡，中短期走强"
            elif m_dir == 0 and w_dir == -1 and d_dir == -1:
                resonance = 0.85
                phase = "月线震荡，中短期走弱"
            else:
                resonance = 0.65
                phase = "多周期方向不一致，观望"
        elif w_dir is not None:
            # 只有日线 + 周线
            if w_dir == 1 and d_dir == 1:
                resonance = 1.20
                phase = "周线日线共振看多"
            elif w_dir == 1 and d_dir == -1:
                resonance = 0.70
                phase = "周线看多，日线回调（关注支撑）"
            elif w_dir == -1 and d_dir == 1:
                resonance = 0.40
                phase = "⚠️ 周线空头下的日线反弹"
            elif w_dir == -1 and d_dir == -1:
                resonance = 1.20
                phase = "周线日线共振看空"
            else:
                resonance = 0.65
                phase = "周线日线方向不一致"
        else:
            # 仅日线
            if d_dir == 1:
                resonance = 0.80
                phase = "日线趋势向上（缺少中长周期确认）"
            elif d_dir == -1:
                resonance = 0.80
                phase = "日线趋势向下（缺少中长周期确认）"
            else:
                resonance = 0.50
                phase = "日线趋势震荡"

        score = base * resonance
        score = max(-1.0, min(1.0, score))

        return score, phase


# ================================================================
# 独立评分函数（对接 SignalGenerator 的现有模式）
# ================================================================

def calculate_trend_score(result: Dict) -> Tuple[float, List[str]]:
    """
    从 TrendAnalyzer.analyze() 的结果中提取评分和信号。

    遵循与 calculate_technical_score / calculate_momentum_score 相同的签名约定。

    Args:
        result: TrendAnalyzer.analyze() 的返回字典

    Returns:
        (trend_score [-1, 1], signal_strings)
    """
    score = result.get("trend_score", 0.0)
    signals: List[str] = []

    phase = result.get("trend_phase", "")
    if phase:
        signals.append(phase)

    # 日线信号
    daily = result.get("daily")
    if daily:
        daily_score = daily.get("score", 0)
        if daily_score > 0.3:
            signals.append(f"日线趋势向上(score={daily_score:.2f})")
        elif daily_score < -0.3:
            signals.append(f"日线趋势向下(score={daily_score:.2f})")

    # 周线信号
    weekly = result.get("weekly")
    if weekly:
        weekly_score = weekly.get("score", 0)
        if weekly_score > 0.2:
            signals.append(f"周线趋势向上(score={weekly_score:.2f})")
        elif weekly_score < -0.2:
            signals.append(f"周线趋势向下(score={weekly_score:.2f})")

    # 月线信号
    monthly = result.get("monthly")
    if monthly:
        monthly_score = monthly.get("score", 0)
        if monthly_score > 0.15:
            signals.append(f"月线趋势向上(score={monthly_score:.2f})")
        elif monthly_score < -0.15:
            signals.append(f"月线趋势向下(score={monthly_score:.2f})")

    # 数据不足提醒
    tfs = result.get("available_timeframes", [])
    if "monthly" not in tfs and "weekly" not in tfs:
        signals.append("⚠️ 仅日线数据可用，趋势信号置信度较低")
    elif "monthly" not in tfs:
        signals.append("⚠️ 缺少月线数据，趋势信号置信度中等")

    return score, signals


# ================================================================
# 测试入口
# ================================================================

if __name__ == "__main__":
    import sys
    sys.path.insert(0, "..")

    from data.fetcher import ETFFetcher

    logging.basicConfig(level=logging.INFO)

    fetcher = ETFFetcher()

    # 测试 ETF
    print("=" * 60)
    print("测试 ETF 趋势分析")
    print("=" * 60)

    for symbol in ["512400", "159990"]:
        df = fetcher.get_etf_historical(symbol)
        if df.empty:
            print(f"{symbol}: 无数据")
            continue

        analyzer = TrendAnalyzer(df)
        result = analyzer.analyze()
        score, signals = calculate_trend_score(result)

        print(f"\n--- {symbol} ---")
        print(f"  趋势评分: {score:.4f}")
        print(f"  趋势阶段: {result['trend_phase']}")
        print(f"  可用周期: {result['available_timeframes']}")
        print(f"  日线: score={result['daily']['score']:.4f}, ADX={result['daily']['adx']:.1f}")
        if result["weekly"]:
            print(f"  周线: score={result['weekly']['score']:.4f}, ADX={result['weekly']['adx']:.1f}")
        if result["monthly"]:
            print(f"  月线: score={result['monthly']['score']:.4f}")
        print(f"  信号: {signals}")
