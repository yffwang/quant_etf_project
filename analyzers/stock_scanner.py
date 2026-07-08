# -*- coding: utf-8 -*-
"""
A股主板强势股扫描器
基于日线 MA20 筛选近一个月未跌破 20 日线的强势股票
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from config import MA20_SCAN_CONFIG
from analyzers.technical import TechnicalAnalyzer
from analyzers.momentum import MomentumAnalyzer

logger = logging.getLogger(__name__)

DEFAULT_REPORT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports"
)

DEFAULT_MAIN_BOARD_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "main_board_stocks.json"
)

DEFAULT_TECH_KEYWORDS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "tech_keywords.json"
)

DEFAULT_TECH_STOCKS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "tech_stocks.json"
)


class MA20Scanner:
    """MA20 主板强势股扫描器"""

    def __init__(self, fetcher, storage=None):
        """
        Args:
            fetcher: ETFFetcher 实例
            storage: ETFStorage 实例（可选，用于保存结果）
        """
        self.fetcher = fetcher
        self.storage = storage
        self.cfg = MA20_SCAN_CONFIG
        self._is_tech_scan = False

    def _safe_fetch_historical(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """带重试的个股历史数据获取"""
        try:
            df = self.fetcher.get_stock_historical(code, start_date=start_date, end_date=end_date)
            if not df.empty and len(df) >= self.cfg["min_history_days"]:
                return df
            logger.debug(f"{code} 历史数据不足: {len(df) if not df.empty else 0} 条")
        except Exception as e:
            logger.debug(f"获取 {code} 历史数据失败: {e}")
        return pd.DataFrame()

    def _load_main_board_codes(self, filepath: Optional[str] = None) -> pd.DataFrame:
        """从本地 JSON 文件加载主板股票代码列表"""
        path = filepath or DEFAULT_MAIN_BOARD_FILE

        if not os.path.exists(path):
            logger.warning(f"本地主板代码文件不存在: {path}，尝试从网络获取")
            return self.fetcher.get_stock_main_board_list()

        try:
            with open(path, "r", encoding="utf-8") as f:
                records = json.load(f)

            if not records:
                logger.warning("本地主板代码文件为空")
                return pd.DataFrame()

            df = pd.DataFrame(records)
            if "code" not in df.columns:
                logger.error("本地主板代码文件缺少 'code' 列")
                return pd.DataFrame()

            logger.info(f"从本地加载主板股票列表，共 {len(df)} 只")
            return df[["code", "name"]].drop_duplicates().reset_index(drop=True)

        except Exception as e:
            logger.error(f"读取本地主板代码文件失败: {e}")
            return self.fetcher.get_stock_main_board_list()

    def _load_tech_stocks(self, filepath: Optional[str] = None) -> pd.DataFrame:
        """从本地 JSON 文件加载科技股票代码列表"""
        path = filepath or DEFAULT_TECH_STOCKS_FILE

        if not os.path.exists(path):
            logger.error(f"本地科技股票列表不存在: {path}")
            return pd.DataFrame()

        try:
            with open(path, "r", encoding="utf-8") as f:
                records = json.load(f)

            if not records:
                logger.warning("本地科技股票列表为空")
                return pd.DataFrame()

            df = pd.DataFrame(records)
            if "code" not in df.columns:
                logger.error("本地科技股票列表缺少 'code' 列")
                return pd.DataFrame()

            logger.info(f"从本地加载科技股票列表，共 {len(df)} 只")
            return df[["code", "name"]].drop_duplicates().reset_index(drop=True)

        except Exception as e:
            logger.error(f"读取本地科技股票列表失败: {e}")
            return pd.DataFrame()

    def _analyze_stock(self, code: str, name: str) -> Optional[Dict]:
        """分析单只股票的 MA20 强势程度，并附加市值与科技标签"""
        today = datetime.now()
        start_date = (today - timedelta(days=self.cfg["history_offset_days"])).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        df = self._safe_fetch_historical(code, start_date, end_date)
        if df.empty:
            return None

        df = df.sort_values("date").reset_index(drop=True)
        df["ma20"] = df["close"].rolling(window=self.cfg["ma_period"]).mean()
        df = df.dropna(subset=["ma20"])

        if len(df) < self.cfg["lookback_days"]:
            return None

        recent = df.tail(self.cfg["lookback_days"]).copy()
        recent["above_ma20"] = recent["close"] >= recent["ma20"]
        recent["distance_pct"] = (recent["close"] - recent["ma20"]) / recent["ma20"] * 100

        latest = recent.iloc[-1]

        # 计算 MA20 强势指标
        ma20_strength = self._calculate_ma20_strength(recent)

        # 计算复合因子的子得分
        volume_price_score, volume_price_corr, volume_change = self._calculate_volume_price_score(df, recent)
        rsi_score, latest_rsi = self._calculate_rsi_score(df)
        volatility_score, volatility_20d = self._calculate_volatility_score(df)
        composite_score = self._calculate_composite_score(
            ma20_strength, volume_price_score, rsi_score, volatility_score
        )

        # 估算总市值
        market_cap = self.fetcher.get_stock_market_cap(code)

        # 是否属于科技板块（优先查本地 tech_stocks.json）
        is_tech = self.fetcher.get_stock_is_tech(code, name)

        return {
            "code": code,
            "name": name,
            "market_cap": round(market_cap, 2),
            "is_tech": is_tech,
            "latest_close": round(latest["close"], 2),
            "latest_ma20": round(latest["ma20"], 2),
            "days_above_ma20": int(recent["above_ma20"].sum()),
            "max_distance_above_ma20_pct": round(recent["distance_pct"].max(), 2),
            "min_distance_above_ma20_pct": round(recent["distance_pct"].min(), 2),
            "avg_distance_above_ma20_pct": round(recent["distance_pct"].mean(), 2),
            "latest_distance_above_ma20_pct": round(recent.iloc[-1]["distance_pct"], 2),
            "ma20_strength_score": round(ma20_strength, 4),
            "last_breach_date": self._get_last_breach_date(df, recent),
            "latest_date": latest["date"].strftime("%Y-%m-%d"),
            # 新增因子字段
            "rsi": round(latest_rsi, 2),
            "volume_price_corr": round(volume_price_corr, 4),
            "volume_change_22d": round(volume_change, 4),
            "volatility_20d": round(volatility_20d, 4),
            "volume_price_score": round(volume_price_score, 4),
            "rsi_score": round(rsi_score, 4),
            "volatility_score": round(volatility_score, 4),
            "composite_score": round(composite_score, 4),
        }

    def _calculate_ma20_strength(self, recent: pd.DataFrame) -> float:
        """
        计算 MA20 强势程度得分

        综合以下因素：
        - 收盘价持续在 MA20 上方的天数占比
        - 收盘价相对 MA20 的平均偏离度
        - 最近收盘价相对 MA20 的偏离度
        - 偏离度的稳定性（标准差越小越稳定）

        返回一个标准化后的得分，范围约 [0, 1]，越大表示越强势
        """
        total_days = len(recent)
        above_ratio = recent["above_ma20"].sum() / total_days

        avg_dist = recent["distance_pct"].mean()
        latest_dist = recent.iloc[-1]["distance_pct"]
        std_dist = recent["distance_pct"].std()

        # 正向因子：上方天数占比、平均偏离、最新偏离
        # 负向因子：波动过大
        if pd.isna(std_dist) or std_dist == 0:
            stability_score = 1.0
        else:
            # 平均偏离在 0~10% 之间较合理，标准差越小越稳定
            stability_score = max(0.0, 1.0 - std_dist / 5.0)

        score = (
            above_ratio * 0.35
            + min(avg_dist / 10.0, 1.0) * 0.25
            + min(latest_dist / 10.0, 1.0) * 0.25
            + stability_score * 0.15
        )

        return max(0.0, min(score, 1.0))

    def _calculate_volume_price_score(
        self,
        df: pd.DataFrame,
        recent: pd.DataFrame
    ) -> tuple[float, float, float]:
        """
        计算量价配合得分

        复用 MomentumAnalyzer.calculate_volume_momentum，综合：
        - volume_price_corr: 近 N 天收盘价与成交量的相关系数
        - volume_change: 近 N 天成交量变化率

        偏好价涨量增、量价同向；不奖励缩量或价量背离。

        Returns:
            (volume_price_score, volume_price_corr, volume_change)
        """
        period = self.cfg.get("volume_price_period", 22)
        cap = self.cfg.get("volume_change_cap", 0.50)

        ma_df = MomentumAnalyzer(df).calculate_volume_momentum(period=period)
        latest = ma_df.iloc[-1]

        volume_price_corr = float(latest.get("volume_price_corr", 0) or 0)
        volume_change = float(latest.get("volume_change", 0) or 0)

        if pd.isna(volume_price_corr):
            volume_price_corr = 0.0
        if pd.isna(volume_change):
            volume_change = 0.0

        corr_score = max(0.0, volume_price_corr)  # 只奖励正相关，范围 [0, 1]
        vol_change_norm = min(max(volume_change, -cap), cap)
        vol_change_score = (vol_change_norm + cap) / (2 * cap)  # 映射到 [0, 1]

        volume_price_score = corr_score * 0.6 + vol_change_score * 0.4
        volume_price_score = max(0.0, min(volume_price_score, 1.0))

        return volume_price_score, volume_price_corr, volume_change

    def _calculate_rsi_score(self, df: pd.DataFrame) -> tuple[float, float]:
        """
        计算 RSI 健康度得分

        复用 TechnicalAnalyzer.calculate_rsi，以 rsi_optimal_center 为最优中心，
        偏离越多得分越低，避免超买和超卖。

        Returns:
            (rsi_score, latest_rsi)
        """
        period = self.cfg.get("rsi_period", 14)
        center = self.cfg.get("rsi_optimal_center", 55)
        range_width = self.cfg.get("rsi_optimal_range", 45)

        rsi_df = TechnicalAnalyzer(df).calculate_rsi(period=period)
        latest_rsi = float(rsi_df.iloc[-1].get("rsi", center))

        if pd.isna(latest_rsi):
            latest_rsi = center

        if range_width <= 0:
            rsi_score = 1.0 if latest_rsi == center else 0.0
        else:
            rsi_score = max(0.0, 1.0 - abs(latest_rsi - center) / range_width)

        return rsi_score, latest_rsi

    def _calculate_volatility_score(self, df: pd.DataFrame) -> tuple[float, float]:
        """
        计算波动率控制得分

        复用 MomentumAnalyzer.calculate_volatility，取 20 日年化波动率。
        波动率越高得分越低，偏好走势稳定的标的。

        Returns:
            (volatility_score, volatility_20d)
        """
        period = self.cfg.get("volatility_period", 20)
        max_volatility = self.cfg.get("volatility_max", 0.60)

        vol_df = MomentumAnalyzer(df).calculate_volatility(periods=[period])
        volatility_20d = float(vol_df.iloc[-1].get(f"volatility_{period}d", 0) or 0)

        if pd.isna(volatility_20d):
            volatility_20d = 0.0

        if max_volatility <= 0:
            volatility_score = 1.0
        else:
            volatility_score = max(0.0, 1.0 - volatility_20d / max_volatility)

        return volatility_score, volatility_20d

    def _calculate_composite_score(
        self,
        ma20_score: float,
        volume_price_score: float,
        rsi_score: float,
        volatility_score: float
    ) -> float:
        """
        计算复合评分

        按 config 中 weights 加权，范围 [0, 1]。
        """
        weights = self.cfg.get("weights", {})
        composite = (
            ma20_score * weights.get("ma20", 0.40)
            + volume_price_score * weights.get("volume_price", 0.25)
            + rsi_score * weights.get("rsi", 0.20)
            + volatility_score * weights.get("volatility", 0.15)
        )
        return max(0.0, min(composite, 1.0))

    def _get_last_breach_date(
        self,
        df: pd.DataFrame,
        recent: pd.DataFrame
    ) -> str:
        """获取历史区间内最近一次跌破 MA20 的日期"""
        # df 已经包含计算好的 MA20，直接复用
        df = df.dropna(subset=["ma20"])
        breach_dates = df[df["close"] < df["ma20"]]["date"]

        if breach_dates.empty:
            return "从未跌破（观测期内）"
        return breach_dates.iloc[-1].strftime("%Y-%m-%d")

    def scan(
        self,
        max_stocks: Optional[int] = None,
        min_ma20_strength: Optional[float] = None,
        codes_file: Optional[str] = None,
        include_all: bool = True,
        min_market_cap: Optional[float] = None,
        tech_only: bool = False,
        use_tech_list: bool = False,
    ) -> pd.DataFrame:
        """
        扫描主板股票的 MA20 强势程度

        Args:
            max_stocks: 最大扫描股票数量（仅用于测试），默认全部
            min_ma20_strength: 最小 MA20 强势得分过滤阈值
            codes_file: 本地股票代码文件路径
                - 默认主板: data/main_board_stocks.json
                - use_tech_list=True 时: data/tech_stocks.json
            include_all: 是否包含所有可分析股票（默认 True）
            min_market_cap: 最小总市值（亿元），默认不过滤
            tech_only: 是否只保留科技板块股票，默认 False
            use_tech_list: 是否直接扫描本地科技股列表，默认 False

        Returns:
            分析结果 DataFrame，按复合得分降序排列
        """
        logger.info("=" * 60)

        if use_tech_list:
            logger.info("开始扫描本地科技股票列表 MA20 强势程度")
            self._is_tech_scan = True
            board_df = self._load_tech_stocks(codes_file)
        else:
            logger.info("开始扫描主板 MA20 强势程度")
            self._is_tech_scan = False
            board_df = self._load_main_board_codes(codes_file)

        if min_market_cap:
            logger.info(f"市值过滤: 总市值 >= {min_market_cap} 亿元")
        if tech_only and not use_tech_list:
            logger.info("板块过滤: 仅保留科技相关股票")

        if board_df.empty:
            logger.error("未能获取股票列表")
            return pd.DataFrame()

        if max_stocks is not None:
            board_df = board_df.head(max_stocks)

        results: List[Dict] = []
        total = len(board_df)

        for idx, row in board_df.iterrows():
            code = str(row.get("code", "")).strip()
            name = str(row.get("name", "")).strip()
            if not code:
                continue

            try:
                result = self._analyze_stock(code, name)
                if not result:
                    continue

                # 市值过滤（科技股列表模式也计算市值，但不默认过滤）
                if min_market_cap is not None and result["market_cap"] < min_market_cap:
                    continue

                # 科技板块过滤（仅主板扫描模式生效）
                if tech_only and not use_tech_list and not result["is_tech"]:
                    continue

                # 强势得分过滤
                if min_ma20_strength is not None and result["ma20_strength_score"] < min_ma20_strength:
                    continue

                results.append(result)
                logger.info(
                    f"[{idx + 1}/{total}] {code} {name} "
                    f"市值:{result['market_cap']:.0f}亿 "
                    f"科技:{result['is_tech']} "
                    f"复合得分:{result['composite_score']:.4f} "
                    f"MA20得分:{result['ma20_strength_score']:.4f}"
                )
            except Exception as e:
                logger.debug(f"分析 {code} 失败: {e}")

            if (idx + 1) % 50 == 0:
                logger.info(f"扫描进度: [{idx + 1}/{total}]，已分析成功 {len(results)} 只")

            time.sleep(self.cfg["request_delay"])

        if not results:
            logger.info("未找到符合条件的股票")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)

        logger.info(f"扫描完成，共分析 {len(df)} 只股票")
        logger.info("=" * 60)
        return df

    def save_report_to_md(
        self,
        df: pd.DataFrame,
        output_dir: Optional[str] = None,
        top_n: Optional[int] = None
    ) -> str:
        """
        将扫描报告保存为 Markdown 文件

        Args:
            df: 扫描结果 DataFrame
            output_dir: 输出目录，默认 reports/
            top_n: Markdown 报告中仅显示前 N 条，默认显示全部

        Returns:
            保存的文件路径
        """
        output_dir = output_dir or DEFAULT_REPORT_DIR
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ma20_scan_report_{timestamp}.md"
        filepath = os.path.join(output_dir, filename)

        report = self.format_report(df, top_n=top_n)

        # 根据扫描模式选择标题和范围说明
        if getattr(self, "_is_tech_scan", False):
            title = "沪深主板科技股票 MA20 强势程度扫描报告"
            scope = "本地科技股列表（data/tech_stocks.json）"
        else:
            title = "主板 MA20 强势程度扫描报告"
            scope = "沪深主板（600/601/603/605/000/001/002/003）"

        # 增加 Markdown 标题和元信息
        md_content = f"""# {title}

- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **扫描范围**: {scope}
- **分析股票总数**: {len(df) if not df.empty else 0} 只
- **观察窗口**: 近 {self.cfg['lookback_days']} 个交易日
- **评分体系**: 复合得分 = MA20强势×{self.cfg['weights']['ma20']:.0%} + 量价配合×{self.cfg['weights']['volume_price']:.0%} + RSI健康×{self.cfg['weights']['rsi']:.0%} + 波动率控制×{self.cfg['weights']['volatility']:.0%}

{report}
"""

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(md_content)

        # 同时保存完整 CSV，方便进一步分析
        csv_filename = f"ma20_scan_report_{timestamp}.csv"
        csv_filepath = os.path.join(output_dir, csv_filename)
        df.to_csv(csv_filepath, index=False, encoding="utf-8-sig")
        logger.info(f"CSV 已保存到: {csv_filepath}")

        logger.info(f"Markdown 报告已保存到: {filepath}")
        return filepath

    def format_report(self, df: pd.DataFrame, top_n: Optional[int] = None) -> str:
        """
        格式化扫描报告

        Args:
            df: 扫描结果 DataFrame
            top_n: 仅显示前 N 条，默认显示全部
        """
        if df.empty:
            return "未找到符合条件的股票。"

        display_df = df.head(top_n) if top_n is not None else df

        if getattr(self, "_is_tech_scan", False):
            title = "📊 沪深主板科技股票 MA20 强势程度扫描报告"
            scope = "本地科技股列表（data/tech_stocks.json）"
        else:
            title = "📊 主板 MA20 强势程度扫描报告"
            scope = "沪深主板（600/601/603/605/000/001/002/003）"

        lines = [
            "",
            "=" * 179,
            title,
            "=" * 179,
            f"统计日期: {datetime.now().strftime('%Y-%m-%d')}",
            f"扫描范围: {scope}",
            f"强势标准: 近 {self.cfg['lookback_days']} 个交易日，综合 MA20 + 量价 + RSI + 波动率",
            f"评分体系: 复合得分 = MA20强势×{self.cfg['weights']['ma20']:.0%} + 量价配合×{self.cfg['weights']['volume_price']:.0%} + RSI健康×{self.cfg['weights']['rsi']:.0%} + 波动率控制×{self.cfg['weights']['volatility']:.0%}",
            f"分析股票总数: {len(df)} 只" + (f"，本表显示前 {top_n} 只" if top_n else ""),
            "",
            f"{'排名':<4} {'代码':<8} {'名称':<10} {'总市值(亿)':<10} {'科技':<5} {'最新价':<8} {'MA20':<8} {'复合得分':<10} {'MA20得分':<10} {'量价得分':<10} {'RSI':<6} {'RSI得分':<8} {'波动率':<8} {'波动率得分':<10} {'成交量变化%':<12} {'上方天数':<8} {'最新偏离%':<10} {'平均偏离%':<10} {'最大偏离%':<10} {'上次跌破':<14}",
            "-" * 179,
        ]

        for i, (_, row) in enumerate(display_df.iterrows(), 1):
            lines.append(
                f"{i:<4} {row['code']:<8} {row['name'][:10]:<10} "
                f"{row['market_cap']:<10.2f} {str(row['is_tech']):<5} "
                f"{row['latest_close']:<8.2f} {row['latest_ma20']:<8.2f} "
                f"{row['composite_score']:<10.4f} {row['ma20_strength_score']:<10.4f} "
                f"{row['volume_price_score']:<10.4f} {row['rsi']:<6.2f} {row['rsi_score']:<8.4f} "
                f"{row['volatility_20d']:<8.4f} {row['volatility_score']:<10.4f} "
                f"{row['volume_change_22d']:<12.2%} {row['days_above_ma20']:<8} "
                f"{row['latest_distance_above_ma20_pct']:<10.2f} {row['avg_distance_above_ma20_pct']:<10.2f} "
                f"{row['max_distance_above_ma20_pct']:<10.2f} {str(row['last_breach_date']):<14}"
            )

        lines.extend([
            "-" * 179,
            "⚠️ 风险提示：本扫描结果仅基于技术指标，不构成投资建议。",
            "=" * 179,
        ])

        return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from data.fetcher import ETFFetcher
    from data.storage import ETFStorage

    logging.basicConfig(level=logging.INFO)
    scanner = MA20Scanner(ETFFetcher(), ETFStorage())
    result_df = scanner.scan(max_stocks=30)
    print(scanner.format_report(result_df))
