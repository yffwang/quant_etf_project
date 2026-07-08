# -*- coding: utf-8 -*-
"""
数据获取模块 - 使用Akshare获取ETF数据
Akshare是免费开源的财经数据接口，无需注册
"""
import akshare as ak
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import time
import re

logger = logging.getLogger(__name__)


def _to_float(value, default: float = 0.0) -> float:
    """安全转换为 float"""
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _standardize_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """将不同来源的OHLCV数据标准化为统一列名"""
    col_map = {
        # akshare ETF
        '日期': 'date', '开盘': 'open', '收盘': 'close',
        '最高': 'high', '最低': 'low', '成交量': 'volume',
        '成交额': 'amount', '涨跌幅': 'pct_change', '涨跌额': 'change',
        # akshare 个股
        '日期': 'date', '开盘': 'open', '收盘': 'close',
        '最高': 'high', '最低': 'low', '成交量': 'volume',
        '成交额': 'amount', '涨跌幅': 'pct_change', '涨跌额': 'change',
        # yfinance
        'Date': 'date', 'Open': 'open', 'Close': 'close',
        'High': 'high', 'Low': 'low', 'Volume': 'volume',
    }
    for old, new in col_map.items():
        if old in df.columns:
            df[new] = df[old]

    # 确保 yfinance 的 volume 是整数，但标准化为 numeric
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    df['symbol'] = symbol
    return df


def _extract_pure_code(code: str) -> str:
    """提取纯数字代码，去掉sz/sh前缀"""
    code = str(code).strip()
    if code.startswith('sz') or code.startswith('sh'):
        return code[2:]
    return code


class ETFFetcher:
    """ETF数据获取器"""

    def __init__(self):
        self.cache = {}

    def get_etf_list(self, prefixes: List[str] = None) -> pd.DataFrame:
        """
        获取A股场内ETF列表

        Args:
            prefixes: ETF代码前缀列表，默认为 ["51", "58", "15", "16"]
        """
        if prefixes is None:
            prefixes = ["51", "58", "15", "16"]

        try:
            df = ak.fund_etf_category_sina(symbol="ETF基金")

            if df.empty:
                logger.warning("未获取到ETF列表数据")
                return pd.DataFrame()

            df = df.rename(columns={
                '代码': 'code',
                '名称': 'name',
                '最新价': 'price',
                '涨跌额': 'change',
                '涨跌幅': 'pct_change',
                '昨收': 'close_yesterday',
                '今开': 'open',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount'
            })

            if prefixes and 'code' in df.columns:
                def extract_code(code):
                    code = str(code)
                    if code.startswith('sz') or code.startswith('sh'):
                        return code[2:]
                    return code
                df = df[df['code'].apply(
                    lambda x: any(extract_code(str(x)).startswith(p) for p in prefixes)
                )]

            logger.info(f"获取到 {len(df) if not df.empty else 0} 只ETF")
            return df
        except Exception as e:
            logger.error(f"获取ETF列表失败: {e}")
            return pd.DataFrame()

    def get_etf_historical(
        self,
        symbol: str,
        period: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取ETF历史数据

        Args:
            symbol: ETF代码 (如 "159990")
            period: 数据周期 ("daily", "weekly", "monthly")
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        pure_code = _extract_pure_code(symbol)
        cache_key = f"{pure_code}_{start_date}_{end_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            pure_code = _extract_pure_code(symbol)
            df = ak.fund_etf_hist_em(
                symbol=pure_code,
                period="daily" if period == "daily" else "weekly" if period == "weekly" else "monthly",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust="qfq"
            )

            if df.empty:
                logger.warning(f"获取ETF {symbol} 数据为空")
                return pd.DataFrame()

            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turn'
            })

            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pct_change', 'change']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df["symbol"] = symbol

            self.cache[cache_key] = df
            logger.info(f"获取ETF {symbol} 数据成功，共 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"获取ETF {symbol} 数据失败: {e}")
            return pd.DataFrame()

    def _map_sina_realtime_row(self, data: pd.Series, symbol: str) -> Dict:
        """将 Sina 行情行映射为统一实时数据格式"""
        return {
            "symbol": symbol,
            "name": data.get('名称', symbol),
            "price": _to_float(data.get('最新价'), 0),
            "change": _to_float(data.get('涨跌额'), 0),
            "volume": _to_float(data.get('成交量'), 0),
            "amount": _to_float(data.get('成交额'), 0),
            "high": _to_float(data.get('最高'), 0),
            "low": _to_float(data.get('最低'), 0),
            "open": _to_float(data.get('今开'), 0),
            "close_yesterday": _to_float(data.get('昨收'), 0),
            "turnover": _to_float(data.get('换手率'), 0),
            "pct_change": _to_float(data.get('涨跌幅'), 0),
        }

    def _get_realtime_from_minute(
        self,
        symbol: str,
        minute_fn,
        historical_fn
    ) -> Dict:
        """通过分钟线最新 bar + 昨日收盘计算实时行情"""
        try:
            pure_code = _extract_pure_code(symbol)
            today = datetime.now().strftime("%Y%m%d")
            df_min = minute_fn(symbol=pure_code, start_date=today, period="1")

            if df_min.empty or '收盘' not in df_min.columns:
                return {}

            df_min = df_min.sort_values('时间' if '时间' in df_min.columns else '日期')
            latest = df_min.iloc[-1]
            price = _to_float(latest.get('收盘'), 0)

            # 获取昨日收盘计算涨跌幅
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            hist_df = historical_fn(pure_code, start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"), end_date=yesterday)
            close_yesterday = 0
            if not hist_df.empty and 'close' in hist_df.columns:
                close_yesterday = _to_float(hist_df.iloc[-1]['close'], 0)
            elif not hist_df.empty and '收盘' in hist_df.columns:
                close_yesterday = _to_float(hist_df.iloc[-1]['收盘'], 0)

            change = price - close_yesterday if close_yesterday else 0
            pct_change = (change / close_yesterday * 100) if close_yesterday else 0

            return {
                "symbol": symbol,
                "name": symbol,
                "price": price,
                "change": change,
                "volume": _to_float(latest.get('成交量'), 0),
                "amount": _to_float(latest.get('成交额'), 0),
                "high": _to_float(latest.get('最高'), 0),
                "low": _to_float(latest.get('最低'), 0),
                "open": _to_float(latest.get('开盘'), 0),
                "close_yesterday": close_yesterday,
                "turnover": 0,
                "pct_change": pct_change,
            }
        except Exception as e:
            logger.debug(f"分钟线实时数据失败 {symbol}: {e}")
        return {}

    def get_etf_realtime(
        self,
        symbol: str,
        storage: Optional["ETFStorage"] = None
    ) -> Dict:
        """
        获取ETF实时行情，带多级 fallback 与缓存兜底
        """
        pure_code = _extract_pure_code(symbol)

        # 1. Sina ETF 行情
        try:
            df = ak.fund_etf_category_sina(symbol="ETF基金")
            if not df.empty and '代码' in df.columns:
                row = df[df['代码'].astype(str).str.strip() == pure_code]
                if not row.empty:
                    result = self._map_sina_realtime_row(row.iloc[0], pure_code)
                    if storage:
                        storage.save_realtime(result)
                    return result
        except Exception as e:
            logger.debug(f"Sina ETF 实时数据失败 {symbol}: {e}")

        # 2. Sina 全市场 A 股快照（包含 ETF）
        try:
            df = ak.stock_zh_a_spot()
            if not df.empty and '代码' in df.columns:
                row = df[df['代码'].astype(str).str.strip() == pure_code]
                if not row.empty:
                    result = self._map_sina_realtime_row(row.iloc[0], pure_code)
                    if storage:
                        storage.save_realtime(result)
                    return result
        except Exception as e:
            logger.debug(f"Sina A股快照实时数据失败 {symbol}: {e}")

        # 3. 分钟线最新 bar
        try:
            result = self._get_realtime_from_minute(
                symbol,
                ak.fund_etf_hist_min_em,
                self.get_etf_historical
            )
            if result:
                if storage:
                    storage.save_realtime(result)
                return result
        except Exception as e:
            logger.debug(f"ETF 分钟线实时数据失败 {symbol}: {e}")

        # 4. SQLite 缓存
        if storage:
            cached = storage.get_cached_realtime(pure_code, max_age_minutes=30)
            if cached:
                logger.warning(f"ETF {symbol} 网络全部失败，使用缓存数据")
                return cached

        logger.error(f"获取ETF {symbol} 实时数据全部失败")
        return {}

    def get_etfs_realtime_batch(
        self,
        symbols: List[str],
        storage: Optional["ETFStorage"] = None
    ) -> pd.DataFrame:
        """
        批量获取ETF实时数据
        """
        if not symbols:
            return pd.DataFrame()

        pure_symbols = [_extract_pure_code(s) for s in symbols]
        results = []
        found = set()

        # 1. 尝试 Sina ETF 分类行情批量获取
        try:
            df = ak.fund_etf_category_sina(symbol="ETF基金")
            if not df.empty and '代码' in df.columns:
                df['代码'] = df['代码'].astype(str).str.strip()
                df = df[df['代码'].isin(pure_symbols)]
                if not df.empty:
                    for _, row in df.iterrows():
                        code = row['代码']
                        found.add(code)
                        results.append(self._map_sina_realtime_row(row, code))
        except Exception as e:
            logger.debug(f"批量 Sina ETF 实时数据失败: {e}")

        # 2. 尝试 Sina 全市场快照批量获取
        missing = [s for s in pure_symbols if s not in found]
        if missing:
            try:
                df = ak.stock_zh_a_spot()
                if not df.empty and '代码' in df.columns:
                    df['代码'] = df['代码'].astype(str).str.strip()
                    df = df[df['代码'].isin(missing)]
                    if not df.empty:
                        for _, row in df.iterrows():
                            code = row['代码']
                            found.add(code)
                            results.append(self._map_sina_realtime_row(row, code))
            except Exception as e:
                logger.debug(f"批量 Sina A股快照实时数据失败: {e}")

        # 3. 逐个 fallback
        missing = [s for s in pure_symbols if s not in found]
        for code in missing:
            try:
                rt = self.get_etf_realtime(code, storage=storage)
                if rt:
                    results.append(rt)
                    found.add(code)
            except Exception as e:
                logger.debug(f"批量 fallback 获取 {code} 失败: {e}")

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        return df

    def _standardize_stock_historical(
        self,
        df: pd.DataFrame,
        symbol: str
    ) -> pd.DataFrame:
        """将个股历史数据标准化为统一列名和类型"""
        if df.empty:
            return df

        df = df.copy()

        # Sina 接口（stock_zh_a_daily）列名
        col_map = {
            'date': 'date', 'open': 'open', 'high': 'high',
            'low': 'low', 'close': 'close', 'volume': 'volume',
            'amount': 'amount', 'outstanding_share': 'outstanding_share',
            'turnover': 'turn',
        }

        for old, new in col_map.items():
            if old in df.columns:
                df[new] = df[old]

        if 'date' not in df.columns and '日期' in df.columns:
            df['date'] = df['日期']
        if 'close' not in df.columns and '收盘' in df.columns:
            df['close'] = df['收盘']

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date")

        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df["symbol"] = symbol
        return df

    def _add_bs_prefix(self, code: str) -> str:
        """根据代码前缀判断交易所并添加 sh/sz 前缀（不带点）"""
        code = _extract_pure_code(code)
        if code.startswith('6') or code.startswith('5'):
            return f"sh{code}"
        return f"sz{code}"

    def get_stock_market_cap(
        self,
        symbol: str,
        use_cache: bool = True
    ) -> float:
        """
        估算股票总市值（亿元）

        使用 AkShare 新浪接口的 outstanding_share（总股本）乘以最新收盘价。
        总股本在大部分场景下等价于全部股本，因此该乘积可作为总市值估算。

        Args:
            symbol: 股票代码（如 601138）
            use_cache: 是否使用内存缓存

        Returns:
            总市值（亿元），失败返回 0.0
        """
        pure_code = _extract_pure_code(symbol)
        cache_key = f"market_cap_{pure_code}"
        if use_cache and cache_key in self.cache:
            return self.cache[cache_key]

        try:
            bs_code = self._add_bs_prefix(pure_code)
            df = ak.stock_zh_a_daily(symbol=bs_code)
            if df.empty:
                return 0.0

            latest = df.iloc[-1]
            close = _to_float(latest.get("close"), 0)
            outstanding = _to_float(latest.get("outstanding_share"), 0)

            # outstanding_share 单位通常是股，乘以价格得到元，再转亿元
            market_cap = close * outstanding / 1e8

            if use_cache:
                self.cache[cache_key] = market_cap
            return market_cap

        except Exception as e:
            logger.debug(f"估算 {symbol} 市值失败: {e}")
            return 0.0

    def get_stock_is_tech(self, symbol: str, name: str = "") -> bool:
        """
        判断个股是否属于科技板块

        优先读取本地持久化的 data/tech_stocks.json 进行代码匹配；
        若本地列表不存在或找不到该代码，则回退到基于名称的关键词匹配。

        Args:
            symbol: 股票代码（如 "000021" 或 "sh000021"）
            name: 股票名称，作为关键词匹配回退使用

        Returns:
            是否为科技类股票
        """
        pure_code = _extract_pure_code(symbol)

        # 1. 优先查本地科技股票清单
        tech_stocks_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data",
            "tech_stocks.json"
        )
        try:
            if os.path.exists(tech_stocks_file):
                with open(tech_stocks_file, "r", encoding="utf-8") as f:
                    tech_stocks = json.load(f)
                tech_codes = {str(s.get("code", "")).strip() for s in tech_stocks}
                if pure_code in tech_codes:
                    return True
        except Exception as e:
            logger.debug(f"读取本地科技股票清单失败: {e}")

        # 2. 回退：按名称关键词匹配
        if name:
            return self.get_stock_industry_keyword_match(name)

        return False

    def get_stock_industry_keyword_match(
        self,
        name: str,
        keywords: Optional[List[str]] = None
    ) -> bool:
        """
        根据股票名称匹配科技关键词

        Args:
            name: 股票名称
            keywords: 关键词列表，默认读取 data/tech_keywords.json

        Returns:
            是否命中任意关键词
        """
        if not name:
            return False

        if keywords is None:
            keywords_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data",
                "tech_keywords.json"
            )
            try:
                with open(keywords_file, "r", encoding="utf-8") as f:
                    keywords = json.load(f)
            except Exception as e:
                logger.warning(f"读取科技关键词文件失败: {e}")
                return False

        name = str(name).lower()
        return any(str(kw).lower() in name for kw in keywords)

    def get_stock_historical(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取A股个股历史数据 (日K)

        优先使用 AkShare 的 Sina 接口（stock_zh_a_daily），相比东财接口
        在当前网络环境下更稳定。

        Args:
            symbol: 股票代码 (如 "601138")
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        pure_code = _extract_pure_code(symbol)
        cache_key = f"stock_{pure_code}_{start_date}_{end_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            # 使用 Sina 接口，需要 sh/sz 前缀
            bs_code = self._add_bs_prefix(pure_code)
            df = ak.stock_zh_a_daily(
                symbol=bs_code,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust="qfq"
            )

            if df.empty:
                logger.warning(f"获取个股 {symbol} 数据为空")
                return pd.DataFrame()

            df = self._standardize_stock_historical(df, pure_code)
            if df.empty:
                logger.warning(f"个股 {symbol} 数据标准化后为空")
                return pd.DataFrame()

            self.cache[cache_key] = df
            logger.info(f"获取个股 {symbol} 数据成功，共 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"获取个股 {symbol} 数据失败: {e}")
            return pd.DataFrame()

    def get_stock_realtime(
        self,
        symbol: str,
        storage: Optional["ETFStorage"] = None
    ) -> Dict:
        """
        获取A股个股实时行情，带多级 fallback 与缓存兜底
        """
        pure_code = _extract_pure_code(symbol)

        # 1. Sina 全市场 A 股快照
        try:
            df = ak.stock_zh_a_spot()
            if not df.empty and '代码' in df.columns:
                row = df[df['代码'].astype(str).str.strip() == pure_code]
                if not row.empty:
                    result = self._map_sina_realtime_row(row.iloc[0], pure_code)
                    if storage:
                        storage.save_realtime(result)
                    return result
        except Exception as e:
            logger.debug(f"Sina A股快照个股数据失败 {symbol}: {e}")

        # 2. 分钟线最新 bar
        try:
            result = self._get_realtime_from_minute(
                symbol,
                ak.stock_zh_a_hist_min_em,
                self.get_stock_historical
            )
            if result:
                if storage:
                    storage.save_realtime(result)
                return result
        except Exception as e:
            logger.debug(f"个股分钟线实时数据失败 {symbol}: {e}")

        # 3. SQLite 缓存
        if storage:
            cached = storage.get_cached_realtime(pure_code, max_age_minutes=30)
            if cached:
                logger.warning(f"个股 {symbol} 网络全部失败，使用缓存数据")
                return cached

        logger.error(f"获取个股 {symbol} 实时数据全部失败")
        return {}

    def get_stock_main_board_list(self, prefixes: List[str] = None) -> pd.DataFrame:
        """
        获取沪深主板股票列表

        Args:
            prefixes: 主板代码前缀列表，默认使用 config.MAIN_BOARD_PREFIXES

        Returns:
            DataFrame 包含 code, name 列
        """
        if prefixes is None:
            from config import MAIN_BOARD_PREFIXES
            prefixes = MAIN_BOARD_PREFIXES

        try:
            df = ak.stock_info_a_code_name()
            if df.empty:
                logger.warning("未获取到A股代码列表")
                return pd.DataFrame()

            df = df.rename(columns={
                'code': 'code',
                'name': 'name'
            })

            # 适配不同列名
            if 'code' not in df.columns and '代码' in df.columns:
                df = df.rename(columns={'代码': 'code'})
            if 'name' not in df.columns and '名称' in df.columns:
                df = df.rename(columns={'名称': 'name'})

            pattern = re.compile(r'^(' + '|'.join(prefixes) + r')')
            df = df[df['code'].astype(str).str.match(pattern)]

            logger.info(f"获取主板股票列表，共 {len(df)} 只")
            return df[['code', 'name']].drop_duplicates().reset_index(drop=True)

        except Exception as e:
            logger.error(f"获取主板股票列表失败: {e}")
            return pd.DataFrame()

    def get_vix_daily(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取VIX指数日度历史数据 (使用 yfinance)

        注意: VIX 是美股数据，美东时间收盘对应北京时间次日凌晨。
        因此 VIX(t) 应该对齐到 A股(t+1)。
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        cache_key = f"vix_{start_date}_{end_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            import yfinance as yf
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            # 配置带重试的 session，降低限流概率
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })

            vix = yf.Ticker("^VIX", session=session)
            df = vix.history(start=start_date, end=end_date, auto_adjust=False)

            if df.empty:
                logger.warning("获取VIX数据为空")
                return pd.DataFrame()

            # 重置时区并标准化日期
            df.index = df.index.tz_localize(None).normalize()
            df = df.reset_index().rename(columns={"index": "date", "Date": "date"})
            df = df.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume"
            })
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            df["symbol"] = "VIX"

            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            self.cache[cache_key] = df
            logger.info(f"获取VIX数据成功，共 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"获取VIX数据失败: {e}")
            return pd.DataFrame()

    def get_etf_info(self, symbol: str) -> Dict:
        """
        获取ETF基本信息
        """
        try:
            df = ak.fund_etf_category_sina(symbol="ETF基金")
            if df.empty:
                return {}

            row = df[df['代码'] == symbol]
            if row.empty:
                return {}

            data = row.iloc[0]
            return {
                "code": data.get('代码', ''),
                "name": data.get('证券简称', ''),
                "net_asset_value": data.get('单位净值', ''),
                "accumulated_net_value": data.get('累计净值', ''),
                "change_pct": data.get('涨跌幅', ''),
                "date": data.get('净值日期', '')
            }
        except Exception as e:
            logger.error(f"获取ETF {symbol} 信息失败: {e}")
        return {}

    def get_all_etf_performance(self, days: int = 5) -> pd.DataFrame:
        """
        获取所有ETF的涨跌幅数据

        Args:
            days: 统计天数

        Returns:
            DataFrame包含所有ETF的涨跌幅数据
        """
        try:
            etf_list = self.get_etf_list()
            if etf_list.empty:
                return pd.DataFrame()

            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")

            all_data = []
            total_etfs = len(etf_list)
            processed_count = 0

            for idx, row in etf_list.iterrows():
                code = row.get('code', '')
                name = row.get('name', '')
                if not code:
                    continue

                try:
                    pure_code = _extract_pure_code(code)
                    df = ak.fund_etf_hist_em(
                        symbol=pure_code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )

                    if df.empty or len(df) < days:
                        continue

                    df = df.sort_values('日期', ascending=False).head(days)

                    current_price = float(df.iloc[0]['收盘'])
                    start_price = float(df.iloc[-1]['收盘'])

                    if start_price > 0:
                        total_change = (current_price - start_price) / start_price * 100

                        max_daily_change = 0
                        for i in range(len(df) - 1):
                            prev_price = float(df.iloc[i + 1]['收盘'])
                            curr_price = float(df.iloc[i]['收盘'])
                            if prev_price > 0:
                                change = abs(curr_price - prev_price) / prev_price * 100
                                max_daily_change = max(max_daily_change, change)

                        all_data.append({
                            'code': code,
                            'name': name,
                            'current_price': current_price,
                            'start_price': start_price,
                            f'{days}_day_change': total_change,
                            'max_daily_change': max_daily_change,
                            'data_points': len(df)
                        })

                        processed_count += 1
                        if processed_count % 50 == 0:
                            logger.info(f"进度: [{processed_count}/{total_etfs}]")

                except Exception as e:
                    logger.debug(f"处理{code}失败: {e}")
                    continue

            result_df = pd.DataFrame(all_data)
            logger.info(f"成功获取 {len(result_df)} 只ETF的涨跌幅数据")
            return result_df

        except Exception as e:
            logger.error(f"获取ETF涨跌幅数据失败: {e}")
            return pd.DataFrame()


def get_all_etf_symbols(prefixes: List[str] = None) -> List[str]:
    """
    获取所有A股场内ETF代码

    Args:
        prefixes: ETF代码前缀列表，默认为 ["51", "58", "15", "16"]
                  51: 上海ETF
                  58: 上海ETF
                  15: 深圳ETF
                  16: 深圳ETF
    """
    if prefixes is None:
        prefixes = ["51", "58", "15", "16"]

    fetcher = ETFFetcher()
    df = fetcher.get_etf_list()
    if not df.empty and 'code' in df.columns:
        codes = df['code'].tolist()

        def extract_code(code):
            code = str(code)
            if code.startswith('sz') or code.startswith('sh'):
                return code[2:]
            return code

        if prefixes:
            codes = [extract_code(c) for c in codes if any(extract_code(str(c)).startswith(p) for p in prefixes)]

        return codes
    return []


if __name__ == "__main__":
    fetcher = ETFFetcher()

    df = fetcher.get_etf_historical("159990")
    print(f"159990 数据条数: {len(df)}")
    if not df.empty:
        print(df.tail())
