# -*- coding: utf-8 -*-
"""
数据获取模块 - 使用Baostock获取ETF数据
Baostock是免费开源的A股数据接口，无需注册
"""
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


def _format_bs_code(symbol: str) -> str:
    """
    格式化Baostock证券代码
    规则:
    - 159xxx -> 深圳ETF -> sz.159990
    - 510xxx -> 上海ETF -> sh.510xxx
    - 511xxx -> 上海ETF -> sh.511xxx
    - 512xxx -> 上海ETF -> sh.512xxx
    - 515xxx -> 上海ETF -> sh.515xxx
    - 516xxx -> 上海ETF -> sh.516xxx
    - 5开头 -> 上海
    - 1/15/16开头 -> 深圳
    """
    symbol = symbol.strip()

    if '.' in symbol:
        return symbol

    if symbol.startswith('5'):
        return f"sh.{symbol}"
    else:
        return f"sz.{symbol}"


class ETFFetcher:
    """ETF数据获取器"""

    def __init__(self):
        self.cache = {}
        self._init_baostock()

    def _init_baostock(self):
        """初始化Baostock"""
        try:
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"Baostock登录失败: {lg.error_msg}")
            else:
                logger.info("Baostock初始化成功")
        except Exception as e:
            logger.error(f"Baostock初始化失败: {e}")

    def get_etf_list(self, prefixes: List[str] = None) -> pd.DataFrame:
        """
        获取A股场内ETF列表

        Args:
            prefixes: ETF代码前缀列表，默认为 ["51", "58", "15", "16"]
        """
        if prefixes is None:
            prefixes = ["51", "58", "15", "16"]

        try:
            rs = bs.query_stock_basic()
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            df = pd.DataFrame(data_list, columns=rs.fields)
            if not df.empty and 'type' in df.columns:
                df = df[df['type'] == '5']

                if prefixes and 'code' in df.columns:
                    df = df[df['code'].apply(
                        lambda x: any(x.split('.')[-1].startswith(p) for p in prefixes)
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
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        cache_key = f"{symbol}_{start_date}_{end_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            bs_code = _format_bs_code(symbol)
            fields = "date,open,high,low,close,volume,amount,turn"

            rs = bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=period[0],
                adjustflag="2"
            )

            if rs.error_code != '0':
                logger.error(f"获取ETF {symbol} 数据失败: {rs.error_msg}")
                return pd.DataFrame()

            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())

            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)

                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")

                df["open"] = pd.to_numeric(df["open"], errors='coerce')
                df["high"] = pd.to_numeric(df["high"], errors='coerce')
                df["low"] = pd.to_numeric(df["low"], errors='coerce')
                df["close"] = pd.to_numeric(df["close"], errors='coerce')
                df["volume"] = pd.to_numeric(df["volume"], errors='coerce')
                df["amount"] = pd.to_numeric(df["amount"], errors='coerce')
                df["turn"] = pd.to_numeric(df["turn"], errors='coerce')

                df["symbol"] = symbol

                if not df.empty:
                    df["pct_change"] = df["close"].pct_change() * 100
                    df["change"] = df["close"].diff()

                self.cache[cache_key] = df
                logger.info(f"获取ETF {symbol} 数据成功，共 {len(df)} 条")
            return df if not df.empty else pd.DataFrame()

        except Exception as e:
            logger.error(f"获取ETF {symbol} 数据失败: {e}")
            return pd.DataFrame()

    def get_etf_realtime(self, symbol: str) -> Dict:
        """
        获取ETF实时行情
        注意: Baostock不提供实时行情，返回最近交易日数据
        """
        try:
            bs_code = _format_bs_code(symbol)
            end_date = datetime.now().strftime("%Y-%m-%d")

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,turn,preclose",
                start_date=end_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )

            if rs.error_code != '0' or not rs.next():
                return {}

            data = rs.get_row_data()

            return {
                "symbol": symbol,
                "name": symbol,
                "price": float(data[4]) if data[4] else 0,
                "change": float(data[4]) - float(data[8]) if data[4] and data[8] else 0,
                "volume": float(data[5]) if data[5] else 0,
                "amount": float(data[6]) if data[6] else 0,
                "high": float(data[2]) if data[2] else 0,
                "low": float(data[3]) if data[3] else 0,
                "open": float(data[1]) if data[1] else 0,
                "close_yesterday": float(data[8]) if data[8] else 0,
                "turnover": float(data[7]) if data[7] else 0,
            }
        except Exception as e:
            logger.error(f"获取ETF {symbol} 实时数据失败: {e}")
        return {}

    def get_etfs_realtime_batch(self, symbols: List[str]) -> pd.DataFrame:
        """
        批量获取ETF数据
        """
        result_dfs = []
        for symbol in symbols:
            df = self.get_etf_historical(symbol, start_date=datetime.now().strftime("%Y-%m-%d"))
            if not df.empty:
                result_dfs.append(df)

        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        return pd.DataFrame()

    def get_etf_info(self, symbol: str) -> Dict:
        """
        获取ETF基本信息
        """
        try:
            rs = bs.query_stock_basic(code_sh=f"sh.{symbol}") or bs.query_stock_basic(code_sz=f"sz.{symbol}")
            if rs and rs.next():
                data = rs.get_row_data()
                return {
                    "code": data[0],
                    "name": data[1],
                    "ipoDate": data[2],
                    "outDate": data[3],
                    "type": data[4],
                    "status": data[5],
                }
        except Exception as e:
            logger.error(f"获取ETF {symbol} 信息失败: {e}")
        return {}

    def __del__(self):
        """退出登录"""
        try:
            bs.logout()
        except:
            pass

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

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")

            all_data = []

            for _, row in etf_list.iterrows():
                code = row.get('code', '')
                name = row.get('name', '')
                if not code:
                    continue

                try:
                    bs_code = code
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,close",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="2"
                    )

                    if rs.error_code != '0':
                        continue

                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())

                    if not data_list:
                        continue

                    df = pd.DataFrame(data_list, columns=rs.fields)
                    df = df[df['close'].notna()]

                    if len(df) < days:
                        continue

                    df = df.sort_values('date', ascending=False).head(days)

                    current_price = float(df.iloc[0]['close'])
                    start_price = float(df.iloc[-1]['close'])

                    if start_price > 0:
                        total_change = (current_price - start_price) / start_price * 100

                        max_daily_change = 0
                        for i in range(len(df) - 1):
                            prev_price = float(df.iloc[i + 1]['close'])
                            curr_price = float(df.iloc[i]['close'])
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
        symbols = [c.split('.')[-1] for c in codes]

        if prefixes:
            symbols = [s for s in symbols if any(s.startswith(p) for p in prefixes)]

        return symbols
    return []


if __name__ == "__main__":
    fetcher = ETFFetcher()

    df = fetcher.get_etf_historical("159990")
    print(f"159990 数据条数: {len(df)}")
    if not df.empty:
        print(df.tail())
