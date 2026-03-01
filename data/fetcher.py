# -*- coding: utf-8 -*-
"""
数据获取模块 - 使用AkShare获取ETF数据
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class ETFFetcher:
    """ETF数据获取器"""
    
    def __init__(self):
        self.cache = {}
    
    def get_etf_list(self) -> pd.DataFrame:
        """
        获取A股所有场内ETF列表
        """
        try:
            # 获取ETF基金列表
            df = ak.fund_etf_hist_sina()
            logger.info(f"获取到 {len(df)} 只ETF")
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
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        
        cache_key = f"{symbol}_{start_date}_{end_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # 尝试使用ETF日K线数据接口
            df = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if df is not None and not df.empty:
                df = df.rename(columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                    "成交额": "amount",
                    "振幅": "amplitude",
                    "涨跌幅": "pct_change",
                    "涨跌额": "change",
                    "换手率": "turnover"
                })
                df["symbol"] = symbol
                df["date"] = pd.to_datetime(df["date"])
                
                self.cache[cache_key] = df
                logger.info(f"获取ETF {symbol} 数据成功，共 {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取ETF {symbol} 数据失败: {e}")
            return pd.DataFrame()
    
    def get_etf_realtime(self, symbol: str) -> Dict:
        """
        获取ETF实时行情
        """
        try:
            df = ak.fund_etf_spot_em()
            etf_row = df[df["代码"] == symbol]
            if not etf_row.empty:
                row = etf_row.iloc[0]
                return {
                    "symbol": symbol,
                    "name": row.get("名称", ""),
                    "price": row.get("最新价", 0),
                    "change": row.get("涨跌幅", 0),
                    "volume": row.get("成交量", 0),
                    "amount": row.get("成交额", 0),
                    "amplitude": row.get("振幅", 0),
                    "high": row.get("最高", 0),
                    "low": row.get("最低", 0),
                    "open": row.get("今开", 0),
                    "close_yesterday": row.get("昨收", 0),
                    "turnover": row.get("换手率", 0),
                }
        except Exception as e:
            logger.error(f"获取ETF {symbol} 实时数据失败: {e}")
        return {}
    
    def get_etfs_realtime_batch(self, symbols: List[str]) -> pd.DataFrame:
        """
        批量获取ETF实时行情
        """
        try:
            df = ak.fund_etf_spot_em()
            df = df[df["代码"].isin(symbols)]
            return df
        except Exception as e:
            logger.error(f"批量获取ETF实时数据失败: {e}")
            return pd.DataFrame()
    
    def get_etf_info(self, symbol: str) -> Dict:
        """
        获取ETF基本信息（规模、溢价率等）
        """
        try:
            df = ak.fund_etf_info_em(symbol=symbol)
            if df is not None and not df.empty:
                info = {}
                for _, row in df.iterrows():
                    info[row.get("item", "")] = row.get("value", "")
                return info
        except Exception as e:
            logger.error(f"获取ETF {symbol} 信息失败: {e}")
        return {}


def get_all_etf_symbols() -> List[str]:
    """
    获取所有A股场内ETF代码
    """
    fetcher = ETFFetcher()
    df = fetcher.get_etf_list()
    if not df.empty and "代码" in df.columns:
        return df["代码"].tolist()
    return []


if __name__ == "__main__":
    # 测试
    fetcher = ETFFetcher()
    
    # 测试获取历史数据
    df = fetcher.get_etf_historical("159990")
    print(f"159990 数据条数: {len(df)}")
    if not df.empty:
        print(df.tail())
