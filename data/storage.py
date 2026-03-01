# -*- coding: utf-8 -*-
"""
数据存储模块 - SQLite本地存储
"""
import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Optional
import os
import logging

from config import DB_PATH

logger = logging.getLogger(__name__)


class ETFStorage:
    """ETF数据存储器"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # ETF历史行情表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etf_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                pct_change REAL,
                turnover REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date)
            )
        """)
        
        # ETF实时行情表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etf_realtime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE NOT NULL,
                name TEXT,
                price REAL,
                change REAL,
                pct_change REAL,
                volume REAL,
                amount REAL,
                high REAL,
                low REAL,
                open REAL,
                close_yesterday REAL,
                turnover REAL,
                update_time TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ETF分析结果表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etf_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                ma5 REAL,
                ma10 REAL,
                ma20 REAL,
                ma60 REAL,
                macd TEXT,
                macd_signal REAL,
                macd_hist REAL,
                rsi REAL,
                boll_upper REAL,
                boll_middle REAL,
                boll_lower REAL,
                momentum_5d REAL,
                momentum_20d REAL,
                momentum_60d REAL,
                signal TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, analysis_date)
            )
        """)
        
        # 买卖信号记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trading_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                signal_date TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                reason TEXT,
                strength REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("数据库初始化完成")
    
    def save_daily(self, df: pd.DataFrame) -> int:
        """保存日线数据"""
        if df.empty:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved = 0
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO etf_daily 
                    (symbol, date, open, high, low, close, volume, amount, pct_change, turnover)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get("symbol", ""),
                    row.get("date", "").strftime("%Y-%m-%d") if pd.notna(row.get("date")) else "",
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("close"),
                    row.get("volume"),
                    row.get("amount"),
                    row.get("pct_change"),
                    row.get("turnover")
                ))
                saved += 1
            except Exception as e:
                logger.error(f"保存数据失败: {e}")
        
        conn.commit()
        conn.close()
        logger.info(f"成功保存 {saved} 条日线数据")
        return saved
    
    def save_realtime(self, data: dict) -> bool:
        """保存实时行情"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO etf_realtime
                (symbol, name, price, change, pct_change, volume, amount, 
                 high, low, open, close_yesterday, turnover, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                data.get("symbol"),
                data.get("name"),
                data.get("price"),
                data.get("change"),
                data.get("pct_change"),
                data.get("volume"),
                data.get("amount"),
                data.get("high"),
                data.get("low"),
                data.get("open"),
                data.get("close_yesterday"),
                data.get("turnover")
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"保存实时数据失败: {e}")
            conn.close()
            return False
    
    def get_daily(
        self, 
        symbol: str, 
        days: int = 60,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取历史数据"""
        conn = sqlite3.connect(self.db_path)
        
        if start_date and end_date:
            query = f"""
                SELECT * FROM etf_daily 
                WHERE symbol = '{symbol}' 
                AND date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date
            """
        else:
            query = f"""
                SELECT * FROM etf_daily 
                WHERE symbol = '{symbol}' 
                ORDER BY date DESC LIMIT {days}
            """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        
        return df
    
    def get_realtime(self, symbol: str) -> dict:
        """获取最新实时行情"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM etf_realtime WHERE symbol = ?", (symbol,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {}
    
    def save_analysis(self, symbol: str, analysis: dict) -> bool:
        """保存分析结果"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO etf_analysis
                (symbol, analysis_date, ma5, ma10, ma20, ma60,
                 macd, macd_signal, macd_hist, rsi,
                 boll_upper, boll_middle, boll_lower,
                 momentum_5d, momentum_20d, momentum_60d, signal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                datetime.now().strftime("%Y-%m-%d"),
                analysis.get("ma5"),
                analysis.get("ma10"),
                analysis.get("ma20"),
                analysis.get("ma60"),
                analysis.get("macd"),
                analysis.get("macd_signal"),
                analysis.get("macd_hist"),
                analysis.get("rsi"),
                analysis.get("boll_upper"),
                analysis.get("boll_middle"),
                analysis.get("boll_lower"),
                analysis.get("momentum_5d"),
                analysis.get("momentum_20d"),
                analysis.get("momentum_60d"),
                analysis.get("signal")
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"保存分析结果失败: {e}")
            conn.close()
            return False
    
    def save_signal(self, symbol: str, signal_type: str, reason: str, strength: float = 0.5) -> bool:
        """保存交易信号"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO trading_signals (symbol, signal_date, signal_type, reason, strength)
                VALUES (?, ?, ?, ?, ?)
            """, (
                symbol,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                signal_type,
                reason,
                strength
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"保存信号失败: {e}")
            conn.close()
            return False
    
    def get_recent_signals(self, days: int = 7) -> pd.DataFrame:
        """获取近期信号"""
        conn = sqlite3.connect(self.db_path)
        query = f"""
            SELECT * FROM trading_signals 
            WHERE signal_date >= datetime('now', '-{days} days')
            ORDER BY signal_date DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df


if __name__ == "__main__":
    # 测试
    storage = ETFStorage()
    print("数据库测试完成")
