# -*- coding: utf-8 -*-
"""
配置文件
"""
import os

# 数据源配置
AKSHARE_TOKEN = os.getenv("AKSHARE_TOKEN", "")  # 可选，部分API需要

# ETF分析配置
ETF_CATEGORIES = {
    "有色金属": ["512400"],
    "化工": ["516020"],
    "稀有金属": ["562800"],
    "电气设备": ["159326"],
    "黄金股": ["512100"],
    # 可自行添加更多ETF代码
}

# 技术指标参数
TECHNICAL_PARAMS = {
    "ma_periods": [5, 10, 20, 60],           # 均线周期
    "macd_fast": 12,                          # MACD快线
    "macd_slow": 26,                          # MACD慢线
    "macd_signal": 9,                         # MACD信号线
    "rsi_period": 14,                         # RSI周期
    "boll_period": 20,                       # 布林带周期
    "boll_std": 2,                           # 布林带标准差倍数
}

# 动量因子参数
MOMENTUM_PARAMS = {
    "short_term": 5,         # 5日动量
    "mid_term": 20,          # 20日动量
    "long_term": 60,         # 60日动量
}

# 信号阈值
SIGNAL_THRESHOLDS = {
    "rsi_oversold": 30,      # RSI超卖
    "rsi_overbought": 70,    # RSI超买
    "macd_cross_up": True,   # MACD金叉
    "momentum_strong": 0.05, # 强动量阈值 (5%)
    "momentum_weak": -0.03,  # 弱动量阈值 (-3%)
}

# 飞书配置 (后续填入)
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "")
FEISHU_SECRET = os.getenv("FEISHU_SECRET", "")

# 数据存储
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DATA_DIR, "etf_data.db")

# 调度配置
FETCH_INTERVAL_MINUTES = 10
