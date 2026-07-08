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

# ============================================================
# 趋势分析参数（多周期共振系统 — 方案三）
# ============================================================
TREND_PARAMS = {
    # 多周期权重（用于共振计算）
    "monthly_weight": 0.35,
    "weekly_weight": 0.35,
    "daily_weight": 0.30,

    # 日线参数
    "daily": {
        "ma_periods": [5, 10, 20, 30, 60],
        # 判定多头排列的均线对（短 > 长）
        "alignment_pairs": [(5, 10), (10, 20), (20, 30), (30, 60)],
        # 哪些均线参与斜率判定
        "slope_periods": [20, 30],
        "slope_lookback": 5,        # 用最近 N 根 bar 计算斜率
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
        "adx_period": 14,
        "price_vs_ma": 20,          # 价格相对哪根均线判断位置
        "price_lookback": 20,       # 最近 N 天统计价格在均线上方的比例
        "min_bars": 60,             # 最少需要多少根日线 bar
    },

    # 周线参数（由日线 resample 得到）
    "weekly": {
        "ma_periods": [4, 8, 13, 26],    # ≈1月/2月/1季/半年
        "alignment_pairs": [(4, 8), (8, 13), (13, 26)],
        "slope_periods": [8, 13],
        "slope_lookback": 3,              # 最近 3 周
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
        "adx_period": 14,
        "price_vs_ma": 13,                # 13 周均线 ≈ 季度线
        "price_lookback": 13,
        "min_bars": 20,                   # 最少 20 根周线
    },

    # 月线参数（由日线 resample 得到）
    "monthly": {
        "ma_periods": [3, 6, 12],         # ≈1季/半年/1年
        "alignment_pairs": [(3, 6), (6, 12)],
        "slope_periods": [6, 12],
        "slope_lookback": 2,              # 最近 2 个月
        "macd_fast": 6,                   # 月线数据少，缩短 MACD 参数
        "macd_slow": 13,
        "macd_signal": 5,
        "adx_period": None,               # 月线不计算 ADX（数据太少）
        "price_vs_ma": 6,                 # 6 月均线 ≈ 半年线
        "price_lookback": 6,
        "min_bars": 12,                   # 最少 12 根月线
    },

    # ADX 趋势强度阈值（用于置信度乘数）
    "adx_strong": 25,        # ADX > 25 → 趋势明确
    "adx_moderate": 20,      # ADX 20-25 → 趋势中等

    # 回撤容忍参数
    "pullback_max_depth_pct": 5.0,    # 最大容忍跌破幅度（%）
    "pullback_max_days": 5,           # 最大容忍连续跌破天数
    "pullback_allowed_ratio": 0.25,   # 跌破天数占比容忍上限
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

# VIX-科技板块监控配置
VIX_MONITOR_CONFIG = {
    # VIX 波动阈值
    "vix_high_threshold": 30,       # VIX > 30 视为高波动/恐慌区
    "vix_extreme_threshold": 35,    # VIX > 35 视为极端恐慌
    "vix_spike_pct": 0.10,          # VIX 单日涨幅 > 10% 视为飙升

    # 科技持仓 ETF (用于构建科技板块等权指数)
    "tech_etfs": {
        "科创50": "588000",
        "科创芯片": "588290",
        "半导体": "512480",
        "电气设备": "159326",
    },

    # 科技持仓个股
    "tech_stocks": {
        "工业富联": "601138",
        "中材科技": "002080",
        "环旭电子": "601231",
    },

    # 滚动相关系数窗口 (交易日)
    "corr_windows": [20, 60],

    # OLS 回归窗口 (交易日)
    "beta_window": 60,

    # 信号阈值
    "signal_corr_threshold": -0.50,  # 20日相关系数 < -0.5 发出风险预警
    "signal_beta_threshold": -0.15,  # |beta| > 0.15 视为敏感
}

# 主板股票代码前缀（上海主板 + 深圳主板含原中小板）
MAIN_BOARD_PREFIXES = ["600", "601", "603", "605", "000", "001", "002", "003"]

# MA20 强势股扫描参数
MA20_SCAN_CONFIG = {
    "lookback_days": 22,      # 近一个月交易日数量
    "ma_period": 20,          # 20 日移动平均线
    "history_offset_days": 400,  # 获取历史数据的起始偏移（含节假日冗余）
                                  # 方案三需要月线数据 → 至少 12 个月 ≈ 400 天
    "min_history_days": 42,   # 至少需要的交易日数量
    "request_delay": 1.0,     # 请求间隔（秒），避免频繁访问数据接口

    # 因子计算周期
    "rsi_period": 14,              # RSI 周期
    "volatility_period": 20,       # 波动率周期
    "volume_price_period": 22,     # 量价配合周期，与 lookback_days 保持一致

    # 子因子权重（复合评分 = 加权平均）— 方案三加入趋势因子
    "weights": {
        "ma20": 0.30,              # MA20 强势程度
        "volume_price": 0.20,      # 量价配合
        "rsi": 0.15,               # RSI 健康度
        "volatility": 0.10,        # 波动率控制
        "trend": 0.25,             # 多周期趋势共振（新增）
    },

    # 阈值参数
    "rsi_optimal_center": 55,      # RSI 最优中心点
    "rsi_optimal_range": 45,       # 偏离中心多少开始降分
    "volatility_max": 0.60,        # 年化波动率 60% 时波动率得分为 0
    "volume_change_cap": 0.50,     # 成交量变化率上限 ±50%
}

# 信号生成器权重（四维度评分）— 方案三加入趋势维度
SIGNAL_WEIGHTS = {
    "technical": 0.30,    # 原 0.40
    "momentum": 0.25,     # 原 0.35
    "etf": 0.20,          # 原 0.25
    "trend": 0.25,        # 新增：多周期趋势共振
}

# 调度配置
FETCH_INTERVAL_MINUTES = 10
