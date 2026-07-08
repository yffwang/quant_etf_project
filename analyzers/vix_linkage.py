# -*- coding: utf-8 -*-
"""
VIX-A股科技板块关联分析模块
核心逻辑: 使用 T-1 的 VIX 数据对齐 T 日的 A股数据
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


def align_vix_a_share(
    vix_df: pd.DataFrame,
    a_share_df: pd.DataFrame,
    vix_shift: int = 1
) -> pd.DataFrame:
    """
    对齐 VIX 与 A股数据

    由于 VIX 是美股数据(美东时间收盘 = 北京时间次日凌晨),
    VIX(t-1) 的信息在 A股(t) 开盘前就已经确定。
    因此将 VIX 向后平移 vix_shift 个交易日，使其与次日的 A股对齐。

    Args:
        vix_df: VIX 日度数据，需包含 'date', 'close' 列
        a_share_df: A股日度数据，需包含 'date', 'close' 列
        vix_shift: VIX 向后平移的交易日数，默认为 1 (T-1 对齐 T)

    Returns:
        合并后的 DataFrame，包含共同交易日
    """
    if vix_df.empty or a_share_df.empty:
        logger.warning("VIX或A股数据为空，无法对齐")
        return pd.DataFrame()

    vix = vix_df[["date", "close"]].copy()
    vix = vix.rename(columns={"close": "vix_close"})
    vix["date"] = pd.to_datetime(vix["date"]).dt.normalize()

    # 兼容价格序列 或 收益率序列
    if "tech_return" in a_share_df.columns:
        a_share = a_share_df[["date", "tech_return"]].copy()
        a_share = a_share.rename(columns={"tech_return": "a_close"})
    else:
        a_share = a_share_df[["date", "close"]].copy()
        a_share = a_share.rename(columns={"close": "a_close"})
    a_share["date"] = pd.to_datetime(a_share["date"]).dt.normalize()

    # VIX 向后平移 (T-1 的 VIX 对齐 T 的 A股)
    vix = vix.sort_values("date").reset_index(drop=True)
    vix["vix_close_shifted"] = vix["vix_close"].shift(vix_shift)
    vix = vix.dropna(subset=["vix_close_shifted"])

    # 取交集 (只保留 A股和 VIX 都有的交易日)
    merged = pd.merge(a_share, vix[["date", "vix_close", "vix_close_shifted"]], on="date", how="inner")
    merged = merged.sort_values("date").reset_index(drop=True)

    logger.info(f"对齐后共同交易日: {len(merged)} 天")
    return merged


def build_tech_basket_return(
    etf_dfs: Dict[str, pd.DataFrame],
    stock_dfs: Dict[str, pd.DataFrame],
    min_history: int = 60
) -> pd.DataFrame:
    """
    构建科技板块等权收益率指数

    注意: 由于个股和ETF价格差异大，不能直接平均价格。
    正确做法是每天计算每只标的的日收益率，然后取等权平均。

    Args:
        etf_dfs: ETF 历史数据字典 {名称: DataFrame}
        stock_dfs: 个股历史数据字典 {名称: DataFrame}
        min_history: 每只标的最少需要的有效数据天数

    Returns:
        DataFrame 包含 date 和 tech_return (等权平均日收益率)
    """
    returns_list = []
    valid_names = []

    all_dfs = {**etf_dfs, **stock_dfs}

    for name, df in all_dfs.items():
        if df.empty or len(df) < min_history:
            logger.warning(f"{name} 数据不足 {min_history} 天，跳过")
            continue

        df = df[["date", "close"]].copy()
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        df[name + "_return"] = df["close"].pct_change()
        df = df.dropna(subset=[name + "_return"])

        if len(df) < min_history // 2:
            logger.warning(f"{name} 有效收益率数据不足，跳过")
            continue

        returns_list.append(df[["date", name + "_return"]])
        valid_names.append(name)

    if not returns_list:
        logger.error("没有有效的科技板块标的收益率数据")
        return pd.DataFrame()

    # 逐个 merge
    merged = returns_list[0]
    for r in returns_list[1:]:
        merged = pd.merge(merged, r, on="date", how="inner")

    return_cols = [c for c in merged.columns if c.endswith("_return")]
    merged["tech_return"] = merged[return_cols].mean(axis=1)

    # 从收益率序列反推一个"价格"序列，便于后续统一做 pct_change
    merged["close"] = (1 + merged["tech_return"]).cumprod()

    logger.info(f"科技板块等权指数构建完成，包含 {len(valid_names)} 只标的: {valid_names}")
    return merged[["date", "close"]]


def rolling_correlation(
    aligned_df: pd.DataFrame,
    window: int = 20,
    method: str = "pearson"
) -> pd.DataFrame:
    """
    计算滚动相关系数

    Args:
        aligned_df: align_vix_a_share 的输出，包含 vix_close_shifted 和 a_close 或 tech_return
        window: 滚动窗口
        method: pearson 或 spearman

    Returns:
        带 rolling_corr 列的 DataFrame
    """
    df = aligned_df.copy()

    # 如果输入是 tech_return 格式
    if "tech_return" in df.columns:
        vix_ret = df["vix_close_shifted"].pct_change()
        target_ret = df["tech_return"]
    else:
        vix_ret = df["vix_close_shifted"].pct_change()
        target_ret = df["a_close"].pct_change()

    df["vix_return"] = vix_ret
    df["target_return"] = target_ret

    if method == "spearman":
        # 手动计算滚动 spearman (pandas 原生不支持直接 rolling().corr(method='spearman'))
        def _spearman(x, y):
            return pd.Series(x).rank().corr(pd.Series(y).rank())
        corr_vals = []
        for i in range(len(df)):
            if i < window - 1:
                corr_vals.append(np.nan)
            else:
                x = df["vix_return"].iloc[i - window + 1:i + 1]
                y = df["target_return"].iloc[i - window + 1:i + 1]
                if x.std() == 0 or y.std() == 0:
                    corr_vals.append(np.nan)
                else:
                    try:
                        corr_vals.append(_spearman(x.values, y.values))
                    except Exception:
                        corr_vals.append(np.nan)
        df["rolling_corr"] = corr_vals
    else:
        df["rolling_corr"] = df["vix_return"].rolling(window=window).corr(df["target_return"])

    return df


def rolling_beta(
    aligned_df: pd.DataFrame,
    window: int = 60
) -> pd.DataFrame:
    """
    计算滚动 OLS 回归 beta

    模型: target_return_t = alpha + beta * vix_return_{t-1} + epsilon
    beta 含义: VIX 每上涨 1%，科技板块平均变化 beta%
    通常 beta 为负，且绝对值越大说明敏感度越高。

    Args:
        aligned_df: align_vix_a_share 的输出
        window: 回归窗口

    Returns:
        带 rolling_beta, rolling_alpha, rolling_r2 列的 DataFrame
    """
    df = aligned_df.copy()

    if "tech_return" in df.columns:
        y = df["tech_return"]
    else:
        y = df["a_close"].pct_change()

    x = df["vix_close_shifted"].pct_change()

    df["target_return"] = y
    df["vix_return"] = x

    betas = []
    alphas = []
    r2s = []

    for i in range(len(df)):
        if i < window - 1:
            betas.append(np.nan)
            alphas.append(np.nan)
            r2s.append(np.nan)
            continue

        x_win = x.iloc[i - window + 1:i + 1].dropna()
        y_win = y.iloc[i - window + 1:i + 1].dropna()

        # 对齐索引
        common_idx = x_win.index.intersection(y_win.index)
        if len(common_idx) < window // 2:
            betas.append(np.nan)
            alphas.append(np.nan)
            r2s.append(np.nan)
            continue

        x_arr = x_win.loc[common_idx].values.astype(float)
        y_arr = y_win.loc[common_idx].values.astype(float)

        if np.std(x_arr) == 0:
            betas.append(np.nan)
            alphas.append(np.nan)
            r2s.append(np.nan)
            continue

        # 简单最小二乘: beta = cov(x,y) / var(x)
        beta = np.cov(x_arr, y_arr, ddof=1)[0, 1] / np.var(x_arr, ddof=1)
        alpha = np.mean(y_arr) - beta * np.mean(x_arr)

        # R2
        y_pred = alpha + beta * x_arr
        ss_res = np.sum((y_arr - y_pred) ** 2)
        ss_tot = np.sum((y_arr - np.mean(y_arr)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot != 0 else np.nan

        betas.append(beta)
        alphas.append(alpha)
        r2s.append(r2)

    df["rolling_beta"] = betas
    df["rolling_alpha"] = alphas
    df["rolling_r2"] = r2s

    return df


def calculate_latest_metrics(
    aligned_df: pd.DataFrame,
    corr_window: int = 20,
    beta_window: int = 60
) -> Dict:
    """
    计算最新的关联指标摘要

    Returns:
        dict 包含 latest_vix, vix_change_pct, corr_20, beta_60, r2_60 等
    """
    df = aligned_df.copy()
    if df.empty:
        return {}

    df = rolling_correlation(df, window=corr_window, method="pearson")
    df = rolling_beta(df, window=beta_window)

    latest = df.iloc[-1]

    def _to_py(x):
        if pd.isna(x):
            return None
        if isinstance(x, (np.floating, np.integer)):
            return float(x)
        return x

    metrics = {
        "date": _to_py(latest.get("date")),
        "latest_vix": round(_to_py(latest.get("vix_close_shifted")), 2) if pd.notna(latest.get("vix_close_shifted")) else None,
        "vix_level": _vix_level(latest.get("vix_close_shifted")),
        "vix_change_pct": round(_to_py(df["vix_close_shifted"].pct_change().iloc[-1]) * 100, 2) if len(df) > 1 else 0,
        f"corr_{corr_window}d": round(_to_py(latest.get("rolling_corr")), 3) if pd.notna(latest.get("rolling_corr")) else None,
        f"beta_{beta_window}d": round(_to_py(latest.get("rolling_beta")), 3) if pd.notna(latest.get("rolling_beta")) else None,
        f"r2_{beta_window}d": round(_to_py(latest.get("rolling_r2")), 3) if pd.notna(latest.get("rolling_r2")) else None,
        "history_start": df["date"].min().strftime("%Y-%m-%d") if not df.empty else None,
        "history_days": int(len(df)),
    }

    return metrics


def _vix_level(vix_value: float) -> str:
    """VIX 水平分级"""
    if pd.isna(vix_value):
        return "未知"
    if vix_value >= 35:
        return "极端恐慌"
    elif vix_value >= 30:
        return "高波动/恐慌"
    elif vix_value >= 20:
        return "正常波动"
    else:
        return "低波动/平静"


def get_vix_spike_history(
    vix_df: pd.DataFrame,
    spike_threshold: float = 0.10
) -> pd.DataFrame:
    """
    获取 VIX 飙升历史记录

    Args:
        vix_df: VIX 原始数据
        spike_threshold: 日涨幅阈值 (默认 10%)

    Returns:
        飙升日期列表
    """
    df = vix_df.copy()
    df["vix_return"] = df["close"].pct_change()
    spikes = df[df["vix_return"] > spike_threshold].copy()
    spikes["vix_return_pct"] = spikes["vix_return"] * 100
    return spikes[["date", "close", "vix_return_pct"]].sort_values("date", ascending=False)


if __name__ == "__main__":
    import sys
    sys.path.append("..")
    from data.fetcher import ETFFetcher
    from config import VIX_MONITOR_CONFIG

    fetcher = ETFFetcher()

    # 获取 VIX
    vix = fetcher.get_vix_daily(start_date="2024-01-01")
    print(f"VIX 数据: {len(vix)} 条")

    # 获取一个科技 ETF
    etf = fetcher.get_etf_historical("588000", start_date="2024-01-01")
    print(f"ETF 数据: {len(etf)} 条")

    if not vix.empty and not etf.empty:
        aligned = align_vix_a_share(vix, etf)
        print(f"对齐后: {len(aligned)} 天")

        metrics = calculate_latest_metrics(aligned, corr_window=20, beta_window=60)
        print("最新指标:", metrics)
