#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于证监会行业分类扩充本地科技股列表

扫描沪深主板所有股票，依据证监会行业分类标准（2012），将以下股票
补充进 data/tech_stocks.json：
  - C 类制造业中，排除食品、钢铁、白酒、纺织、服装、皮革、烟草等
    明显非科技大类后的剩余股票
  - I 类信息传输、软件和信息技术服务业
  - M73 研究和试验发展

过滤掉的制造业大类：
  - C13 农副食品加工业
  - C14 食品制造业
  - C15 酒、饮料和精制茶制造业
  - C16 烟草制品业
  - C17 纺织业
  - C18 纺织服装、服饰业
  - C19 皮革、毛皮、羽毛及其制品和制鞋业
  - C31 黑色金属冶炼和压延加工业

实现说明：
  - 上交所主板股票列表直接来自上交所官网接口，包含 CSRC_CODE 字段。
  - 深交所主板股票列表来自 AkShare 的 stock_info_sz_name_code，含
    "所属行业"字段（即证监会门类）。
  - 对于 M 类股票，会进一步调用巨潮资讯接口确认细分行业；若接口无
    数据或失败，默认保留该股票（宁可多不要少）。
  - 申万行业分类接口在当前环境下因 SSL 证书问题不可用，故未采用；
    如后续 AkShare 修复，可再扩展。

运行方式：
    source venv/bin/activate
    python scripts/expand_tech_stocks_by_industry.py

选项：
    --dry-run      只输出统计信息，不写入文件
    --keep-all-m   不进一步过滤 M 类，保留所有 M 类股票
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import akshare as ak
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
TECH_STOCKS_FILE = os.path.join(DATA_DIR, "tech_stocks.json")

logger = logging.getLogger(__name__)

# 证监会门类代码
CSRC_MANUFACTURING = "C"
CSRC_IT = "I"
CSRC_SCIENCE_TECH = "M"
CSRC_RD_MAJOR = "研究和试验发展"

# 制造业中需要排除的非科技大类（证监会 2012 行业分类）
EXCLUDED_MANUFACTURING_MAJORS = {
    "农副食品加工业",
    "食品制造业",
    "酒、饮料和精制茶制造业",
    "烟草制品业",
    "纺织业",
    "纺织服装、服饰业",
    "皮革、毛皮、羽毛及其制品和制鞋业",
    "黑色金属冶炼和压延加工业",
}

# 深交所主板代码前缀
SZ_MAIN_BOARD_PREFIXES = ("000", "001", "002", "003")

# 上交所主板接口配置
SH_MAIN_BOARD_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SH_MAIN_BOARD_PARAMS = {
    "STOCK_TYPE": "1",
    "REG_PROVINCE": "",
    "CSRC_CODE": "",
    "STOCK_CODE": "",
    "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
    "COMPANY_STATUS": "2,4,5,7,8",
    "type": "inParams",
    "isPagination": "true",
    "pageHelp.cacheSize": "1",
    "pageHelp.beginPage": "1",
    "pageHelp.pageSize": "10000",
    "pageHelp.pageNo": "1",
    "pageHelp.endPage": "1",
}
SH_MAIN_BOARD_HEADERS = {
    "Host": "query.sse.com.cn",
    "Pragma": "no-cache",
    "Referer": "https://www.sse.com.cn/assortment/stock/list/share/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
    ),
}


def load_json(filepath: str) -> List[Dict]:
    """加载 JSON 文件，不存在时返回空列表"""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(filepath: str, data: List[Dict], indent: int = 2):
    """保存 JSON 文件"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)


def fetch_sh_main_board() -> pd.DataFrame:
    """
    获取上交所主板股票列表及证监会门类

    Returns:
        DataFrame 包含 code, name, csrc_code, csrc_name, exchange
    """
    try:
        r = requests.get(
            SH_MAIN_BOARD_URL,
            params=SH_MAIN_BOARD_PARAMS,
            headers=SH_MAIN_BOARD_HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if "result" not in data or not data["result"]:
            logger.warning("上交所主板接口返回为空")
            return pd.DataFrame()

        df = pd.DataFrame(data["result"])
        df = df.rename(
            columns={
                "A_STOCK_CODE": "code",
                "SEC_NAME_CN": "name",
                "CSRC_CODE": "csrc_code",
                "CSRC_CODE_DESC": "csrc_name",
            }
        )
        df["code"] = df["code"].astype(str).str.strip()
        df["csrc_code"] = (
            df["csrc_code"].astype(str).str.strip().str.upper()
        )
        df["csrc_name"] = df["csrc_name"].astype(str).str.strip()
        df["exchange"] = "SH"
        return df[["code", "name", "csrc_code", "csrc_name", "exchange"]].copy()

    except Exception as e:
        logger.error(f"获取上交所主板列表失败: {e}")
        return pd.DataFrame()


def fetch_sz_main_board() -> pd.DataFrame:
    """
    获取深交所主板股票列表及证监会门类

    Returns:
        DataFrame 包含 code, name, csrc_code, csrc_name, exchange
    """
    try:
        df = ak.stock_info_sz_name_code()
        df = df.rename(
            columns={
                "A股代码": "code",
                "A股简称": "name",
                "所属行业": "csrc_desc",
            }
        )
        df["code"] = df["code"].astype(str).str.strip()

        # 仅保留主板（过滤创业板 300/301/302 等）
        df = df[df["code"].str.startswith(SZ_MAIN_BOARD_PREFIXES)].copy()

        # 所属行业格式如 "C 制造业" / "I 信息技术"
        df["csrc_code"] = (
            df["csrc_desc"].astype(str).str.split().str[0].str.upper()
        )
        df["csrc_name"] = (
            df["csrc_desc"].astype(str).str.split(n=1).str[1]
        )
        df["exchange"] = "SZ"
        return df[["code", "name", "csrc_code", "csrc_name", "exchange"]].copy()

    except Exception as e:
        logger.error(f"获取深交所主板列表失败: {e}")
        return pd.DataFrame()


def fetch_stock_csrc_major(code: str, max_retries: int = 3, delay: float = 0.3) -> Optional[str]:
    """
    查询单只股票的证监会行业大类。

    Args:
        code: 股票代码
        max_retries: 最大重试次数
        delay: 重试间隔基数

    Returns:
        行业大类字符串；失败或无数据时返回 None。
    """
    end_date = datetime.now().strftime("%Y%m%d")
    for attempt in range(max_retries):
        try:
            df = ak.stock_industry_change_cninfo(
                symbol=code, start_date="19900101", end_date=end_date
            )
            if df.empty:
                return None

            csrc = df[df["分类标准编码"] == "008021"]
            if csrc.empty:
                return None

            latest = csrc.sort_values("变更日期").iloc[-1]
            major = str(latest.get("行业大类", "")).strip()
            return major if major else None

        except Exception as e:
            logger.debug(
                f"查询 {code} 行业大类失败（第 {attempt + 1}/{max_retries} 次）: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))

    return None


def filter_manufacturing_stocks(
    c_df: pd.DataFrame,
    excluded_majors: Set[str],
    request_delay: float = 0.15,
) -> pd.DataFrame:
    """
    对 C 类制造业股票按行业大类进一步过滤，排除传统行业。

    说明：上交所、深交所接口只返回证监会门类，无法直接拿到大类。
    这里通过巨潮资讯接口逐个查询行业大类。该接口内部使用
    py_mini_racer，多线程不安全，因此采用串行查询。

    Args:
        c_df: C 类制造业股票 DataFrame
        excluded_majors: 需要排除的行业大类集合
        request_delay: 请求间隔（秒）

    Returns:
        过滤后的制造业股票 DataFrame
    """
    if c_df.empty:
        return c_df

    codes = c_df["code"].tolist()
    major_map: Dict[str, Optional[str]] = {}

    for i, code in enumerate(codes, 1):
        major_map[code] = fetch_stock_csrc_major(code)
        if i % 20 == 0 or i == len(codes):
            logger.info(f"制造业大类查询进度: {i}/{len(codes)}")
        time.sleep(request_delay)

    def _keep(row) -> bool:
        code = row["code"]
        major = major_map.get(code)
        if not major:
            logger.debug(f"{code} {row['name']} 未查询到行业大类，按宁可多不要少保留")
            return True
        if major in excluded_majors:
            logger.debug(f"{code} {row['name']} 属于 {major}，已排除")
            return False
        return True

    return c_df[c_df.apply(_keep, axis=1)].copy()


def is_rd_stock(code: str, max_retries: int = 3, delay: float = 0.2) -> Optional[bool]:
    """
    查询巨潮资讯接口，判断股票是否属于 M73 研究和试验发展。

    Args:
        code: 股票代码
        max_retries: 最大重试次数
        delay: 重试间隔基数

    Returns:
        True 表示是 M73；False 表示不是；None 表示查询失败或无数据。
    """
    end_date = datetime.now().strftime("%Y%m%d")
    for attempt in range(max_retries):
        try:
            df = ak.stock_industry_change_cninfo(
                symbol=code, start_date="19900101", end_date=end_date
            )
            if df.empty:
                return None

            csrc = df[df["分类标准编码"] == "008021"]
            if csrc.empty:
                return None

            latest = csrc.sort_values("变更日期").iloc[-1]
            major = str(latest.get("行业大类", "")).strip()
            return major == CSRC_RD_MAJOR

        except Exception as e:
            logger.debug(
                f"查询 {code} 细分行业失败（第 {attempt + 1}/{max_retries} 次）: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))

    return None


def filter_m73_stocks(
    m_df: pd.DataFrame,
    keep_all_m_on_failure: bool = True,
    request_delay: float = 0.15,
) -> pd.DataFrame:
    """
    对 M 类股票进一步过滤，仅保留 M73 研究和试验发展。

    说明：巨潮资讯接口内部使用 py_mini_racer 计算签名，多线程下该
    JS 引擎初始化存在竞争问题，故采用串行查询。M 类主板股票通常只
    有几十只，串行速度可接受。

    Args:
        m_df: M 类股票 DataFrame
        keep_all_m_on_failure: 接口失败时是否保留该股票（宁可多不要少）
        request_delay: 请求间隔（秒）

    Returns:
        过滤后的 M73 股票 DataFrame
    """
    if m_df.empty:
        return m_df

    codes = m_df["code"].tolist()
    results: Dict[str, Optional[bool]] = {}

    for i, code in enumerate(codes, 1):
        results[code] = is_rd_stock(code)
        if i % 10 == 0 or i == len(codes):
            logger.info(f"M 类细分查询进度: {i}/{len(codes)}")
        time.sleep(request_delay)

    def _keep(row) -> bool:
        code = row["code"]
        is_rd = results.get(code)
        if is_rd is True:
            return True
        if is_rd is False:
            logger.debug(f"{code} {row['name']} 非 M73，已排除")
            return False
        # 查询失败或无数据：按配置保留或排除
        if keep_all_m_on_failure:
            logger.debug(f"{code} {row['name']} 细分查询失败，按宁可多不要少保留")
            return True
        return False

    return m_df[m_df.apply(_keep, axis=1)].copy()


def build_candidate_stocks(
    keep_all_m: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    构建候选科技股列表。

    Returns:
        (all_df, c_df, i_df, m73_df)
    """
    logger.info("开始获取上交所主板股票列表...")
    sh_df = fetch_sh_main_board()
    logger.info(f"上交所主板: {len(sh_df)} 只")

    logger.info("开始获取深交所主板股票列表...")
    sz_df = fetch_sz_main_board()
    logger.info(f"深交所主板: {len(sz_df)} 只")

    all_df = pd.concat([sh_df, sz_df], ignore_index=True)
    all_df = all_df.drop_duplicates(subset=["code"]).reset_index(drop=True)
    logger.info(f"沪深主板合计（去重）: {len(all_df)} 只")

    # 按证监会门类筛选
    c_df = all_df[all_df["csrc_code"] == CSRC_MANUFACTURING].copy()
    i_df = all_df[all_df["csrc_code"] == CSRC_IT].copy()
    m_df = all_df[all_df["csrc_code"] == CSRC_SCIENCE_TECH].copy()

    logger.info(f"C 类制造业: {len(c_df)} 只")
    logger.info(f"I 类信息传输/软件和信息技术服务业: {len(i_df)} 只")
    logger.info(f"M 类科学研究和技术服务业: {len(m_df)} 只（待确认 M73）")

    # 对 C 类制造业按行业大类排除传统行业
    c_df = filter_manufacturing_stocks(
        c_df,
        excluded_majors=EXCLUDED_MANUFACTURING_MAJORS,
    )
    logger.info(f"过滤后 C 类制造业: {len(c_df)} 只")

    if keep_all_m:
        logger.info("参数 --keep-all-m：保留所有 M 类股票")
        m73_df = m_df.copy()
    else:
        m73_df = filter_m73_stocks(
            m_df, keep_all_m_on_failure=True
        )
    logger.info(f"M73 研究和试验发展: {len(m73_df)} 只")

    return all_df, c_df, i_df, m73_df


def merge_with_existing(
    candidates: pd.DataFrame, tech_stocks_file: str
) -> Tuple[List[Dict], List[Dict]]:
    """
    将候选股票与本地科技股列表合并，保留原有股票，仅补充新增。

    Returns:
        (combined, new_stocks)
    """
    today = datetime.now().strftime("%Y-%m-%d")
    existing = load_json(tech_stocks_file)
    existing_codes: Dict[str, Dict] = {
        str(s.get("code", "")).strip(): s for s in existing if s.get("code")
    }
    logger.info(f"本地已有科技股: {len(existing_codes)} 只")

    new_stocks: List[Dict] = []
    industry_tag_map = {
        CSRC_MANUFACTURING: "证监会-C类制造业",
        CSRC_IT: "证监会-I类信息技术",
        CSRC_SCIENCE_TECH: "证监会-M73研究和试验发展",
    }

    for _, row in candidates.iterrows():
        code = str(row["code"]).strip()
        if not code or code in existing_codes:
            continue

        csrc_code = str(row["csrc_code"]).strip()
        tag = industry_tag_map.get(csrc_code, f"证监会-{csrc_code}类")

        new_stocks.append(
            {
                "code": code,
                "name": str(row["name"]).strip(),
                "is_tech": True,
                "tech_tags": [tag],
                "csrc_code": csrc_code,
                "csrc_name": str(row["csrc_name"]).strip(),
                "exchange": str(row["exchange"]).strip(),
                "updated_at": today,
            }
        )

    combined = list(existing_codes.values()) + new_stocks
    # 按代码排序，便于查看和 diff
    combined.sort(key=lambda x: str(x.get("code", "")).strip())

    # 保险去重
    seen: Set[str] = set()
    unique: List[Dict] = []
    for s in combined:
        code = str(s.get("code", "")).strip()
        if code and code not in seen:
            seen.add(code)
            unique.append(s)

    return unique, new_stocks


def print_summary(stocks: List[Dict], new_stocks: List[Dict]):
    """打印合并统计信息"""
    tag_counts: Dict[str, int] = {}
    for s in stocks:
        for tag in s.get("tech_tags", []):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    logger.info("=== 行业来源统计 ===")
    for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {tag}: {count} 只")

    logger.info(f"本次新增: {len(new_stocks)} 只")
    logger.info(f"合并后总数: {len(stocks)} 只")

    if new_stocks:
        logger.info("=== 新增股票示例（前 30 只）===")
        for s in new_stocks[:30]:
            tags = ",".join(s.get("tech_tags", []))
            logger.info(f"  {s['code']} {s['name']} [{tags}]")


def expand_tech_stocks_by_industry(
    dry_run: bool = False, keep_all_m: bool = False
):
    """主入口：基于证监会行业分类扩充科技股列表"""
    all_df, c_df, i_df, m73_df = build_candidate_stocks(
        keep_all_m=keep_all_m
    )

    candidates = pd.concat([c_df, i_df, m73_df], ignore_index=True)
    candidates = candidates.drop_duplicates(subset=["code"]).reset_index(drop=True)
    logger.info(f"候选科技股总数: {len(candidates)} 只")

    combined, new_stocks = merge_with_existing(candidates, TECH_STOCKS_FILE)

    if not dry_run:
        save_json(TECH_STOCKS_FILE, combined)
        logger.info(f"结果已保存到: {TECH_STOCKS_FILE}")
    else:
        logger.info("本次为 --dry-run，未写入文件")

    print_summary(combined, new_stocks)
    return combined, new_stocks


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="基于证监会行业分类扩充科技股列表"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="只统计不写入"
    )
    parser.add_argument(
        "--keep-all-m",
        action="store_true",
        help="保留所有 M 类股票，不细分为 M73",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    expand_tech_stocks_by_industry(
        dry_run=args.dry_run,
        keep_all_m=args.keep_all_m,
    )
