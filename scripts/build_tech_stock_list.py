# -*- coding: utf-8 -*-
"""
构建并维护沪深主板科技股票列表

基于本地 data/main_board_stocks.json 和 data/tech_keywords.json，
通过股票名称关键词匹配，离线生成科技类股票清单并持久化保存到 data/tech_stocks.json。

运行方式:
    source venv/bin/activate
    python scripts/build_tech_stock_list.py
"""
import json
import os
import sys
from datetime import datetime
from typing import Dict, List

# 将项目根目录加入路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

MAIN_BOARD_FILE = os.path.join(DATA_DIR, "main_board_stocks.json")
TECH_KEYWORDS_FILE = os.path.join(DATA_DIR, "tech_keywords.json")
TECH_STOCKS_FILE = os.path.join(DATA_DIR, "tech_stocks.json")


def load_json(filepath: str) -> List[Dict]:
    """加载 JSON 文件"""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(filepath: str, data: List[Dict], indent: int = 2):
    """保存 JSON 文件"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)


def match_tech_tags(name: str, keywords: List[str]) -> List[str]:
    """
    根据股票名称匹配科技关键词，返回命中的关键词列表

    匹配规则:
        - 大小写不敏感
        - 子串匹配
        - 去重并按原词序返回
    """
    if not name:
        return []

    name_lower = str(name).lower()
    tags = []
    seen = set()

    for kw in keywords:
        kw_lower = str(kw).lower()
        if kw_lower in name_lower and kw_lower not in seen:
            tags.append(kw)
            seen.add(kw_lower)

    return tags


def build_tech_stock_list(
    main_board_file: str = MAIN_BOARD_FILE,
    keywords_file: str = TECH_KEYWORDS_FILE,
    output_file: str = TECH_STOCKS_FILE,
) -> List[Dict]:
    """
    基于主板股票列表和科技关键词，构建科技股票清单

    Returns:
        科技股票列表，每只包含 code, name, tech_tags, updated_at
    """
    stocks = load_json(main_board_file)
    keywords = load_json(keywords_file)

    print(f"加载主板股票: {len(stocks)} 只")
    print(f"加载科技关键词: {len(keywords)} 个")

    tech_stocks = []
    today = datetime.now().strftime("%Y-%m-%d")

    for stock in stocks:
        code = str(stock.get("code", "")).strip()
        name = str(stock.get("name", "")).strip()
        if not code or not name:
            continue

        tags = match_tech_tags(name, keywords)
        if tags:
            tech_stocks.append({
                "code": code,
                "name": name,
                "is_tech": True,
                "tech_tags": tags,
                "updated_at": today,
            })

    # 按代码排序，便于查看和 diff
    tech_stocks.sort(key=lambda x: x["code"])

    save_json(output_file, tech_stocks)

    print(f"识别出科技类股票: {len(tech_stocks)} 只")
    print(f"结果已保存到: {output_file}")

    return tech_stocks


def print_summary(tech_stocks: List[Dict]):
    """打印科技股票分类统计"""
    tag_counts: Dict[str, int] = {}
    for stock in tech_stocks:
        for tag in stock.get("tech_tags", []):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    print("\n=== 科技关键词命中统计（Top 30）===")
    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
    for tag, count in sorted_tags[:30]:
        print(f"  {tag}: {count} 只")

    print(f"\n共识别 {len(tech_stocks)} 只主板科技股票")


if __name__ == "__main__":
    tech_stocks = build_tech_stock_list()
    print_summary(tech_stocks)
