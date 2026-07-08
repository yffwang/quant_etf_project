# -*- coding: utf-8 -*-
"""
获取并保存沪深主板股票代码列表到本地
"""
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.fetcher import ETFFetcher
from scripts.build_tech_stock_list import build_tech_stock_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "main_board_stocks.json"
)


def save_main_board_codes():
    """获取主板股票列表并保存为 JSON，同时刷新科技股票列表"""
    fetcher = ETFFetcher()
    df = fetcher.get_stock_main_board_list()

    if df.empty:
        logger.error("未能获取主板股票列表")
        return

    records = df.to_dict(orient="records")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info(f"已保存 {len(records)} 只主板股票代码到 {OUTPUT_PATH}")
    logger.info(f"前 10 只示例: {records[:10]}")

    # 同步刷新本地科技股票清单
    logger.info("开始同步刷新科技股票列表...")
    build_tech_stock_list()


if __name__ == "__main__":
    save_main_board_codes()
