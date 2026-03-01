# -*- coding: utf-8 -*-
"""
主程序 - ETF量化分析系统
"""
import logging
import time
import schedule
import os
import sys
from datetime import datetime
from typing import List, Dict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from data.fetcher import ETFFetcher
from data.storage import ETFStorage
from signals.generator import SignalGenerator, SignalType, format_signal_report
from reporters.feishu import FeishuReporter


class QuantETFSystem:
    """ETF量化分析系统"""
    
    def __init__(self):
        self.fetcher = ETFFetcher()
        self.storage = ETFStorage()
        self.signal_generator = SignalGenerator()
        self.feishu_reporter = FeishuReporter()
        
        # ETF关注列表
        self.watch_list = []
        
        # 加载配置
        self._load_watch_list()
    
    def _load_watch_list(self):
        """加载关注列表"""
        # 从配置加载
        for category, codes in config.ETF_CATEGORIES.items():
            for code in codes:
                self.watch_list.append({
                    "code": code,
                    "category": category
                })
        
        logger.info(f"已加载 {len(self.watch_list)} 只关注ETF")
    
    def fetch_data(self):
        """获取数据"""
        logger.info("开始获取ETF数据...")
        
        historical_data = {}
        
        for etf in self.watch_list:
            code = etf["code"]
            try:
                # 获取历史数据
                df = self.fetcher.get_etf_historical(code)
                if not df.empty:
                    historical_data[code] = df
                    # 保存到数据库
                    self.storage.save_daily(df)
                    logger.info(f"获取 {code} 历史数据成功: {len(df)} 条")
                
                # 获取实时数据
                realtime = self.fetcher.get_etf_realtime(code)
                if realtime:
                    self.storage.save_realtime(realtime)
                
                # 避免请求过快
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"获取 {code} 数据失败: {e}")
        
        logger.info(f"数据获取完成，共 {len(historical_data)} 只ETF")
        return historical_data
    
    def analyze(self, historical_data: Dict):
        """分析ETF"""
        logger.info("开始分析ETF...")
        
        signals = []
        
        for etf in self.watch_list:
            code = etf["code"]
            
            if code not in historical_data:
                continue
            
            try:
                historical = historical_data[code]
                realtime = self.storage.get_realtime(code)
                
                # 生成信号
                signal = self.signal_generator.analyze(
                    symbol=code,
                    name=etf.get("category", code),
                    historical_data=historical,
                    realtime_data=realtime
                )
                
                signals.append(signal)
                
                # 保存分析结果
                self.storage.save_analysis(code, {
                    "ma5": signal.indicators.get("ma5"),
                    "ma10": signal.indicators.get("ma10"),
                    "ma20": signal.indicators.get("ma20"),
                    "ma60": signal.indicators.get("ma60"),
                    "macd": signal.indicators.get("macd"),
                    "macd_signal": signal.indicators.get("macd_signal"),
                    "macd_hist": signal.indicators.get("macd_hist"),
                    "rsi": signal.indicators.get("rsi"),
                    "boll_upper": signal.indicators.get("boll_upper"),
                    "boll_middle": signal.indicators.get("boll_middle"),
                    "boll_lower": signal.indicators.get("boll_lower"),
                    "momentum_5d": signal.indicators.get("momentum_short"),
                    "momentum_20d": signal.indicators.get("momentum_medium"),
                    "momentum_60d": signal.indicators.get("momentum_long"),
                    "signal": signal.signal.value
                })
                
                # 保存交易信号
                if signal.signal in [SignalType.STRONG_BUY, SignalType.STRONG_SELL]:
                    self.storage.save_signal(
                        code,
                        signal.signal.value,
                        "; ".join(signal.reasons),
                        signal.strength
                    )
                
            except Exception as e:
                logger.error(f"分析 {code} 失败: {e}")
        
        # 按评分排序
        signals.sort(key=lambda x: x.score, reverse=True)
        
        logger.info(f"分析完成，共生成 {len(signals)} 个信号")
        return signals
    
    def generate_report(self, signals: List):
        """生成报告"""
        logger.info("生成分析报告...")
        
        # 打印到控制台
        report = format_signal_report(signals)
        print("\n" + report + "\n")
        
        # 发送到飞书
        if config.FEISHU_WEBHOOK:
            self.feishu_reporter.send_signal_report(signals)
            logger.info("报告已发送到飞书")
        else:
            logger.warning("未配置飞书Webhook，跳过推送")
    
    def run_once(self):
        """运行一次分析"""
        logger.info("=" * 50)
        logger.info(f"开始ETF量化分析 - {datetime.now()}")
        
        # 1. 获取数据
        historical_data = self.fetch_data()
        
        if not historical_data:
            logger.warning("没有获取到任何数据")
            return
        
        # 2. 分析
        signals = self.analyze(historical_data)
        
        if not signals:
            logger.warning("没有生成任何信号")
            return
        
        # 3. 生成报告
        self.generate_report(signals)
        
        logger.info(f"分析完成 - {datetime.now()}")
        logger.info("=" * 50)
    
    def run_schedule(self):
        """定时运行"""
        logger.info("启动定时任务...")
        
        # 每10分钟运行一次
        schedule.every(config.FETCH_INTERVAL_MINUTES).minutes.do(self.run_once)
        
        # 立即运行一次
        self.run_once()
        
        # 保持运行
        while True:
            schedule.run_pending()
            time.sleep(60)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ETF量化分析系统")
    parser.add_argument("--once", action="store_true", help="仅运行一次")
    parser.add_argument("--schedule", action="store_true", help="定时运行")
    
    args = parser.parse_args()
    
    system = QuantETFSystem()
    
    if args.once or not args.schedule:
        system.run_once()
    else:
        system.run_schedule()


if __name__ == "__main__":
    main()
