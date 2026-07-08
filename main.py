# -*- coding: utf-8 -*-
"""
主程序 - ETF量化分析系统
"""
import logging
import time
import schedule
import os
import sys
from datetime import datetime, timedelta
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
from signals.vix_signal import generate_vix_signal, format_vix_report
from analyzers import vix_linkage
from analyzers.stock_scanner import MA20Scanner
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
                realtime = self.fetcher.get_etf_realtime(code, storage=self.storage)
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

    def analyze_market_performance(self):
        """分析全市场ETF表现"""
        logger.info("=" * 60)
        logger.info("开始全市场ETF涨幅分析")

        perf_df = self.fetcher.get_all_etf_performance(days=5)

        if perf_df.empty:
            logger.error("未能获取ETF数据")
            return

        logger.info(f"共获取 {len(perf_df)} 只ETF的涨跌幅数据")

        print("\n" + "=" * 60)
        print("📊 全市场ETF涨幅分析")
        print("=" * 60)

        print(f"\n📈 统计时间: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"📊 分析ETF总数: {len(perf_df)}")

        strong_5d = perf_df[perf_df['5_day_change'] > 10].sort_values('5_day_change', ascending=False)
        print(f"\n🔺 5日涨幅超过10%的ETF ({len(strong_5d)}只):")
        if not strong_5d.empty:
            for _, row in strong_5d.head(10).iterrows():
                code = row['code'].split('.')[-1]
                print(f"  • {code} {row['name'][:15]:15s} 5日涨幅: {row['5_day_change']:+.2f}%")
        else:
            print("  无")

        strong_1d = perf_df[perf_df['max_daily_change'] > 3].sort_values('max_daily_change', ascending=False)
        print(f"\n🔥 单日涨幅超过3%的ETF ({len(strong_1d)}只):")
        if not strong_1d.empty:
            for _, row in strong_1d.head(10).iterrows():
                code = row['code'].split('.')[-1]
                print(f"  • {code} {row['name'][:15]:15s} 最大单日: {row['max_daily_change']:+.2f}%")
        else:
            print("  无")

        print("\n" + "=" * 60)
        logger.info("全市场ETF涨幅分析完成")

    def run_vix_monitor(self):
        """
        运行 VIX-科技板块关联监控
        获取 VIX(T-1) 与科技持仓(T) 的关联指标，生成仓位调整信号
        """
        logger.info("=" * 60)
        logger.info("开始 VIX-科技板块关联监控")

        vix_cfg = config.VIX_MONITOR_CONFIG
        tech_etfs = vix_cfg.get("tech_etfs", {})
        tech_stocks = vix_cfg.get("tech_stocks", {})

        # 1. 获取 VIX 数据 (至少取过去一年，保证60日回归有足够数据)
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        vix_df = self.fetcher.get_vix_daily(start_date=start_date, end_date=end_date)
        if vix_df.empty:
            logger.error("未能获取VIX数据，跳过VIX监控")
            return

        # 2. 获取科技持仓历史数据
        etf_dfs = {}
        stock_dfs = {}

        for name, code in tech_etfs.items():
            try:
                df = self.fetcher.get_etf_historical(code, start_date=start_date, end_date=end_date)
                if not df.empty:
                    etf_dfs[name] = df
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"获取ETF {name}({code}) 失败: {e}")

        for name, code in tech_stocks.items():
            try:
                df = self.fetcher.get_stock_historical(code, start_date=start_date, end_date=end_date)
                if not df.empty:
                    stock_dfs[name] = df
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"获取个股 {name}({code}) 失败: {e}")

        if not etf_dfs and not stock_dfs:
            logger.error("未能获取任何科技持仓数据，跳过VIX监控")
            return

        # 3. 构建科技板块等权收益率指数
        tech_basket = vix_linkage.build_tech_basket_return(etf_dfs, stock_dfs, min_history=60)
        if tech_basket.empty:
            logger.error("科技板块等权指数构建失败")
            return

        # 4. 对齐 VIX(T-1) 与 科技板块(T)
        aligned = vix_linkage.align_vix_a_share(vix_df, tech_basket, vix_shift=1)
        if aligned.empty or len(aligned) < 60:
            logger.warning(f"对齐后数据不足，仅 {len(aligned)} 天，需要至少60天")
            return

        # 5. 计算最新指标
        corr_window = vix_cfg.get("corr_windows", [20])[0]
        beta_window = vix_cfg.get("beta_window", 60)
        metrics = vix_linkage.calculate_latest_metrics(aligned, corr_window=corr_window, beta_window=beta_window)

        logger.info(f"VIX最新指标: {metrics}")

        # 6. 生成交易信号
        signal = generate_vix_signal(metrics)

        # 7. 获取今日持仓表现 (实时涨跌幅)
        tech_holdings_today = {}
        for name, code in tech_etfs.items():
            try:
                rt = self.fetcher.get_etf_realtime(code, storage=self.storage)
                if rt:
                    tech_holdings_today[name] = rt.get("pct_change", 0) or 0
            except Exception:
                pass
        for name, code in tech_stocks.items():
            try:
                rt = self.fetcher.get_stock_realtime(code, storage=self.storage)
                if rt:
                    tech_holdings_today[name] = rt.get("pct_change", 0) or 0
            except Exception:
                pass

        # 8. 输出报告
        report = format_vix_report(signal, tech_holdings=tech_holdings_today)
        print("\n" + report + "\n")

        # 9. 发送到飞书
        if config.FEISHU_WEBHOOK:
            self.feishu_reporter.send_vix_report(signal, tech_holdings=tech_holdings_today)
            logger.info("VIX监控报告已发送到飞书")
        else:
            logger.warning("未配置飞书Webhook，跳过VIX报告推送")

        logger.info("VIX-科技板块关联监控完成")
        logger.info("=" * 60)

    def scan_ma20_stocks(self):
        """扫描主板 MA20 强势程度，并保存完整结果到文件"""
        scanner = MA20Scanner(self.fetcher, self.storage)
        result_df = scanner.scan(include_all=True)
        report = scanner.format_report(result_df, top_n=50)
        print(report)

        # 保存完整结果到 reports/ 目录（含 CSV）
        report_path = scanner.save_report_to_md(result_df, top_n=100)
        print(f"\n报告已保存: {report_path}")

    def scan_tech_ma20_stocks(self):
        """扫描本地科技股列表中所有股票的 MA20 强势程度"""
        scanner = MA20Scanner(self.fetcher, self.storage)
        result_df = scanner.scan(use_tech_list=True)
        report = scanner.format_report(result_df)
        print(report)

        # 保存结果到 reports/ 目录
        report_path = scanner.save_report_to_md(result_df)
        print(f"\n科技股报告已保存: {report_path}")
        return result_df


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="ETF量化分析系统")
    parser.add_argument("--once", action="store_true", help="仅运行一次")
    parser.add_argument("--schedule", action="store_true", help="定时运行")
    parser.add_argument("--market", action="store_true", help="分析全市场ETF涨幅")
    parser.add_argument("--vix", action="store_true", help="运行VIX-科技板块关联监控")
    parser.add_argument("--scan-ma20", action="store_true", help="扫描近一个月未跌破20日线的主板股票")
    parser.add_argument("--scan-tech-ma20", action="store_true", help="扫描本地科技股列表中所有股票的MA20强势程度")

    args = parser.parse_args()

    system = QuantETFSystem()

    if args.market:
        system.analyze_market_performance()
    elif args.vix:
        system.run_vix_monitor()
    elif args.scan_tech_ma20:
        system.scan_tech_ma20_stocks()
    elif args.scan_ma20:
        system.scan_ma20_stocks()
    elif args.once or not args.schedule:
        system.run_once()
    else:
        system.run_schedule()


if __name__ == "__main__":
    main()
