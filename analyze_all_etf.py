# -*- coding: utf-8 -*-
"""
全量ETF分析脚本 - 分步执行
"""
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from data.fetcher import get_all_etf_symbols, ETFFetcher
from signals.generator import SignalGenerator, SignalType, format_signal_report

def main():
    print("\n" + "=" * 60)
    print("🚀 开始全量ETF量化分析")
    print("=" * 60)

    # 1. 获取ETF列表
    logger.info("获取ETF代码列表...")
    symbols = get_all_etf_symbols()
    logger.info(f"共获取 {len(symbols)} 只ETF")

    # 2. 获取历史数据
    logger.info("开始获取ETF历史数据...")
    fetcher = ETFFetcher()
    historical_data = {}

    success_count = 0
    fail_count = 0

    for i, symbol in enumerate(symbols):
        try:
            df = fetcher.get_etf_historical(symbol)
            if not df.empty and len(df) >= 30:
                historical_data[symbol] = df
                success_count += 1
            else:
                fail_count += 1

            if (i + 1) % 50 == 0:
                logger.info(f"进度: {i+1}/{len(symbols)}, 成功: {success_count}, 失败: {fail_count}")

            time.sleep(0.3)

        except Exception as e:
            fail_count += 1
            if fail_count <= 5:
                logger.warning(f"获取 {symbol} 失败: {e}")

    logger.info(f"数据获取完成，成功: {success_count}, 失败: {fail_count}")
    print(f"\n📊 成功获取 {len(historical_data)} 只ETF的历史数据\n")

    if not historical_data:
        print("没有获取到足够的数据，退出")
        return

    # 3. 分析ETF
    logger.info("开始分析ETF...")
    generator = SignalGenerator()
    signals = []

    for symbol, df in historical_data.items():
        try:
            signal = generator.analyze(
                symbol=symbol,
                name=symbol,
                historical_data=df,
                realtime_data={}
            )
            signals.append(signal)
        except Exception as e:
            logger.warning(f"分析 {symbol} 失败: {e}")

    signals.sort(key=lambda x: x.score, reverse=True)
    logger.info(f"分析完成，共生成 {len(signals)} 个信号")

    # 4. 筛选Top 10买入信号
    buy_signals = [s for s in signals if s.signal in [SignalType.STRONG_BUY, SignalType.BUY]]
    top10 = buy_signals[:10]

    # 5. 生成报告
    print("\n" + "=" * 60)
    print("📈 买入推荐 TOP 10 ETF")
    print("=" * 60)
    print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"分析ETF总数: {len(signals)}")
    print(f"买入信号数: {len(buy_signals)}")
    print()

    for i, s in enumerate(top10, 1):
        print(f"#{i} {s.symbol}")
        print(f"    综合评分: {s.score:.3f}")
        print(f"    技术评分: {s.technical_score:.3f} | 动量评分: {s.momentum_score:.3f} | ETF评分: {s.etf_score:.3f}")
        if s.indicators:
            ma5 = s.indicators.get('ma5', 0)
            ma20 = s.indicators.get('ma20', 0)
            rsi = s.indicators.get('rsi', 0)
            print(f"    MA5: {ma5:.3f} | MA20: {ma20:.3f} | RSI: {rsi:.1f}")
            mom = s.indicators.get('momentum_short', 0)
            if mom:
                print(f"    5日动量: {mom*100:+.2f}%")
        print(f"    买入原因: {', '.join(s.reasons[:3]) if s.reasons else '综合分析'}")
        print()

    print("=" * 60)
    print("📊 信号分布统计")
    print("=" * 60)
    signal_counts = {}
    for s in signals:
        key = s.signal.value
        signal_counts[key] = signal_counts.get(key, 0) + 1

    for sig, count in sorted(signal_counts.items(), key=lambda x: x[1], reverse=True):
        pct = count / len(signals) * 100
        print(f"  {sig}: {count}只 ({pct:.1f}%)")

    print("\n" + "=" * 60)
    print("💡 提示: 本报告仅供参考，不构成投资建议")
    print("=" * 60)

if __name__ == "__main__":
    main()
