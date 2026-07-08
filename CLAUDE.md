# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A quantitative analysis system for Chinese A-share ETFs (场内ETF). It fetches market data via AkShare, computes technical indicators and momentum factors, generates trading signals, and optionally pushes reports to Feishu (Lark).

## Commands

```bash
# Activate the local venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run a single analysis pass for the watch-list ETFs
python main.py --once

# Start the scheduled loop (every 10 minutes, configured in config.py)
python main.py --schedule

# Analyze all-market ETF performance (5-day change, max daily change)
python main.py --market

# Run any module standalone for quick testing
python data/fetcher.py
python signals/generator.py
python analyzers/technical.py
python analyzers/momentum.py
python analyzers/etf_factors.py
python reporters/feishu.py
```

## Architecture

### Data Pipeline

`QuantETFSystem` (main.py) orchestrates the pipeline:
1. **Fetch** — `ETFFetcher` pulls historical/real-time data from AkShare for each ETF in `config.ETF_CATEGORIES`.
2. **Store** — `ETFStorage` persists data to a local SQLite DB (`data/etf_data.db`) with four tables: `etf_daily`, `etf_realtime`, `etf_analysis`, `trading_signals`.
3. **Analyze** — `SignalGenerator` runs three analyzers per ETF and aggregates their scores.
4. **Report** — Results are printed to console and, if `FEISHU_WEBHOOK` is set, sent to Feishu via `FeishuReporter`.

### Three-Dimensional Scoring

`SignalGenerator` (signals/generator.py) weights three analyzer dimensions:
- **Technical** (0.40) — MA alignment, MACD cross, RSI, Bollinger position
- **Momentum** (0.35) — short/medium/long momentum, acceleration, volatility-adjusted Sharpe, max drawdown
- **ETF Factors** (0.25) — premium rate, liquidity, turnover, volatility, tracking error, recent returns

Each dimension returns a score in `[-1, 1]` and a list of signal reasons. The composite score maps to `SignalType` thresholds: `≥0.6` STRONG_BUY, `≥0.3` BUY, `≥-0.3` HOLD, `≥-0.6` SELL, else STRONG_SELL.

### Data Source

AkShare (`akshare`) is a free, open-source Chinese financial data library. Most APIs require no token. ETF codes use numeric prefixes: `51`/`58` (Shanghai), `15`/`16` (Shenzhen). The fetcher strips `sz`/`sh` prefixes before calling AkShare and caches results in-memory.

### Real-Time vs Historical

- Historical data comes from `ak.fund_etf_hist_em` (daily OHLCV, qfq adjusted).
- Real-time data comes from `ak.fund_etf_spot_em` (a full snapshot filtered by symbol).
- Both are stored in SQLite; analysis uses historical DataFrames plus the latest realtime row when available.

## Key Files

- `config.py` — ETF watch list, technical parameters, signal thresholds, DB path, and Feishu env-var reads.
- `data/fetcher.py` — All AkShare I/O and a bulk all-market performance scanner.
- `data/storage.py` — SQLite CRUD and schema initialization.
- `signals/generator.py` — `SignalGenerator`, `TradingSignal` dataclass, and `format_signal_report`.
- `analyzers/technical.py` — `TechnicalAnalyzer` with MA, MACD, RSI, BOLL, KDJ, ATR; scoring function.
- `analyzers/momentum.py` — `MomentumAnalyzer` with returns, composite momentum, Sharpe, drawdown, volume momentum; scoring function.
- `analyzers/etf_factors.py` — `ETFFactorAnalyzer` with premium, tracking error, liquidity, turnover, volatility, return factors; scoring function.
- `reporters/feishu.py` — `FeishuReporter` supporting text and interactive card messages with optional HMAC-SHA256 signing.

## Environment Variables

- `FEISHU_WEBHOOK` — Feishu bot webhook URL
- `FEISHU_SECRET` — Feishu bot secret (optional, for signature verification)
- `AKSHARE_TOKEN` — Optional AkShare token for premium APIs

## Notes

- The SQLite DB file is created automatically at `data/etf_data.db` on first run.
- Strong signals (STRONG_BUY / STRONG_SELL) are persisted to the `trading_signals` table.
- The `main.py` scheduler uses the `schedule` library and sleeps in a `while True` loop.
