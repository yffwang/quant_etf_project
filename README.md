# ETF量化分析项目

## 项目结构

```
quant_etf_project/
├── config.py              # 配置文件
├── main.py                # 主入口
├── data/
│   ├── fetcher.py        # 数据获取 (AkShare)
│   └── storage.py        # 数据存储
├── analyzers/
│   ├── technical.py      # 技术指标
│   ├── momentum.py       # 动量因子
│   └── etf_factors.py    # ETF专用因子
├── signals/
│   └── generator.py      # 信号生成
├── reporters/
│   └── feishu.py         # 飞书推送
└── requirements.txt      # 依赖
```

## 功能

- ✅ 每10分钟自动获取A股场内ETF数据
- ✅ 技术指标分析 (MACD, RSI, BOLL, MA)
- ✅ 动量因子 (1周/1月/3月/6月)
- ✅ ETF专用因子 (溢价率, 跟踪误差, 规模)
- ✅ 买卖信号生成
- ✅ 飞书推送报告
