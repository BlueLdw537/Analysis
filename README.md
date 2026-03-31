# Analysis

用于存放可直接运行的市场数据与行业分析脚本。当前目录按职责分成三组：

- `src/a_share/`：A 股财报、区间涨跌幅、事件回测
- `src/monitoring/`：行业词频、行业讨论命中统计
- `src/market/`：指数或宏观市场数据下载

## 目录结构

```text
Analysis/
├── docs/                    # 说明文档与模板
├── output/                  # 默认输出目录
├── src/
│   ├── a_share/             # A 股扫描脚本
│   ├── monitoring/          # 行业/讨论监控脚本
│   ├── market/              # 市场数据下载脚本
│   ├── taxonomy/            # 行业词表
│   └── README.md            # src 入口索引
├── tests/
├── python.cmd               # 本地 Python wrapper
├── py.cmd                   # 兼容 wrapper
└── requirements.txt
```

## 快速开始

```powershell
cd Analysis
.\python.cmd src\a_share\scan_a_share_quarterly_revenue_growth.py --help
.\python.cmd src\monitoring\monitor_long_term_theme_heat.py --help
```

如果需要完整脚本索引和示例命令，查看 [src/README.md](d:\github\Analysis\src\README.md)。
