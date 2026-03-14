# Analysis

用于 A 股数据扫描与专题分析的脚本仓库，当前聚焦 3 类任务：
- 财报营收相关扫描
- 区间涨跌幅筛选
- 行业词频与主题热度统计

## 当前目录结构

```text
Analysis/
├── .github/                    # CI 与 issue 模板
├── docs/
│   ├── ARCHITECTURE.md         # 架构说明（详细）
│   └── templates/
│       └── output_template.xlsx
├── output/                     # 分析产物目录（默认输出，已忽略）
├── src/                        # 可执行脚本与内置词表
│   ├── README.md               # 脚本索引与运行指令
│   ├── scan_a_share_quarterly_revenue_growth.py
│   ├── run_once_a_share_2025q4_24h_scan.py
│   ├── scan_a_share_interval_change.py
│   ├── run_revenue_event_analysis.py
│   ├── scan_industry_term_frequency.py
│   └── taxonomy/
│       ├── a_share_sw_taxonomy.json
│       └── gics_us_taxonomy.json
├── tests/
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
```

## 快速开始

```powershell
cd Analysis
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -q
```

## 运行脚本

所有脚本运行说明见：
- [src/README.md](src/README.md)

## 架构说明

完整的目录分层与维护约定见：
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
