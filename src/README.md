# src 脚本索引

`src` 只保留三类内容：可执行脚本、行业词表、少量运行文档。

## 分组

### `a_share/`

- `scan_a_share_quarterly_revenue_growth.py`：单季度营收同比扫描
- `scan_a_share_interval_change.py`：区间涨跌幅筛选
- `run_a_share_24h_scan_v2.py`：24 小时公告窗口扫描
- `run_revenue_event_analysis.py`：财报事件后收益/回撤统计

### `monitoring/`

- `scan_industry_term_frequency.py`：行业词频统计
- `monitor_long_term_theme_heat.py`：按申万行业词表统计公开讨论命中次数 TopN（默认 `bing`，每查询最多 `500` 条）

### `market/`

- `download_ixic_full_history.py`：下载纳指历史数据

### `taxonomy/`

- `a_share_sw_taxonomy.json`：A 股申万行业词表
- `gics_us_taxonomy.json`：美股 GICS 行业词表

## 常用命令

```powershell
cd Analysis

.\python.cmd src\a_share\scan_a_share_quarterly_revenue_growth.py --year 2025 --quarter Q4 --growth-threshold 20
.\python.cmd src\a_share\scan_a_share_interval_change.py --StartYear 2020 --EndYear 2025 --Direction rise --ChangeThresholdPct 200
.\python.cmd src\a_share\run_a_share_24h_scan_v2.py --target-year 2025 --target-quarter Q4 --notice-within-hours 24
.\python.cmd src\a_share\run_revenue_event_analysis.py --Codes 600000,000001 --WindowMonths 2

.\python.cmd src\monitoring\scan_industry_term_frequency.py --lookback-days 3
.\python.cmd src\monitoring\monitor_long_term_theme_heat.py --taxonomy-path .\src\taxonomy\a_share_sw_taxonomy.json --lookback-days 10 --top-n 10 --sources bing --max-items-per-query 500

.\python.cmd src\market\download_ixic_full_history.py --output-dir .\output
```

## 说明

- 默认输出目录是 `Analysis/output/`
- 新脚本统一写根目录 `output/`
- 如果当前环境没有系统级 `python`，优先使用仓库内的 `python.cmd`
