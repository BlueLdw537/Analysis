# src 脚本索引

本目录仅保留“可直接运行”的功能脚本入口，方便快速执行。

## 快速开始

```powershell
cd Analysis/src
python .\scan_a_share_quarterly_revenue_growth.py --help
```

## 脚本清单

| 脚本 | 作用 | 常用命令 |
| --- | --- | --- |
| `scan_a_share_quarterly_revenue_growth.py` | 单季度营收同比扫描（通用版） | `python .\scan_a_share_quarterly_revenue_growth.py --year 2025 --quarter Q4 --growth-threshold 20` |
| `run_once_a_share_2025q4_24h_scan.py` | 最近 N 小时公告窗口扫描 | `python .\run_once_a_share_2025q4_24h_scan.py --target-year 2025 --target-quarter Q4 --notice-within-hours 24` |
| `run_a_share_24h_scan_v2.py` | 按 `run_a_share_24h_scan_v2.ps1` 等价重写的 Python 版 24h 扫描 | `python .\run_a_share_24h_scan_v2.py --target-year 2025 --target-quarter Q4 --notice-within-hours 24` |
| `scan_a_share_interval_change.py` | 按年份区间筛选涨跌幅 | `python .\scan_a_share_interval_change.py --StartYear 2020 --EndYear 2025 --Direction rise --ChangeThresholdPct 200` |
| `run_revenue_event_analysis.py` | 财报事件后 N 月收益/回撤统计 | `python .\run_revenue_event_analysis.py --Codes 600000,000001 --WindowMonths 2` |
| `scan_industry_term_frequency.py` | 近 N 天行业词频统计（CSV） | `python .\scan_industry_term_frequency.py --lookback-days 3` |

## 详细运行示例

### 1) 单季度营收同比扫描（通用）

```powershell
python .\scan_a_share_quarterly_revenue_growth.py `
  --year 2025 `
  --quarter Q4 `
  --growth-threshold 20 `
  --output-path ..\output\a_share_2025_q4_scan.xlsx `
  --max-retry 4
```

### 2) 最近公告窗口扫描（24h 可调）

```powershell
python .\run_once_a_share_2025q4_24h_scan.py `
  --target-year 2025 `
  --target-quarter Q4 `
  --notice-within-hours 24 `
  --growth-threshold 20 `
  --output-dir ..\output `
  --max-retry 1
```

### 3) PowerShell v2 对齐版（Python）（能用）

```powershell
python .\run_a_share_24h_scan_v2.py `
  --target-year 2025 `
  --target-quarter Q4 `
  --notice-within-hours 24 `
  --growth-threshold 20 `
  --output-dir D:\codex\output `
  --max-retry 4
```

### 4) 区间涨跌幅扫描

```powershell
python .\scan_a_share_interval_change.py `
  --StartYear 2020 `
  --EndYear 2025 `
  --Direction rise `
  --ChangeThresholdPct 200 `
  --TopN 200 `
  --OutputPath ..\output\a_share_interval_2020_2025.xlsx
```

### 5) 营收事件回测

```powershell
python .\run_revenue_event_analysis.py `
  --Codes 600000,000001 `
  --WindowMonths 2 `
  --YoyThreshold 20 `
  --ProfitThreshold 20 `
  --LossThreshold 20 `
  --OutputDir ..\output
```

### 6) 行业词频统计

```powershell
python .\scan_industry_term_frequency.py `
  --lookback-days 3 `
  --top-level1 3 `
  --top-level2 5 `
  --top-level3 10 `
  --top-level4 15 `
  --sources public_rss,baidu,bing,msn_edge,google,google_en,gdelt `
  --output-path ..\output\industry_term_frequency.csv
```

- A股行业词表默认读取：`src/taxonomy/a_share_sw_taxonomy.json`（由 AkShare `sw_index_*` 数据生成）
- 美股行业词表默认读取：`src/taxonomy/gics_us_taxonomy.json`

## 输出目录约定

- 默认输出目录：`Analysis/output`
- `run_a_share_24h_scan_v2.py` 默认输出目录为：`D:\codex\output`
- `output/` 下结果文件已在 `.gitignore` 中忽略，不会污染版本库

## 维护说明

- 详细的架构约定与目录职责：`../docs/ARCHITECTURE.md`
