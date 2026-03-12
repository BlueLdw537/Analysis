# src 脚本说明

本文档说明 `Analysis/src` 目录下各功能脚本的用途和差异，便于快速选择运行入口。

## 文件清单

- `scan_a_share_quarterly_revenue_growth.py`
- `run_once_a_share_2025q4_24h_scan.py`
- `__init__.py`（包初始化文件，无业务逻辑）

## 1) scan_a_share_quarterly_revenue_growth.py

### 作用

通用版 A 股单季度营收同比扫描脚本。  
面向“指定年份 + 指定季度”的常规分析，不限制公告发布时间窗口。

### 主要功能

- 拉取东财财报接口数据（分页 + 重试）
- 将累计营收转换为单季度营收（Q2=Q2累计-Q1累计，Q3/Q4 同理）
- 对比上一年同季度，计算单季度营收同比增速
- 补充行业/市值/上市日期信息（含补充接口）
- 输出 Excel（`Summary` + `Result` 两个工作表，含表格样式）
- 打印 JSON 摘要（输出路径、命中数量等）

### 关键参数

- `--year`：目标年份（如 2025）
- `--quarter`：目标季度（`Q1/Q2/Q3/Q4`）
- `--growth-threshold`：同比阈值（默认 20）
- `--output-path`：输出 Excel 完整路径
- `--max-retry`：接口重试次数（默认 4）

### 默认输出

未传 `--output-path` 时，输出到当前工作目录下的 `output/`，文件名示例：  
`a_share_2025_Q4_single_revenue_yoy_gt20_20260312_194619.xlsx`

### 适用场景

- 做某一季度的完整同比筛选
- 不要求“最近 N 小时公告”限制

## 2) run_once_a_share_2025q4_24h_scan.py

### 作用

一次性扫描版脚本，核心是“最近 N 小时内发布公告”的窗口过滤。  
这是从原 PowerShell 脚本迁移来的 Python 版本。

### 主要功能

- 拉取东财财报接口数据（分页 + 重试）
- 计算单季度营收并对比上年同季度同比
- 仅保留“目标季度且公告时间在最近 N 小时内”的公司
- 过滤同比增速大于阈值的标的
- 输出 Excel（`Summary` + `Result`，表格样式与模板一致）
- 打印 JSON 摘要（`success/matched_companies/output_excel`）

### 关键参数

- `--TargetYear` 或 `--target-year`：目标年份（默认 2025）
- `--TargetQuarter` 或 `--target-quarter`：目标季度（默认 `Q4`）
- `--GrowthThreshold` 或 `--growth-threshold`：同比阈值（默认 20）
- `--NoticeWithinHours` 或 `--notice-within-hours`：公告窗口小时数（默认 24，范围 1-168）
- `--OutputDir` 或 `--output-dir`：输出目录（默认 `Analysis/output`）
- `--MaxRetry` 或 `--max-retry`：接口重试次数（默认 4）

### 默认输出

默认输出目录：`Analysis/output`  
文件名示例：  
`a_share_2025q4_24h_once_result_20260312_212530.xlsx`

### 适用场景

- 每日/每小时滚动看“近 24 小时（或自定义小时）新公告”的高增长公司
- 替代原 `run_once_a_share_2025q4_24h_scan.ps1` 运行链路

## 如何选择

- 需要“通用季度分析、覆盖面更全”：用 `scan_a_share_quarterly_revenue_growth.py`
- 需要“最近 N 小时公告窗口监控”：用 `run_once_a_share_2025q4_24h_scan.py`
