# Analysis 架构说明

## 目标

在不牺牲可执行性的前提下，保持仓库结构清晰、入口固定、输出与源码隔离。

## 分层设计

- `src/`：只放可执行脚本入口（一个脚本解决一个分析任务）
- `docs/`：放说明文档与模板资源，不放运行产物
- `output/`：统一存放运行结果（Excel/CSV/JSON 等），默认不纳入版本控制
- `tests/`：放自动化测试（当前为仓库结构与烟雾测试）

## 目录职责

```text
Analysis/
├── src/                    # 功能脚本入口
├── docs/                   # 文档和导出模板
├── output/                 # 运行产物（git ignore）
├── tests/                  # 测试代码
├── requirements.txt        # 依赖
└── README.md               # 项目总览
```

## 约定

1. 新脚本命名使用动词前缀：
   - `scan_*.py`：扫描/筛选类任务
   - `run_*.py`：一次性执行或事件分析任务
2. 新脚本默认输出写入 `output/`，并支持 `--output-path` 或 `--output-dir` 覆盖。
3. 每新增脚本必须同步更新：
   - `src/README.md` 的脚本索引和命令示例
   - 必要的 `tests/` 用例（至少 smoke）
4. 避免把数据结果直接提交到 Git；如果确需保留示例，放到 `docs/` 并明确用途。

## 推荐开发流程

```powershell
cd Analysis
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -q
```
