# Analysis

一个用于存放多功能分析脚本的仓库骨架，当前只提供结构与工程规范，不包含具体业务脚本。

## 目录结构

```text
Analysis/
├── .github/
│   ├── workflows/
│   │   └── ci.yml
│   └── ISSUE_TEMPLATE/
│       ├── bug_report.md
│       └── feature_request.md
├── docs/
│   └── images/
├── src/
│   └── __init__.py
├── tests/
│   └── test_smoke.py
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
```

## 快速开始

1. 创建并激活虚拟环境
2. 安装依赖：`pip install -r requirements.txt`
3. 运行测试：`pytest -q`

## 开发约定

- 新功能脚本放在 `src/`
- 对应测试放在 `tests/`
- 文档和示意图放在 `docs/`
- 通过 Pull Request 合并改动
