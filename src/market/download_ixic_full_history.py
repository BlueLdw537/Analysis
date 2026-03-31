import requests
import pandas as pd

# 你的 FRED API Key
API_KEY = "88d629973c38600e5199cc5b4f2d19dc"

# FRED 序列代码：纳斯达克综合指数
SERIES_ID = "NASDAQCOM"

url = "https://api.stlouisfed.org/fred/series/observations"

params = {
    "series_id": SERIES_ID,
    "api_key": API_KEY,
    "file_type": "json"
}

resp = requests.get(url, params=params, timeout=30)
resp.raise_for_status()

data = resp.json()

# 提取 observations
obs = data["observations"]

df = pd.DataFrame(obs)

# 只保留需要的列
df = df[["date", "value"]].copy()

# 重命名
df.columns = ["Date", "Close"]

# 类型转换
df["Date"] = pd.to_datetime(df["Date"])
df["Close"] = pd.to_numeric(df["Close"], errors="coerce")

# 排序
df = df.sort_values("Date").reset_index(drop=True)

# 导出 Excel
output_file = "NASDAQCOM_FRED.xlsx"
df.to_excel(output_file, index=False, engine="openpyxl")

print(f"已导出: {output_file}")
print(df.head())
print(df.tail())
