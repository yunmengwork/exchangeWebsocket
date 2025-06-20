import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 将父目录添加到系统路径

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataProcess import binanceDataReader, bitgetDataReader
import argparse
from scipy import stats

# 设置参数解析器
parser = argparse.ArgumentParser(description="ARMA Model Analysis")
parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to analyze")

args = parser.parse_args()
symbol = args.symbol if args.symbol.endswith("USDT") else args.symbol + "USDT"

# 读取数据
binanceDf = binanceDataReader(symbol)
bitgetDf = bitgetDataReader(symbol)
# 合并数据
df = pd.merge(binanceDf, bitgetDf, on="timestamp", suffixes=("_binance", "_bitget"))
df.index = pd.to_datetime(df.index * 100, unit="ms")
df.ffill(inplace=True)  # 向前填充缺失值
df.dropna(inplace=True)
df["askDiff"] = df["askPx_binance"] - df["askPx_bitget"]
#
ts = df["askDiff"]

# 聚合ts,按10s聚合
# ts = ts.resample("5S").last()
# ts.ffill(inplace=True)  # 向前填充缺失值
#
observeWindow = 800
timeWindow = int(observeWindow * 0.08)
interval = 10
threshold = 0.05
plt.figure(figsize=(12, 6))
plt.axvline(x=ts.index[observeWindow], color="black", linestyle="--", linewidth=0.5)
for i in range((len(ts) - observeWindow - timeWindow) // interval - 1):
    start = i * interval
    end = start + observeWindow
    # series = ts[start:end]

    #
    observeSeries = ts[start:end]
    extendedSeries = ts[start : end + timeWindow]

    # 双均值检验
    t_stat, p_value = stats.ttest_ind(observeSeries, extendedSeries)
    print(f"Window {i}: t-statistic = {t_stat}, p-value = {p_value}")
    # 在end+timeWindow处添加竖线
    if p_value < threshold:
        plt.axvline(x=ts.index[end + timeWindow], color="green", linestyle="--")

# 绘制全部数据
plt.plot(ts.index, ts, label="Original Series", alpha=0.5)
plt.legend()
plt.savefig("./model/images/{}_think1.png".format(symbol))
plt.show()
