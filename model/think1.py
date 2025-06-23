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
parser.add_argument(
    "--plotObserveWindow",
    type=bool,
    default=False,
    help="Whether to plot the observation window",
)
parser.add_argument(
    "--pltShow",
    type=bool,
    default=True,
    help="Whether to show the plot",
)

args = parser.parse_args()
symbol = args.symbol if args.symbol.endswith("USDT") else args.symbol + "USDT"
symbol = symbol.upper()
plotObserveWindow = args.plotObserveWindow
pltShow = args.pltShow

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

# 聚合ts, 按1s聚合
ts = ts.resample("1s").last()
ts.ffill(inplace=True)  # 向前填充缺失值
ts.dropna(inplace=True)
#
observeWindow = 60 * 60
timeWindow = int(observeWindow * 0.05)
interval = 10
threshold = 0.01
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
        if t_stat > 0:
            # 画出观测区间，用灰色填充观测区间
            if plotObserveWindow:
                plt.axvspan(ts.index[start], ts.index[end], color="gray", alpha=0.3)
            # 画出预测区间，用红色填充预测区间
            plt.axvspan(
                ts.index[end], ts.index[end + timeWindow], color="red", alpha=0.3
            )
        else:
            # 画出观测区间，用灰色填充观测区间
            if plotObserveWindow:
                plt.axvspan(ts.index[start], ts.index[end], color="gray", alpha=0.3)
            # 画出预测区间，用绿色填充预测区间
            plt.axvspan(
                ts.index[end], ts.index[end + timeWindow], color="green", alpha=0.3
            )

# 绘制全部数据
plt.plot(ts.index, ts, label="Original Series", alpha=0.5)
plt.legend()
plt.savefig("./model/images/{}_think1.png".format(symbol))
if pltShow:
    plt.show()
plt.close()  # 关闭当前图形以释放内存
