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
symbol = symbol.upper()

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

# 聚合ts,按3s聚合
# ts = ts.resample("3s").last()
# ts.ffill(inplace=True)  # 向前填充缺失值
# ts.dropna(inplace=True)  # 删除缺失值
#
observeWindow = 800
timeWindow = 20
interval = 10
# 截断上下阈值
clipLower = 0.1
clipUpper = 0.9
thresholdRate = 0.95
lowerThreshold = (clipLower ** (timeWindow * thresholdRate)) * (
    0.5 ** (timeWindow * (1 - thresholdRate))
)
upperThreshold = (clipUpper ** (timeWindow * thresholdRate)) * (
    0.5 ** (timeWindow * (1 - thresholdRate))
)
plt.figure(figsize=(12, 6))
plt.axvline(x=ts.index[observeWindow], color="black", linestyle="--", linewidth=0.5)
for i in range((len(ts) - observeWindow - timeWindow) // interval - 1):
    start = i * interval
    end = start + observeWindow

    #
    observeSeries = ts[start:end]
    predictedSeries = ts[end : end + timeWindow]

    # 使用observeSeries拟合正态分布，使用拟合的正态分布估计predictedSeries出现的概率
    mu, std = stats.norm.fit(observeSeries)
    # 计算predictedSeries的cdf
    predicted_cdf = stats.norm.cdf(predictedSeries, mu, std)
    # 需要对predicted_cdf数值进行截断
    predicted_cdf = np.clip(predicted_cdf, clipLower, clipUpper)  # 避免出现0或1的情况
    prob = predicted_cdf.cumprod()[-1]  # 取最后一个值作为结果
    benchmark = 1 / 2**timeWindow  # 设定一个基准值
    # prob/benchmark 越小，表示predictedSeries偏下，有下跌趋势
    # prob/benchmark 越大，表示predictedSeries偏上，有上涨趋势
    # if (prob / benchmark) < 1 / (threshold):
    #     # 画出观测区间，用灰色填充观测区间
    #     plt.axvspan(ts.index[start], ts.index[end], color="gray", alpha=0.3)
    #     # 在end+timeWindow处添加竖线
    #     plt.axvline(x=ts.index[end + timeWindow], color="green", linestyle="--")
    # if (prob / benchmark) > (1 / benchmark**0.9):
    #     # 画出观测区间，用灰色填充观测区间
    #     plt.axvspan(ts.index[start], ts.index[end], color="gray", alpha=0.3)
    #     # 在end+timeWindow处添加竖线
    #     plt.axvline(x=ts.index[end + timeWindow], color="red", linestyle="--")
    if prob < lowerThreshold:
        # 画出观测区间，用灰色填充观测区间
        plt.axvspan(ts.index[start], ts.index[end], color="gray", alpha=0.3)
        # 在end+timeWindow处添加竖线
        plt.axvline(x=ts.index[end + timeWindow], color="green", linestyle="--")
    if prob > upperThreshold:
        # 画出观测区间，用灰色填充观测区间
        plt.axvspan(ts.index[start], ts.index[end], color="gray", alpha=0.3)
        # 在end+timeWindow处添加竖线
        plt.axvline(x=ts.index[end + timeWindow], color="red", linestyle="--")


# 绘制全部数据
plt.plot(ts.index, ts, label="Original Series", alpha=0.5)
plt.legend()
plt.savefig("./model/images/{}_think2.png".format(symbol))
plt.show()
