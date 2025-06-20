import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dataProcess import binanceDataReader, bitgetDataReader
import argparse
from scipy import stats
import os


def plotTs(symbol, resample="100ms"):
    plt.figure(figsize=(15, 5))
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
    #
    ts = ts.resample(resample).last()
    ts.dropna(inplace=True)  # 删除缺失值

    ts.plot(figsize=(15, 5), title=f"{symbol} askPx_diff")
    plt.savefig(f"./images/ts/askPx_diff_{symbol}.png")


symbols = os.listdir("./data/binance/bookTicker/")
symbols = [s.split(".")[0] for s in symbols if s.endswith(".csv")]
for symbol in symbols:
    try:
        plotTs(symbol, resample="100ms")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
