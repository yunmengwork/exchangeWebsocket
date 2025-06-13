import pandas as pd
import numpy as np
import os
from loguru import logger
import matplotlib.pyplot as plt
from enum import Enum

# 显示中文
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 指定默认字体
plt.rcParams["axes.unicode_minus"] = False  # 解决保存图像是负号'-'显示为方块的问题


def bitgetDataReader(symbol):
    file = f"./data/bitget/ticker/{symbol}.csv"
    if not os.path.exists(file):
        logger.error(f"File {file} does not exist.")
        return
    df = pd.read_csv(file)
    # print(df.columns)
    df["timestamp"] = (df["timestamp"].astype(int) / 100).astype(int)
    df = df.groupby("timestamp").agg(
        {
            "fundingRate": "mean",
            "indexPrice": "mean",
            "askPx": "mean",
            "askSz": "mean",
            "bidPx": "mean",
            "bidSz": "mean",
        }
    )
    return df


def binanceDataReader(symbol):
    bookTickerFile = f"./data/binance/bookTicker/{symbol}.csv"
    markPriceUpdateFile = f"./data/binance/markPriceUpdate/{symbol}.csv"
    if not os.path.exists(bookTickerFile):
        logger.error(f"File {bookTickerFile} does not exist.")
        return
    if not os.path.exists(markPriceUpdateFile):
        logger.error(f"File {markPriceUpdateFile} does not exist.")
        return
    bookTickerDf = pd.read_csv(bookTickerFile)
    bookTickerDf["timestamp"] = (bookTickerDf["timestamp"].astype(int) / 100).astype(
        int
    )
    bookTickerDf = bookTickerDf.groupby("timestamp").agg(
        {
            "askPx": "mean",
            "askSz": "mean",
            "bidPx": "mean",
            "bidSz": "mean",
        }
    )
    markPriceUpdateDf = pd.read_csv(markPriceUpdateFile)
    markPriceUpdateDf["timestamp"] = (
        markPriceUpdateDf["timestamp"].astype(int) / 100
    ).astype(int)
    markPriceUpdateDf = markPriceUpdateDf.groupby("timestamp").agg(
        {
            "fundingRate": "mean",
            "indexPrice": "mean",
        }
    )
    df = pd.merge(
        markPriceUpdateDf,
        bookTickerDf,
        on="timestamp",
        how="right",
    )
    df.ffill(inplace=True)
    df.dropna(inplace=True)
    return df


def okxDataReader(symbol):
    symbol = symbol.strip("USDT") + "-USDT"
    fundingRateFile = f"./data/okx/funding-rate/{symbol}-SWAP.csv"
    indexPriceFile = f"./data/okx/index-tickers/{symbol}.csv"
    tickersFile = f"./data/okx/tickers/{symbol}-SWAP.csv"

    if not os.path.exists(fundingRateFile):
        logger.error(f"File {fundingRateFile} does not exist.")
        return
    if not os.path.exists(indexPriceFile):
        logger.error(f"File {indexPriceFile} does not exist.")
        return
    if not os.path.exists(tickersFile):
        logger.error(f"File {tickersFile} does not exist.")
        return

    fundingRateDf = pd.read_csv(fundingRateFile)
    indexPriceDf = pd.read_csv(indexPriceFile)
    tickersDf = pd.read_csv(tickersFile)

    fundingRateDf["timestamp"] = (fundingRateDf["timestamp"].astype(int) / 100).astype(
        int
    )
    indexPriceDf["timestamp"] = (indexPriceDf["timestamp"].astype(int) / 100).astype(
        int
    )
    tickersDf["timestamp"] = (tickersDf["timestamp"].astype(int) / 100).astype(int)

    fundingRateDf = fundingRateDf.groupby("timestamp").agg({"fundingRate": "mean"})
    indexPriceDf = indexPriceDf.groupby("timestamp").agg({"indexPrice": "mean"})
    tickersDf = tickersDf.groupby("timestamp").agg(
        {
            "askPx": "mean",
            "askSz": "mean",
            "bidPx": "mean",
            "bidSz": "mean",
        }
    )

    df = pd.merge(fundingRateDf, indexPriceDf, on="timestamp", how="outer")
    df = pd.merge(df, tickersDf, on="timestamp", how="outer")
    df.ffill(inplace=True)
    df.dropna(inplace=True)
    return df


def analyzeData(dfDict: dict[str, pd.DataFrame], feeRate: float = 0.00025):
    if len(dfDict) != 2:
        logger.error("df数量不等于2，无法分析")
        return

    exchange1, exchange2 = list(dfDict.keys())
    df1 = dfDict.get(exchange1)
    df2 = dfDict.get(exchange2)

    if df1 is None or df2 is None:
        return

    # 根据index合并
    df = pd.merge(
        df1,
        df2,
        on="timestamp",
        how="left",
        suffixes=("_" + exchange1, "_" + exchange2),
    )
    df.dropna(inplace=True)

    df.index = pd.to_datetime(
        df.index.to_series() * 100, unit="ms", utc=True
    ).dt.tz_convert("Asia/Shanghai")

    #  在exchange1做maker long,在exchange2做taker short
    df["operation1"] = (
        df["bidPx_" + exchange2] / df["bidPx_" + exchange1] - 1
    ) - feeRate
    # 在exchange1做taker long,在exchange2做maker short
    df["operation2"] = (
        df["askPx_" + exchange2] / df["askPx_" + exchange1] - 1
    ) - feeRate
    # 在exchange1做maker short,在exchange2做taker long
    df["operation3"] = (
        df["bidPx_" + exchange1] / df["bidPx_" + exchange2] - 1
    ) - feeRate
    # 在exchange1做taker short,在exchange2做maker long
    df["operation4"] = (
        +(df["askPx_" + exchange1] / df["askPx_" + exchange2] - 1) - feeRate
    )

    df["spread_" + exchange1] = df["askPx_" + exchange1] - df["bidPx_" + exchange1]
    df["spread_" + exchange2] = df["askPx_" + exchange2] - df["bidPx_" + exchange2]

    return df, exchange1, exchange2


def PlotOperations(
    df: pd.DataFrame, exchange1: str, exchange2: str, singleShow: bool = False
):
    """
    绘制策略图表
    :param df: 包含策略数据的DataFrame
    """
    if df.empty:
        logger.error("DataFrame is empty, cannot plot strategies.")
        return

    # 确保索引是时间戳格式
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit="ms")

    # 绘制策略图表

    plt.figure(figsize=(14, 8))
    plt.plot(
        df.index,
        df["operation1"],
        label=f"operation 1:在{exchange1}做maker long,在{exchange2}做taker short",
    )
    plt.plot(
        df.index,
        df["operation2"],
        label=f"operation 2:在{exchange1}做taker long,在{exchange2}做maker short",
    )
    plt.plot(
        df.index,
        df["operation3"],
        label=f"operation 3:在{exchange1}做maker short,在{exchange2}做taker long",
    )
    plt.plot(
        df.index,
        df["operation4"],
        label=f"operation 4:在{exchange1}做taker short,在{exchange2}做maker long",
    )
    plt.title("进场/出场利润率")
    plt.xlabel("时间")
    plt.ylabel("利润率")
    plt.xticks(rotation=45)
    plt.legend(loc="upper left")
    plt.grid()
    if singleShow:
        plt.show()


def plotStrategies(
    df: pd.DataFrame, exchange1: str, exchange2: str, singleShow: bool = False
):
    """
    绘制策略图表
    :param df: 包含策略数据的DataFrame
    """
    if df.empty:
        logger.error("DataFrame is empty, cannot plot strategies.")
        return

    # 确保索引是时间戳格式
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit="ms")

    # 绘制策略图表
    plt.figure(figsize=(14, 8))
    plt.plot(
        df.index,
        df["operation1"] + df["operation3"],
        label=f"strategy: operation 1+3",
    )
    plt.plot(
        df.index,
        df["operation2"] + df["operation4"],
        label=f"strategy: operation 2+4",
    )
    plt.plot(
        df.index,
        df["operation1"] + df["operation4"],
        label=f"strategy: operation 1+4",
    )
    plt.plot(
        df.index,
        df["operation2"] + df["operation3"],
        label=f"strategy: operation 2+3",
    )
    plt.title("策略组合")
    plt.xlabel("时间")
    plt.ylabel("利润率")
    plt.xticks(rotation=45)
    plt.legend(loc="upper left")
    plt.grid()
    if singleShow:
        plt.show()


def plotSpread(
    df: pd.DataFrame, exchange1: str, exchange2: str, singleShow: bool = False
):
    """
    绘制价差图表
    :param df: 包含价差数据的DataFrame
    """
    if df.empty:
        logger.error("DataFrame is empty, cannot plot spread.")
        return

    # 确保索引是时间戳格式
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit="ms")

    # 绘制价差图表
    plt.figure(figsize=(14, 8))
    plt.plot(
        df.index,
        df["spread_" + exchange1] / df["indexPrice_" + exchange1],
        label=f"spread {exchange1}",
    )
    plt.plot(
        df.index,
        df["spread_" + exchange2] / df["indexPrice_" + exchange2],
        label=f"spread {exchange2}",
    )
    plt.title("买卖一档价差")
    plt.xlabel("时间")
    plt.ylabel("价差率")
    plt.xticks(rotation=45)
    plt.legend(loc="upper left")
    plt.grid()
    if singleShow:
        plt.show()


def plotFundingRate(
    df: pd.DataFrame, exchange1: str, exchange2: str, singleShow: bool = False
):
    """
    绘制资金费率图表
    :param df: 包含资金费率数据的DataFrame
    """
    if df.empty:
        logger.error("DataFrame is empty, cannot plot funding rate.")
        return

    # 确保索引是时间戳格式
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit="ms")

    # 绘制资金费率图表
    plt.figure(figsize=(14, 8))
    plt.plot(
        df.index,
        df["fundingRate_" + exchange1],
        label=f"funding rate {exchange1}",
    )
    plt.plot(
        df.index,
        df["fundingRate_" + exchange2],
        label=f"funding rate {exchange2}",
    )
    plt.title("资金费率")
    plt.xlabel("时间")
    plt.ylabel("资金费率")
    plt.xticks(rotation=45)
    plt.legend(loc="upper left")
    plt.grid()
    if singleShow:
        plt.show()


class Exchange(Enum):
    BITGET = "bitget"
    BINANCE = "binance"
    OKX = "okx"


def analyze(symbol: str, exchange1: Exchange, exchange2: Exchange):
    exchangeDict = {
        Exchange.BITGET: bitgetDataReader,
        Exchange.BINANCE: binanceDataReader,
        Exchange.OKX: okxDataReader,
    }
    df1 = exchangeDict[exchange1](symbol)
    df2 = exchangeDict[exchange2](symbol)
    if df1 is not None and df2 is not None:
        df, exchange_1, exchange_2 = analyzeData(
            {exchange1.value: df1, exchange2.value: df2}, feeRate=0.0001
        )
        if df is not None:
            PlotOperations(df, exchange_1, exchange_2)
            plotStrategies(df, exchange_1, exchange_2)
            plotSpread(df, exchange_1, exchange_2)
            plotFundingRate(df, exchange_1, exchange_2)
    plt.show()


analyze("BTCUSDT", Exchange.OKX, Exchange.BINANCE)
