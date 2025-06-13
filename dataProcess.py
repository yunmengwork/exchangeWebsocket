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
    renameDict = {
        "ts": "timestamp",
        "fundingRate": "fundingRate",
        "nextFundingTime": "nextFundingTime",
        "indexPrice": "indexPrice",
        "askPr": "askPx",
        "askSz": "askSz",
        "bidPr": "bidPx",
        "bidSz": "bidSz",
    }
    file = f"./data/bitget/ticker/{symbol}.csv"
    if not os.path.exists(file):
        logger.error(f"File {file} does not exist.")
        return
    df = pd.read_csv(file)
    df.rename(columns=renameDict, inplace=True)
    # print(df.columns)
    df["timestamp"] = (df["timestamp"].astype(int) / 100).astype(int)
    df = df.groupby("timestamp").agg(
        {
            "fundingRate": "mean",
            "nextFundingTime": "median",
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
    bookTickerDf.rename(
        columns={
            "T": "timestamp",
            "a": "askPx",
            "A": "askSz",
            "b": "bidPx",
            "B": "bidSz",
        },
        inplace=True,
    )
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
    markPriceUpdateDf.rename(
        columns={
            "E": "timestamp",
            "i": "indexPrice",
            "r": "fundingRate",
            "T": "nextFundingTime",
        },
        inplace=True,
    )
    markPriceUpdateDf["timestamp"] = (
        markPriceUpdateDf["timestamp"].astype(int) / 100
    ).astype(int)
    markPriceUpdateDf = markPriceUpdateDf.groupby("timestamp").agg(
        {
            "fundingRate": "mean",
            "nextFundingTime": "median",
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
    fundingRateDf.rename(
        columns={
            "ts": "timestamp",
            "fundingRate": "fundingRate",
            "nextFundingTime": "nextFundingTime",
        },
        inplace=True,
    )
    indexPriceDf = pd.read_csv(indexPriceFile)
    indexPriceDf.rename(
        columns={
            "ts": "timestamp",
            "idxPx": "indexPrice",
        },
        inplace=True,
    )
    tickersDf = pd.read_csv(tickersFile)
    tickersDf.rename(
        columns={
            "ts": "timestamp",
            "askPx": "askPx",
            "askSz": "askSz",
            "bidPx": "bidPx",
            "bidSz": "bidSz",
        },
        inplace=True,
    )

    fundingRateDf["timestamp"] = (fundingRateDf["timestamp"].astype(int) / 100).astype(
        int
    )
    indexPriceDf["timestamp"] = (indexPriceDf["timestamp"].astype(int) / 100).astype(
        int
    )
    tickersDf["timestamp"] = (tickersDf["timestamp"].astype(int) / 100).astype(int)

    fundingRateDf = fundingRateDf.groupby("timestamp").agg(
        {"fundingRate": "mean", "nextFundingTime": "median"}
    )
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


def addFundingTimeAtFig(ax: plt.Axes, fundingTime: list[int], exchange: str):
    """在资金费率时间节点添加垂直线"""
    fundingTime = [
        pd.to_datetime(time, unit="ms", utc=True).tz_convert("Asia/Shanghai")
        for time in fundingTime
    ]
    for time in fundingTime:
        ax.axvline(x=time, color="r", linestyle="--", label=f"{exchange} funding time")
    return ax


def PlotOperations(
    df: pd.DataFrame,
    exchange1: str,
    exchange2: str,
    singleShow: bool = False,
    fundingTimeFlag: bool = False,
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

    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    ax.plot(
        df.index,
        df["operation1"],
        label=f"operation 1:在{exchange1}做maker long,在{exchange2}做taker short",
    )
    ax.plot(
        df.index,
        df["operation2"],
        label=f"operation 2:在{exchange1}做taker long,在{exchange2}做maker short",
    )
    ax.plot(
        df.index,
        df["operation3"],
        label=f"operation 3:在{exchange1}做maker short,在{exchange2}做taker long",
    )
    ax.plot(
        df.index,
        df["operation4"],
        label=f"operation 4:在{exchange1}做taker short,在{exchange2}做maker long",
    )
    ax.set_title("进场/出场利润率")
    ax.set_xlabel("时间")
    ax.set_ylabel("利润率")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper left")
    ax.grid()
    if fundingTimeFlag:
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
    if singleShow:
        plt.show()


def plotStrategies(
    df: pd.DataFrame,
    exchange1: str,
    exchange2: str,
    singleShow: bool = False,
    fundingTimeFlag: bool = False,
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
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    ax.plot(
        df.index,
        df["operation1"] + df["operation3"],
        label=f"strategy: operation 1+3",
    )
    ax.plot(
        df.index,
        df["operation2"] + df["operation4"],
        label=f"strategy: operation 2+4",
    )
    ax.plot(
        df.index,
        df["operation1"] + df["operation4"],
        label=f"strategy: operation 1+4",
    )
    ax.plot(
        df.index,
        df["operation2"] + df["operation3"],
        label=f"strategy: operation 2+3",
    )
    ax.set_title("策略组合")
    ax.set_xlabel("时间")
    ax.set_ylabel("利润率")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper left")
    ax.grid()
    if fundingTimeFlag:
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
    if singleShow:
        plt.show()


def plotSpread(
    df: pd.DataFrame,
    exchange1: str,
    exchange2: str,
    singleShow: bool = False,
    fundingTimeFlag: bool = False,
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
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    ax.plot(
        df.index,
        df["spread_" + exchange1] / df["indexPrice_" + exchange1],
        label=f"spread {exchange1}",
    )
    ax.plot(
        df.index,
        df["spread_" + exchange2] / df["indexPrice_" + exchange2],
        label=f"spread {exchange2}",
    )
    ax.set_title("买卖一档价差")
    ax.set_xlabel("时间")
    ax.set_ylabel("价差率")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper left")
    ax.grid()
    if fundingTimeFlag:
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
    if singleShow:
        plt.show()


def plotFundingRate(
    df: pd.DataFrame,
    exchange1: str,
    exchange2: str,
    singleShow: bool = False,
    fundingTimeFlag: bool = False,
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
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    ax.plot(
        df.index,
        df["fundingRate_" + exchange1],
        label=f"funding rate {exchange1}",
    )
    ax.plot(
        df.index,
        df["fundingRate_" + exchange2],
        label=f"funding rate {exchange2}",
    )
    ax.set_title("资金费率")
    ax.set_xlabel("时间")
    ax.set_ylabel("资金费率")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper left")
    ax.grid()
    if fundingTimeFlag:
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
    if singleShow:
        plt.show()


def plotPairAskBidPriceInterval(
    df: pd.DataFrame,
    exchange1: str,
    exchange2: str,
    singleShow: bool = False,
    fundingTimeFlag: bool = False,
):
    """
    绘制两个交易所的买一卖一价差图表
    """
    if df.empty:
        logger.error("DataFrame is empty, cannot plot pair ask-bid price interval.")
        return

    # 确保索引是时间戳格式
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit="ms")

    # 绘制买一卖一价差图表
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    ax.plot(
        df.index,
        (df["askPx_" + exchange1] - df["askPx_" + exchange2])
        / (df["askPx_" + exchange1] + df["askPx_" + exchange2]),
        label=f"{exchange1} ask - {exchange2} ask",
    )
    ax.plot(
        df.index,
        (df["bidPx_" + exchange1] - df["bidPx_" + exchange2])
        / (df["bidPx_" + exchange1] + df["bidPx_" + exchange2]),
        label=f"{exchange1} bid - {exchange2} bid",
    )
    ax.set_title("交易所买一卖一价差")
    ax.set_xlabel("时间")
    ax.set_ylabel("价差率")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper left")
    ax.grid()
    if fundingTimeFlag:
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax = addFundingTimeAtFig(
            ax, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
    if singleShow:
        plt.show()


def plotMiddlePriceMove(
    df: pd.DataFrame,
    exchange1: str,
    exchange2: str,
    singleShow: bool = False,
    fundingTimeFlag: bool = False,
):
    """
    绘制两个交易所的中间价格移动图表
    :param df: 包含中间价格数据的DataFrame
    """
    if df.empty:
        logger.error("DataFrame is empty, cannot plot middle price move.")
        return

    # 确保索引是时间戳格式
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit="ms")

    # 绘制中间价格移动图表
    fig = plt.figure(figsize=(14, 8))
    ax1 = fig.add_subplot(2, 1, 1)
    ax1.plot(
        df.index,
        (df["askPx_" + exchange1] + df["bidPx_" + exchange1]) / 2,
        label=f"{exchange1} mid price",
    )
    ax1.plot(
        df.index,
        (df["askPx_" + exchange2] + df["bidPx_" + exchange2]) / 2,
        label=f"{exchange2} mid price",
    )
    ax1.set_title("交易所中间价格移动")
    # ax1.set_xlabel("时间")
    ax1.set_ylabel("中间价格")
    ax1.tick_params(axis="x", rotation=45)
    ax1.legend(loc="upper left")
    ax1.grid()
    if fundingTimeFlag:
        ax1 = addFundingTimeAtFig(
            ax1, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax1 = addFundingTimeAtFig(
            ax1, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
    ax2 = fig.add_subplot(2, 1, 2)
    ax2.plot(
        df.index,
        (
            (df["askPx_" + exchange1] + df["bidPx_" + exchange1]) / 2
            - (df["askPx_" + exchange2] + df["bidPx_" + exchange2]) / 2
        )
        / (
            (
                df["askPx_" + exchange1]
                + df["bidPx_" + exchange1]
                + df["askPx_" + exchange2]
                + df["bidPx_" + exchange2]
            )
            / 4
        ),
        label=f"{exchange1} mid - {exchange2} mid",
    )
    ax2.set_title("交易所中间价格差")
    ax2.set_xlabel("时间")
    ax2.set_ylabel("中间价格差率")
    ax2.tick_params(axis="x", rotation=45)
    ax2.legend(loc="upper left")
    ax2.grid()
    if fundingTimeFlag:
        ax2 = addFundingTimeAtFig(
            ax2, df["nextFundingTime_" + exchange1].unique(), exchange1
        )
        ax2 = addFundingTimeAtFig(
            ax2, df["nextFundingTime_" + exchange2].unique(), exchange2
        )
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
            plotPairAskBidPriceInterval(df, exchange_1, exchange_2)
            plotMiddlePriceMove(df, exchange_1, exchange_2)
            plotFundingRate(df, exchange_1, exchange_2, fundingTimeFlag=True)
    plt.show()


analyze("RVNUSDT", Exchange.BINANCE, Exchange.BITGET)
