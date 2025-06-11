import pandas as pd
import numpy as np
import os


def bitgetDataProcess(symbol):
    file = f"./data/bitget/ticker/{symbol}.csv"
    if not os.path.exists(file):
        return
    df = pd.read_csv(file)
    print(df.columns)
    df["timestamp"] = df["timestamp"].astype(int) / 100
    df = df.groupby("timestamp").agg(
        {
            "fundingRate": np.mean,
            "indexPrice": np.mean,
            "askPx": np.mean,
            "askSz": np.mean,
            "bidPx": np.mean,
            "bidSz": np.mean,
        }
    )
    return df


def binanceDataProcess(symbol):
    bookTickerFile = f"./data/binance/bookTicker/{symbol}.csv"
    markPriceUpdateFile = f"./data/binance/markPriceUpdate/{symbol}.csv"
    if not os.path.exists(bookTickerFile) or not os.path.exists(markPriceUpdateFile):
        return
    bookTickerDf = pd.read_csv(bookTickerFile)
    bookTickerDf["timestamp"] = (bookTickerDf["timestamp"].astype(int) / 100).astype(
        int
    )
    bookTickerDf = bookTickerDf.groupby("timestamp").agg(
        {
            "askPx": np.mean,
            "askSz": np.mean,
            "bidPx": np.mean,
            "bidSz": np.mean,
        }
    )
    markPriceUpdateDf = pd.read_csv(markPriceUpdateFile)
    markPriceUpdateDf["timestamp"] = (
        markPriceUpdateDf["timestamp"].astype(int) / 100
    ).astype(int)
    markPriceUpdateDf = markPriceUpdateDf.groupby("timestamp").agg(
        {
            "fundingRate": np.mean,
            "indexPrice": np.mean,
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


df = bitgetDataProcess("AXLUSDT")
print(df)
df = binanceDataProcess("AXLUSDT")
print(df)
