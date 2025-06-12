import pandas as pd
import numpy as np
import os


def bitgetDataProcess(symbol):
    file = f"./data/bitget/ticker/{symbol}.csv"
    if not os.path.exists(file):
        return
    df = pd.read_csv(file)
    print(df.columns)
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

symbols = ['ANIMEUSDT','AXLUSDT','DOLOUSDT','MASKUSDT','MOVEUSDT']
symbol = symbols[0]
df1 = bitgetDataProcess(symbol)
df2 = binanceDataProcess(symbol)
# 根据index合并
df = pd.merge(
    df1,
    df2,
    on="timestamp",
    how="left",
    suffixes=("_bitget", "_binance"),
)
df.dropna(inplace=True)

df.index = pd.to_datetime(
    df.index.to_series() * 100, unit="ms", utc=True
).dt.tz_convert("Asia/Shanghai")

feeRate = 0.00025
# 做多bitget做空binance,在bitget做maker long,在binance做taker short
df["stragegy1"] = (
    # -df["fundingRate_bitget"]
    # + df["fundingRate_binance"]
    +(df["bidPx_binance"] / df["bidPx_bitget"] - 1)
    - feeRate
)
# 做多bitget做空binance,在binance做maker short,在bitget做taker long
df["stragegy2"] = (
    # -df["fundingRate_bitget"]
    # + df["fundingRate_binance"]
    +(df["askPx_binance"] / df["askPx_bitget"] - 1)
    - feeRate
)
# 做多binance做空bitget,在binance做maker long,在bitget做taker short
df["stragegy3"] = (
    # +df["fundingRate_bitget"]
    # - df["fundingRate_binance"]
    +(df["bidPx_bitget"] / df["bidPx_binance"] - 1)
    - feeRate
)
# 做多binance做空bitget,在bitget做maker short,在binance做taker long
df["stragegy4"] = (
    # +df["fundingRate_bitget"]
    # - df["fundingRate_binance"]
    +(df["askPx_bitget"] / df["askPx_binance"] - 1)
    - feeRate
)

df['spread_bitget'] = df["askPx_bitget"]-df['bidPx_bitget']
df['spread_binance'] = df["askPx_binance"]-df['bidPx_binance']


# 查看效果
# print(df["stragegy1"].sort_values(ascending=False).head(20))
# print(df["stragegy2"].sort_values(ascending=False).head(20))
# print(df["stragegy3"].sort_values(ascending=False).head(20))
# print(df["stragegy4"].sort_values(ascending=False).head(20))


from matplotlib import pyplot as plt

# 显示中文
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 指定默认字体
plt.rcParams["axes.unicode_minus"] = False  # 解决保存图像是负号'-'显示为方块的问题


fig1 = plt.figure()
ax1 = fig1.add_subplot(1, 1, 1)
ax1.plot(
    df.index,
    df["stragegy1"],
    label="Strategy 1,在bitget做maker long,在binance做taker short",
)
ax1.plot(
    df.index,
    df["stragegy2"],
    label="Strategy 2,在binance做maker short,在bitget做taker long",
)
ax1.plot(
    df.index,
    df["stragegy3"],
    label="Strategy 3,在binance做maker long,在bitget做taker short",
)
ax1.plot(
    df.index,
    df["stragegy4"],
    label="Strategy 4,在bitget做maker short,在binance做taker long",
)
# x轴少些拥挤
ax1.xaxis.set_tick_params(rotation=45)
# 为每一条线打上标签
ax1.legend(loc="upper left")  # 指定图例位置

fig2 = plt.figure()
ax2 = fig2.add_subplot(1, 1, 1)
ax2.plot(df.index, df["stragegy1"] + df["stragegy3"], label="Strategy 1+3")
ax2.plot(df.index, df["stragegy2"] + df["stragegy4"], label="Strategy 2+4")
ax2.plot(df.index, df["stragegy1"] + df["stragegy4"], label="Strategy 1+4")
ax2.plot(df.index, df["stragegy2"] + df["stragegy3"], label="Strategy 2+3")
ax2.xaxis.set_tick_params(rotation=45)
ax2.legend(loc="upper left")


fig3 = plt.figure()
ax3 = fig3.add_subplot(1, 1, 1)
ax3.plot(df.index, df['spread_bitget'], label="spread_bitget")
ax3.plot(df.index, df["spread_binance"], label="spread_binance")
ax3.xaxis.set_tick_params(rotation=45)
ax3.legend(loc="upper left")


fig4 = plt.figure()
ax4 = fig4.add_subplot(1, 1, 1)
ax4.plot(df.index, df['fundingRate_bitget'], label="fundingRate_bitget")
ax4.plot(df.index, df["fundingRate_binance"], label="fundingRate_binance")
ax4.xaxis.set_tick_params(rotation=45)
ax4.legend(loc="upper left")


plt.show()
