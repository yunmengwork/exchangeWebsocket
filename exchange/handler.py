import json
import os
from loguru import logger
import time
import math


def okxSingleMsgHandler(msg: str | dict):
    # logger.debug(msg)
    try:
        if isinstance(msg, str):
            msg = json.loads(msg)
        if "event" in msg.keys():
            return
        channel = msg["arg"]["channel"]
        data = msg["data"][0]
        if not os.path.exists(f"data/okx/{channel}/"):
            os.makedirs(f"data/okx/{channel}/")
        saveFile = f"data/okx/{channel}/{msg['arg']['instId']}.csv"
        if channel == "funding-rate":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(data.keys())}\n")
            with open(saveFile, "a+") as f:
                f.write(f"{','.join(str(data[col]) for col in data.keys())}\n")
        if channel == "index-tickers":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(data.keys())}\n")
            with open(saveFile, "a+") as f:
                f.write(f"{','.join(str(data[col]) for col in data.keys())}\n")
        if channel == "tickers":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(data.keys())}\n")
            with open(saveFile, "a+") as f:
                f.write(f"{','.join(str(data[col]) for col in data.keys())}\n")
    except Exception as e:
        logger.error(msg)
        logger.error(e)


def bitgetSingleMsgHandler(msg: str | dict):
    # logger.debug(msg)
    try:
        if isinstance(msg, str):
            msg = json.loads(msg)
        if "event" in msg.keys():
            return
        channel = msg["arg"]["channel"]
        data = msg["data"][0]
        if not os.path.exists(f"data/bitget/{channel}/"):
            os.makedirs(f"data/bitget/{channel}/")
        saveFile = f"data/bitget/{channel}/{msg['arg']['instId']}.csv"
        if channel == "ticker":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(data.keys())}\n")
            with open(saveFile, "a+") as f:
                f.write(f"{','.join(str(data[col]) for col in data.keys())}\n")
    except Exception as e:
        logger.error(msg)
        logger.error(e)


bookTickerCacheDict: dict[str, list[dict]] = {}
latestTimestamp: int = int(math.floor(time.time() * 1000))


def aggregateBookTickerCache(listCache: list[dict]):
    # e,s,b,B,a,A,T,E
    aggregatedData = {
        "e": "bookTicker",
        "s": listCache[-1]["s"],
        "b": listCache[-1]["b"],
        "B": sum(float(item["B"]) for item in listCache) / len(listCache),
        "a": listCache[-1]["a"],
        "A": sum(float(item["A"]) for item in listCache) / len(listCache),
        "T": listCache[-1]["T"],
        "E": listCache[-1]["E"],
    }
    return aggregatedData


def updateBookTickerCache(msg: dict):
    global bookTickerCacheDict
    global latestTimestamp
    symbol = msg["s"]
    msgTimeToSeconds = int(msg["T"]) // 100 * 100
    if symbol not in bookTickerCacheDict:
        bookTickerCacheDict[symbol] = []
    if msgTimeToSeconds > latestTimestamp:
        latestTimestamp = msgTimeToSeconds
        if len(bookTickerCacheDict[symbol]) == 0:
            bookTickerCacheDict[symbol].append(msg)
            return None
        else:
            # Aggregate the cached data
            aggData = aggregateBookTickerCache(bookTickerCacheDict[symbol])
            # Clear the cache for this symbol
            bookTickerCacheDict[symbol] = [msg]
            return aggData
    else:
        bookTickerCacheDict[symbol].append(msg)
        return None


def binanceSingleMsgHandler(msg: str | dict):
    try:
        if isinstance(msg, str):
            msg = json.loads(msg)
        if "e" not in msg.keys():
            return
        event = msg["e"]
        symbol = msg["s"]
        if not os.path.exists(f"data/binance/{event}/"):
            os.makedirs(f"data/binance/{event}/")
        saveFile = f"data/binance/{event}/{symbol}.csv"
        if event == "bookTicker":
            data = updateBookTickerCache(msg)
            if data is not None:
                if not os.path.exists(saveFile):
                    with open(saveFile, "a+") as f:
                        f.write(f"{','.join(data.keys())}\n")
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(str(data[col]) for col in data.keys())}\n")
        elif event == "markPriceUpdate":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(msg.keys())}\n")
            with open(saveFile, "a+") as f:
                f.write(f"{','.join(str(msg[col]) for col in msg.keys())}\n")
        else:
            logger.debug(msg)
    except Exception as e:
        logger.error(msg)
        logger.error(e)
