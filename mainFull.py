from exchange.okx import Okx, getAllOkxSymbols
from exchange.bitget import Bitget, getAllBitgetSymbols
from exchange.binance import Binance, getAllBinanceSymbols
from exchange.bybit import Bybit
from loguru import logger
import asyncio
import os
import json
import time
import multiprocessing


exchanges = ["okx", "bitget", "binance", "bybit"]
for e in exchanges:
    if not os.path.exists(f"data/{e}"):
        os.makedirs(f"data/{e}")

# [timestamp, fundingRate, indexPrice, askPx, askSz, bidPx, bidSz]


def okxMsgHandler(msg):
    # logger.debug(msg)
    try:
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
                    f.write(f"timestamp,fundingRate\n")
            with open(saveFile, "a+") as f:
                f.write(f"{data['ts']},{data['fundingRate']}\n")
        if channel == "index-tickers":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"timestamp,indexPrice\n")
            with open(saveFile, "a+") as f:
                f.write(f"{data['ts']},{data['idxPx']}\n")
        if channel == "tickers":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"timestamp,askPx,askSz,bidPx,bidSz\n")
            with open(saveFile, "a+") as f:
                f.write(
                    f"{data['ts']},{data['askPx']},{data['askSz']},{data['bidPx']},{data['bidSz']}\n"
                )
    except Exception as e:
        logger.error(msg)
        logger.error(e)


def _binanceMsgHandler(msg: dict):  # 单条数据
    try:
        if "e" not in msg.keys():
            return
        event = msg["e"]
        symbol = msg["s"]
        if not os.path.exists(f"data/binance/{event}/"):
            os.makedirs(f"data/binance/{event}/")
        saveFile = f"data/binance/{event}/{symbol}.csv"
        if event == "bookTicker":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"timestamp,askPx,askSz,bidPx,bidSz\n")
            with open(saveFile, "a+") as f:
                f.write(f"{msg['T']},{msg['a']},{msg['A']},{msg['b']},{msg['B']}\n")
        elif event == "markPriceUpdate":
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"timestamp,fundingRate,indexPrice\n")
            with open(saveFile, "a+") as f:
                f.write(f"{msg['E']},{msg['r']},{msg['i']}\n")
        else:
            logger.debug(msg)
    except Exception as e:
        logger.error(msg)
        logger.error(e)


def binanceMsgHandler(msg):
    # logger.debug(msg)
    try:
        msg = json.loads(msg)
        if isinstance(msg, list):
            for m in msg:
                _binanceMsgHandler(m)
        else:
            _binanceMsgHandler(msg)
    except Exception as e:
        logger.error(msg)
        logger.error(e)


def bitgetMsgHandler(msg):
    # logger.debug(msg)
    try:
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
                    f.write(
                        f"timestamp,fundingRate,indexPrice,askPx,askSz,bidPx,bidSz\n"
                    )
            with open(saveFile, "a+") as f:
                f.write(
                    f"{data['ts']},{data['fundingRate']},{data['indexPrice']},{data['askPr']},{data['askSz']},{data['bidPr']},{data['bidSz']}\n"
                )
    except Exception as e:
        logger.error(msg)
        logger.error(e)


class OkxExtend(Okx):
    def __init__(
        self, url, needLogin, apikey=None, secret=None, passphrase=None, *args, **kwargs
    ):
        super().__init__(url, needLogin, apikey, secret, passphrase, *args, **kwargs)

    async def _processRecv(self, recvMsg):
        okxMsgHandler(recvMsg)


class BinanceExtend(Binance):
    def __init__(self, url, needLogin, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def _processRecv(self, recvMsg):
        binanceMsgHandler(recvMsg)


class BitgetExtend(Bitget):
    def __init__(self, url, needLogin, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def _processRecv(self, recvMsg):
        bitgetMsgHandler(recvMsg)


# 从okx获取资金费率，指数价格，买卖一档
okxPublicWss = "wss://wspap.okx.com:8443/ws/v5/public"
okxDict: dict[str, OkxExtend] = {}
# 订阅
okxSubscribeBatchSize = 50  # 每批订阅的数量
okxArgs = []
allOkxSymbols = getAllOkxSymbols()
if not allOkxSymbols:
    logger.error("No OKX symbols found. Exiting.")
    exit(1)
for okxCoin in allOkxSymbols:
    okxArgs.append({"channel": "funding-rate", "instId": f"{okxCoin}"})
    okxArgs.append({"channel": "index-tickers", "instId": f"{okxCoin.rstrip('-SWAP')}"})
    okxArgs.append({"channel": "tickers", "instId": f"{okxCoin}"})

# 从binance获取买卖一档，指数价格，资金费率
# binancePublicWss = "wss://stream.binance.com:9443/ws" # 现货的ws
binancePublicWss = "wss://fstream.binance.com/ws"  # 期货的ws
binanceDict: dict[str, BinanceExtend] = {}
binanceSubscribeBatchSize = 50  # 每批订阅的数量
binanceArgs = [
    "!markPrice@arr",
]
allBinanceSymbols = getAllBinanceSymbols()
for binanceCoin in allBinanceSymbols:
    binanceArgs.append("".join([binanceCoin.lower(), "@bookTicker"]))


# 从bitget获取资金费率，指数价格，买卖一档
bitgetPublicWss = "wss://ws.bitget.com/v2/ws/public"
bitgetDict: dict[str, BitgetExtend] = {}
bitgetSubscribeBatchSize = 50  # 每批订阅的数量
bitgetArgs = []
allBitgetSymbols = getAllBitgetSymbols()
for bitgetCoin in allBitgetSymbols:
    bitgetArgs.append(
        {"instType": "USDT-FUTURES", "channel": "ticker", "instId": f"{bitgetCoin}"}
    )


async def _okxRun():
    tasks = []
    for i in range(int(len(okxArgs) / okxSubscribeBatchSize + 1)):
        okxDict[f"okx_{i}"] = OkxExtend(okxPublicWss, False)
        if (i + 1) * okxSubscribeBatchSize > len(okxArgs):
            okxArgsBatch = okxArgs[i * okxSubscribeBatchSize : len(okxArgs)]
        else:
            okxArgsBatch = okxArgs[
                i * okxSubscribeBatchSize : (i + 1) * okxSubscribeBatchSize
            ]
        taskSubscribe = asyncio.create_task(okxDict[f"okx_{i}"].subscribe(okxArgsBatch))
        taskRun = asyncio.create_task(okxDict[f"okx_{i}"].run())
        tasks.append(taskSubscribe)
        tasks.append(taskRun)
    await asyncio.gather(*tasks)


def okxRun():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_okxRun())
    loop.close()


async def _binanceRun():
    tasks = []
    for i in range(int(len(binanceArgs) / binanceSubscribeBatchSize + 1)):
        binanceDict[f"binance_{i}"] = BinanceExtend(binancePublicWss, False)
        if (i + 1) * binanceSubscribeBatchSize > len(binanceArgs):
            binanceArgsBatch = binanceArgs[
                i * binanceSubscribeBatchSize : len(binanceArgs)
            ]
        else:
            binanceArgsBatch = binanceArgs[
                i * binanceSubscribeBatchSize : (i + 1) * binanceSubscribeBatchSize
            ]
        taskSubscribe = asyncio.create_task(
            binanceDict[f"binance_{i}"].subscribe(binanceArgsBatch)
        )
        taskRun = asyncio.create_task(binanceDict[f"binance_{i}"].run())
        tasks.append(taskSubscribe)
        tasks.append(taskRun)
    await asyncio.gather(*tasks)


def binanceRun():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_binanceRun())
    loop.close()


async def _bitgetRun():
    tasks = []
    for i in range(int(len(bitgetArgs) / bitgetSubscribeBatchSize + 1)):
        bitgetDict[f"bitget_{i}"] = BitgetExtend(bitgetPublicWss, False)
        if (i + 1) * bitgetSubscribeBatchSize > len(bitgetArgs):
            bitgetArgsBatch = bitgetArgs[i * bitgetSubscribeBatchSize : len(bitgetArgs)]
        else:
            bitgetArgsBatch = bitgetArgs[
                i * bitgetSubscribeBatchSize : (i + 1) * bitgetSubscribeBatchSize
            ]
        taskSubscribe = asyncio.create_task(
            bitgetDict[f"bitget_{i}"].subscribe(bitgetArgsBatch)
        )
        taskRun = asyncio.create_task(bitgetDict[f"bitget_{i}"].run())
        tasks.append(taskSubscribe)
        tasks.append(taskRun)
    await asyncio.gather(*tasks)


def bitgetRun():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_bitgetRun())
    loop.close()


if __name__ == "__main__":
    processList = []
    tasks = [okxRun, binanceRun, bitgetRun]
    # tasks = [binanceRun]
    for task in tasks:
        p = multiprocessing.Process(target=task)
        p.start()
        processList.append(p)
    for p in processList:
        p.join()
        if p.exitcode != 0:
            logger.error(f"Process {p.name} exited with code {p.exitcode}")
        else:
            logger.info(f"Process {p.name} completed successfully.")
