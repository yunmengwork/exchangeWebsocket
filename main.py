from exchange.okx import Okx
from exchange.bitget import Bitget
from exchange.binance import Binance
from exchange.bybit import Bybit
from exchange.handler import (
    okxSingleMsgHandler,
    bitgetSingleMsgHandler,
    binanceSingleMsgHandler,
)
from loguru import logger
import asyncio
import os
import json
import time

exchanges = ["okx", "bitget", "binance", "bybit"]
for e in exchanges:
    if not os.path.exists(f"data/{e}"):
        os.makedirs(f"data/{e}")

# [timestamp, fundingRate, indexPrice, askPx, askSz, bidPx, bidSz]


def okxMsgHandler(msg):
    # logger.debug(msg)
    return okxSingleMsgHandler(msg)


def bitgetMsgHandler(msg):
    # logger.debug(msg)
    return bitgetSingleMsgHandler(msg)


def binanceMsgHandler(msg):
    # logger.debug(msg)
    return binanceSingleMsgHandler(msg)


def bybitMsgHandler(msg):
    # logger.debug(msg)
    try:
        msg = json.loads(msg)
        if "op" in msg.keys():
            logger.debug(msg)
            return
        channel = msg["topic"].split(".")[0]
        symbol = msg["data"]["symbol"]
        if not os.path.exists(f"data/bybit/{channel}/"):
            os.makedirs(f"data/bybit/{channel}/")
        saveFile = f"data/bybit/{channel}/{symbol}.csv"
        if channel == "tickers":
            with open(saveFile, "a+") as f:
                f.write(f"{json.dumps(msg)}\n")
        else:
            logger.debug(msg)
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


class BitgetExtend(Bitget):
    def __init__(self, url, needLogin, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def _processRecv(self, recvMsg):
        bitgetMsgHandler(recvMsg)


class BinanceExtend(Binance):
    def __init__(self, url, needLogin, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def _processRecv(self, recvMsg):
        binanceMsgHandler(recvMsg)


class BybitExtend(Bybit):
    def __init__(self, url, needLogin, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def _processRecv(self, recvMsg):
        bybitMsgHandler(recvMsg)


# Coin
coins = ["KMNO", "ANIME", "SPOH", "MASK", "RVN", "SOON"]
# coins = ["MOVE"]
okxCoins = coins
bitgetCoins = coins
binanceCoins = coins
bybitCoins = coins

# 从okx获取资金费率，指数价格，买卖一档
okxPublicWss = "wss://wspap.okx.com:8443/ws/v5/public"
okx = OkxExtend(okxPublicWss, False)
# 订阅
okxArgs = []
for okxCoin in okxCoins:
    okxArgs.append({"channel": "funding-rate", "instId": f"{okxCoin}-USDT-SWAP"})
    okxArgs.append({"channel": "index-tickers", "instId": f"{okxCoin}-USDT"})
    okxArgs.append({"channel": "tickers", "instId": f"{okxCoin}-USDT"})


# 从bitget获取资金费率，指数价格，买卖一档
bitgetPublicWss = "wss://ws.bitget.com/v2/ws/public"
bitget = BitgetExtend(bitgetPublicWss, False)
bitgetArgs = [
    {"instType": "USDT-FUTURES", "channel": "ticker", "instId": f"{bitgetCoin}USDT"}
    for bitgetCoin in bitgetCoins
]

# 从binance获取买卖一档，指数价格，资金费率
# binancePublicWss = "wss://stream.binance.com:9443/ws" # 现货的ws
binancePublicWss = "wss://fstream.binance.com/ws"  # 期货的ws
binance = BinanceExtend(binancePublicWss, False, processRecvInterval=0.0001)
binanceArgs = []
for binanceCoin in binanceCoins:
    binanceArgs.append(f"{binanceCoin.lower()}usdt@bookTicker")
    binanceArgs.append(f"{binanceCoin.lower()}usdt@markPrice@1s")

# 从bybit获取数据
bybitPublicWss = "wss://stream.bybit.com/v5/public/linear"
bybit = BybitExtend(bybitPublicWss, False)
bybitArgs = [f"tickers.{bybitCoin}USDT" for bybitCoin in bybitCoins]


async def main():
    # await okx.subscribe(okxArgs)
    # await bitget.subscribe(bitgetArgs)
    await binance.subscribe(binanceArgs)
    # [await bybit.subscribe([bybitArg]) for bybitArg in bybitArgs]
    await asyncio.gather(
        # okx.run(),
        # bitget.run(),
        binance.run(),
        # bybit.run()
    )


asyncio.get_event_loop().run_until_complete(main())
