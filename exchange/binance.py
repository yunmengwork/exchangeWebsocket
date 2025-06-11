import asyncio
import sys
import os
import json
import random
from loguru import logger

sys.path.append(os.path.dirname(__file__) + "/..")
from lib.baseWebsocket import ExchangeWebsocket
from websockets.protocol import State


class Binance(ExchangeWebsocket):
    def __init__(self, url: str, needLogin: bool, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def keepAlive(self):
        """保持连接"""
        while True:
            await asyncio.sleep(self.pingInterval)

            if self.ws and self.ws.state == State.OPEN:
                # 发送ping frame
                await self.ws.ping()

    async def processRecv(self):
        while True:
            if self.ws and self.ws.state == State.OPEN:
                recv = await self.ws.recv()
                if recv == "ping":
                    logger.info("recv ping")
                    await self.ws.pong()
                    continue
                await self._processRecv(recv)

            await asyncio.sleep(self.processRecvInterval)

    async def subscribe(self, args: list[str]):
        """这里需要以列表的形式输入订阅channel"""
        op = "SUBSCRIBE"
        params = {
            "method": op,
            "params": args,
            "id": random.randint(1, 10000),  # 请求 ID，需唯一
        }
        await self.addRequest(params)


if __name__ == "__main__":

    async def main():
        url = "wss://fstream.binance.com/ws"
        binance = Binance(url, False)
        args = ["btcusdt@markPrice@1s"]
        await binance.subscribe(args)
        await binance.run()

    asyncio.get_event_loop().run_until_complete(main())
