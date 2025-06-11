import asyncio
import sys
import os
import json

sys.path.append(os.path.dirname(__file__) + "/..")
from lib.baseWebsocket import ExchangeWebsocket
from websockets.protocol import State


class Bybit(ExchangeWebsocket):
    def __init__(self, url: str, needLogin: bool, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def keepAlive(self):
        """保持连接"""
        while True:
            await asyncio.sleep(self.pingInterval)

            if self.ws and self.ws.state == State.OPEN:
                await self.ws.send(json.dumps({"op": "ping"}))

    async def subscribe(self, args):
        params = {"op": "subscribe", "args": args}
        await self.addRequest(params)

    async def processRecv(self):
        while True:
            if self.ws and self.ws.state == State.OPEN:
                recv = await self.ws.recv()
                if "op" in json.loads(recv).keys():
                    if json.loads(recv)["op"] == "ping":
                        continue
                await self._processRecv(recv)

            await asyncio.sleep(self.processRecvInterval)


if __name__ == "__main__":

    async def main():
        # url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"
        url = "wss://stream.bybit.com/v5/public/linear"
        bybit = Bybit(url, False)
        args = ["tickers.AXLUSDT"]
        await bybit.subscribe(args)
        await bybit.run()

    asyncio.get_event_loop().run_until_complete(main())
