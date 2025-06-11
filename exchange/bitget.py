import asyncio
import sys
import os
import json

sys.path.append(os.path.dirname(__file__) + "/..")
from lib.baseWebsocket import ExchangeWebsocket


class Bitget(ExchangeWebsocket):
    def __init__(self, url, needLogin, *args, **kwargs):
        super().__init__(url, needLogin, *args, **kwargs)

    async def subscribe(self, args: list[dict]):
        op = "subscribe"
        params = {"op": op, "args": args}
        await self.addRequest(params)


if __name__ == "__main__":

    async def main():
        url = "wss://ws.bitget.com/v2/ws/public"
        bigget = Bitget(url, False)
        args = [{"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"}]
        await bigget.subscribe(args)
        await bigget.run()

    asyncio.get_event_loop().run_until_complete(main())
