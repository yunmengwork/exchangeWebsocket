import asyncio
import sys
import os
import json

sys.path.append(os.path.dirname(__file__) + "/..")
from lib.baseWebsocket import ExchangeWebsocket


if __name__ == "__main__":
    import time

    async def main():
        url = "wss://api.gateio.ws/ws/v4/"
        gateio = ExchangeWebsocket(url, False)
        params = {
            "time": int(time.time()),
            "channel": "spot.tickers",
            "event": "subscribe",  # "unsubscribe" for unsubscription
            "payload": ["BTC_USDT"],
        }
        await gateio.addRequest(params)
        await gateio.run()

    asyncio.get_event_loop().run_until_complete(main())
