import asyncio
import sys
import os
import requests

sys.path.append(os.path.dirname(__file__) + "/..")
from lib.baseWebsocket import ExchangeWebsocket
from exchange.okxLogin import getLoginParams


class Okx(ExchangeWebsocket):
    def __init__(
        self,
        url: str,
        needLogin: bool,
        apikey=None,
        secret=None,
        passphrase=None,
        *args,
        **kwargs,
    ):
        super().__init__(url, needLogin, *args, **kwargs)
        self.apikey = apikey
        self.secret = secret
        self.passphrase = passphrase

    async def login(self, *args, **kwargs):
        loginParams = getLoginParams("login", self.apikey, self.secret, self.passphrase)
        await self.ws.send(loginParams)

    async def subscribe(self, args: list[dict]):
        op = "subscribe"
        params = {"op": op, "args": args}
        await self.addRequest(params)


def getAllOkxSymbols() -> list[str]:
    api_url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        symbols = [
            item["instId"] for item in data["data"] if item["settleCcy"] == "USDT"
        ]
        return symbols
    else:
        print(f"Error fetching symbols: {response.status_code}")
        return []


if __name__ == "__main__":
    getAllOkxSymbols()
    # async def main():
    #     url = "wss://wspap.okx.com:8443/ws/v5/public"
    #     okx = Okx(
    #         url,
    #         False,
    #         # apikey=okxConfig["apikey"],
    #         # secret=okxConfig["secret"],
    #         # passphrase=okxConfig["passphrase"],
    #     )
    #     # 订阅btc-usdt的markprice
    #     await okx.subscribe([{"channel": "funding-rate", "instId": instId} for instId in getAllOkxSymbols()[:10]])
    #     await okx.run()

    # asyncio.get_event_loop().run_until_complete(main())
