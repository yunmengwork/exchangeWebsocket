import asyncio
import json
import websockets
from websockets.protocol import State
from loguru import logger

# class State(enum.IntEnum):
#     """A WebSocket connection is in one of these four states."""
#     CONNECTING, OPEN, CLOSING, CLOSED = range(4)


class ExchangeWebsocket:
    def __init__(self, url: str, needLogin: bool, *args, **kwargs):
        """
        这是一个连接交易所websocket的基类
        :param url: websocket连接地址
        :param args: 其他参数
        :param kwargs: 其他参数
        """
        # 获取参数配置
        self.pingInterval = kwargs.get("pingInterval", 20)
        self.pingTimeout = kwargs.get("pingTimeout", 10)
        self.checkRequestInterval = kwargs.get("checkRequestInterval", 0.2)
        self.checkStateConsistentInterval = kwargs.get(
            "checkStateConsistentInterval", 5
        )
        self.execRequestsInterval = kwargs.get("execRequestsInterval", 0.2)
        self.processRecvInterval = kwargs.get("processRecvInterval", 0.0001)
        self.maxWaitforRecvInterval = kwargs.get("maxWaitforRecvInterval", 90)
        self.queueMaxSize = kwargs.get("queueMaxSize", 100)
        # 初始化
        self.url = url
        self.ws = None
        self.needLogin = needLogin
        self.requestQueue = asyncio.Queue(self.queueMaxSize)  # 实时请求队列
        self.requestLogList = []  # 日志列表，只在重连时使用
        self.state = State.CLOSED
        self.reconnecting = False

    async def keepAlive(self):
        """保持连接"""
        while True:
            await asyncio.sleep(self.pingInterval)

            if self.ws and self.ws.state == State.OPEN:
                await self.ws.send("ping")

    async def checkStateConsistent(self):
        while True:
            await asyncio.sleep(self.checkStateConsistentInterval)
            if self.reconnecting:
                continue
            if self.ws == None:
                # 未初始化，重新连接
                logger.debug("ws未初始化，正在重新连接...")
                await self.reconnect()
            if self.ws and self.ws.state != self.state:
                # 状态不一致，需要重新连接
                logger.debug("状态不一致，正在重新连接...")
                await self.reconnect()
            if self.ws and self.ws.state == self.state and self.state == State.CLOSED:
                # 断线，需要重新连接
                logger.debug("断线，正在重新连接...")
                await self.reconnect()

    async def reconnect(self, *args, **kwargs):
        # 重新连接
        self.reconnecting = True
        recoverQueue = asyncio.Queue()
        for requestMsg in self.requestLogList:
            await recoverQueue.put(requestMsg)
        if not (await self.connect(*args, **kwargs)):
            self.reconnecting = False
            return False
        # 执行先前的请求
        while not recoverQueue.empty():
            requestMsg = await recoverQueue.get()
            await self._execRequest(requestMsg)
            await asyncio.sleep(self.execRequestsInterval)
        # 重连结束
        self.reconnecting = False
        logger.info("重新连接成功")
        return True

    async def connect(self, *args, **kwargs):
        """连接websocket"""
        try:
            self.ws = await websockets.connect(self.url)
            if self.needLogin:
                await self.login(*args, **kwargs)
            self.state = State.OPEN
            logger.info("连接成功")
            return True
        except Exception as e:
            logger.warning("连接失败")
            self.state = State.CLOSED
            return False

    async def login(self, *args, **kwargs):  # 根据需要重写
        """如果需要登录的话自己写登录逻辑"""
        pass

    async def addRequest(self, requestMsg):
        """添加请求"""
        await self.requestQueue.put(json.dumps(requestMsg))  # 添加到请求队列
        self.requestLogList.append(json.dumps(requestMsg))  # 添加到日志队列

    # 监听与执行
    async def execRequests(self):
        """循环检查请求队列，如果存在请求则执行请求"""
        while True:
            if not self.requestQueue.empty() and not self.reconnecting:
                requestMsg = await self.requestQueue.get()
                await self._execRequest(requestMsg)

            await asyncio.sleep(self.checkRequestInterval)

    # 具体的执行逻辑
    async def _execRequest(self, requestMsg):
        """执行请求"""
        if self.ws and self.ws.state == State.OPEN:
            await self.ws.send(requestMsg)
        else:
            await asyncio.sleep(self.execRequestsInterval)
            await self._execRequest(requestMsg)

    # 处理接受到的消息
    async def processRecv(self):
        """循环接受消息并将其交给_processRecv处理"""
        while True:
            if self.ws and self.ws.state == State.OPEN:
                recv = await self.ws.recv()
                if recv == "pong":
                    continue
                await self._processRecv(recv)

            await asyncio.sleep(self.processRecvInterval)

    async def _processRecv(self, recvMsg):  # 根据需要重写
        """处理processRecv接受到的消息"""
        if recvMsg == "pong":
            pass
        else:
            logger.info("recive msg {}".format(recvMsg))

    async def run(self):
        """运行程序"""
        await asyncio.gather(
            self.connect(),
            self.keepAlive(),
            self.checkStateConsistent(),
            self.execRequests(),
            self.processRecv(),
        )


if __name__ == "__main__":

    class OKX(ExchangeWebsocket):
        def __init__(
            self,
            url: str,
            needLogin: bool,
            apikey=None,
            secret=None,
            passphrase=None,
            *args,
            **kwargs
        ):
            super().__init__(
                url, needLogin, apikey, secret, passphrase, *args, **kwargs
            )

        async def subscribe(self, args: list[dict]):
            op = "subscribe"
            params = {"op": op, "args": args}
            await self.addRequest(params)

    async def main():
        url = "wss://wspap.okx.com:8443/ws/v5/public"
        okx = OKX(url, False)
        # 订阅btc-usdt的markprice
        await okx.subscribe([{"channel": "funding-rate", "instId": "BTC-USD-SWAP"}])
        await okx.run()

    asyncio.get_event_loop().run_until_complete(main())
