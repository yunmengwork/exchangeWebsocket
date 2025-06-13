import json
import os
from loguru import logger


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
            if not os.path.exists(saveFile):
                with open(saveFile, "a+") as f:
                    f.write(f"{','.join(msg.keys())}\n")
            with open(saveFile, "a+") as f:
                f.write(f"{','.join(str(msg[col]) for col in msg.keys())}\n")
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
