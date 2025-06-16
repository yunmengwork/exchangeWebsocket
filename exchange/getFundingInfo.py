import requests
import json
import time
import os
from loguru import logger

# 获取okx所有币种的资金费率信息
# 保存结果到data/fundingInfo/okxfundingInfo.json
# okxSavePath = "data/fundingInfo/okxfundingInfo.json"


def _getBinanceFundingInfo():
    url = "https://fapi.binance.com//fapi/v1/fundingInfo"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logger.error(f"HTTP Error: {response.status_code}")
        return None


def updateBinanceFundingInfo(binanceSavePath):
    fundingInfoData = _getBinanceFundingInfo()
    if fundingInfoData:
        # 保存到文件
        if not os.path.exists(os.path.dirname(binanceSavePath)):
            os.makedirs(os.path.dirname(binanceSavePath))
        with open(binanceSavePath, "w") as f:
            fundingInfo = {
                "timestamp": int(time.time()),
                "data": fundingInfoData,
            }
            json.dump(fundingInfo, f, indent=4)
        logger.info("Binance funding info updated successfully.")
        return fundingInfoData
    else:
        logger.error("Failed to update Binance funding info.")
        return None


def getBinanceFundingInfo(binanceSavePath):
    if not os.path.exists(os.path.dirname(binanceSavePath)):
        os.makedirs(os.path.dirname(binanceSavePath))
    if not os.path.exists(binanceSavePath):
        updateBinanceFundingInfo(binanceSavePath)
    else:
        with open(binanceSavePath, "r") as f:
            fundingInfo = json.load(f)
            if time.time() - fundingInfo["timestamp"] < 86400:
                return fundingInfo["data"]
            else:
                logger.info("Funding info is outdated, updating...")
                return updateBinanceFundingInfo(binanceSavePath)
    return None


def getBinanceFundingInfoBySombol(symbol):
    binanceSavePath = "data/fundingInfo/binanceFundingInfo.json"
    fundingInfo = getBinanceFundingInfo(binanceSavePath)
    if fundingInfo:
        for item in fundingInfo:
            if item["symbol"] == symbol:
                return item
    logger.error(f"Symbol {symbol} not found in Binance funding info.")
    return None


def _getBitgetFundingInfo():
    url = f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=usdt-futures"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data["data"]
    else:
        logger.error(f"HTTP Error: {response.status_code}")
        return None


def updateBitgetFundingInfo(bitgetSavePath):
    fundingInfoData = _getBitgetFundingInfo()
    if fundingInfoData:
        # 保存到文件
        if not os.path.exists(os.path.dirname(bitgetSavePath)):
            os.makedirs(os.path.dirname(bitgetSavePath))
        with open(bitgetSavePath, "w") as f:
            fundingInfo = {
                "timestamp": int(time.time()),
                "data": fundingInfoData,
            }
            json.dump(fundingInfo, f, indent=4)
        logger.info("Bitget funding info updated successfully.")
        return fundingInfoData
    else:
        logger.error("Failed to update Bitget funding info.")
        return None


def getBitgetFundingInfo(bitgetSavePath):
    if not os.path.exists(os.path.dirname(bitgetSavePath)):
        os.makedirs(os.path.dirname(bitgetSavePath))
    if not os.path.exists(bitgetSavePath):
        updateBitgetFundingInfo(bitgetSavePath)
    else:
        with open(bitgetSavePath, "r") as f:
            fundingInfo = json.load(f)
            if time.time() - fundingInfo["timestamp"] < 86400:
                return fundingInfo["data"]
            else:
                logger.info("Funding info is outdated, updating...")
                return updateBitgetFundingInfo(bitgetSavePath)
    return None


def getBitgetFundingInfoBySombol(symbol):
    bitgetSavePath = "data/fundingInfo/bitgetFundingInfo.json"
    fundingInfo = getBitgetFundingInfo(bitgetSavePath)
    if fundingInfo:
        for item in fundingInfo:
            if item["symbol"] == symbol:
                return item
    logger.error(f"Symbol {symbol} not found in Bitget funding info.")
    return None


if __name__ == "__main__":
    # binanceSavePath = "data/fundingInfo/binanceFundingInfo.json"
    # print(getBinanceFundingInfoBySombol("SUSDT"))
    bitgetSavePath = "data/fundingInfo/bitgetFundingInfo.json"
    # print(getBinanceFundingInfoBySombol("BTCUSDT"))
    print(getBitgetFundingInfoBySombol("BTCUSDT"))
