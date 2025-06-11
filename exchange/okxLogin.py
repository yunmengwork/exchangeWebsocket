import hmac
import hashlib
import base64
import time
import json


def getSignParam(timestamp: float | int, secret: str) -> str:
    """
    生成签名参数
    :param timestamp: 时间戳
    :param secret: 密钥
    :return: 签名参数
    """
    # 构造待签名的数据
    data_to_sign = timestamp + "GET" + "/users/self/verify"

    # 将数据和密钥转换为字节
    message = data_to_sign.encode("utf-8")
    secret_key = secret.encode("utf-8")

    # 生成HMAC-SHA256签名
    hmac_signature = hmac.new(secret_key, message, hashlib.sha256)
    digest = hmac_signature.digest()

    # 转换为Base64字符串
    signature_b64 = base64.b64encode(digest).decode("utf-8")

    return signature_b64


# 只接受单个账户
def getLoginParams(op: str, apiKey: str, secretKey: str, passphrase: str) -> str:
    """
    生成登录参数,只接受单个账户
    :param op: 操作类型
    :param apiKey: API密钥
    :param secretKey: API密钥
    :param passphrase: 密码短语
    :param sign: 签名
    :return: 登录参数字典
    """
    timestamp = str(int(time.time()))
    params = {
        "op": op,
        "args": [
            {
                "apiKey": apiKey,
                "passphrase": passphrase,
                "timestamp": timestamp,
                "sign": getSignParam(timestamp, secretKey),
            }
        ],
    }
    return json.dumps(params)
