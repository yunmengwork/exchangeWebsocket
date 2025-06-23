import os
import subprocess

# 多线程处理所有symbol的think2和think3模型
import threading

pythonPath = "./.venv/Scripts/python.exe"  # Windows下的虚拟环境python路径
plotObserveWindow = False
pltShow = False

symbols = os.listdir("./data/binance/bookTicker/")
symbols = [s.split(".")[0] for s in symbols if s.endswith(".csv")]


def process_symbol(symbol):
    try:
        subprocess.run(
            [
                pythonPath,
                "./model/think2.py",
                "--symbol",
                symbol,
                "--plotObserveWindow",
                str(plotObserveWindow),
                "--pltShow",
                str(pltShow),
            ],
            check=True,
        )
        print(f"Processed think2 {symbol} successfully.")
        subprocess.run(
            [
                pythonPath,
                "./model/think3.py",
                "--symbol",
                symbol,
                "--plotObserveWindow",
                str(plotObserveWindow),
                "--pltShow",
                str(pltShow),
            ],
            check=True,
        )
        print(f"Processed think3 {symbol} successfully.")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")


# 最大线程数
max_threads = 5  # 根据需要调整
threads = []
for symbol in symbols:
    while len(threads) >= max_threads:
        for t in threads:
            if not t.is_alive():
                threads.remove(t)
    thread = threading.Thread(target=process_symbol, args=(symbol,))
    thread.start()
    threads.append(thread)
