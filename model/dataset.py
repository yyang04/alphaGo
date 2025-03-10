from concurrent.futures import ThreadPoolExecutor, as_completed
from sklearn.feature_extraction import FeatureHasher
import time
import random

# 定义一个函数，该函数将被线程池中的线程执行
def task(n):
    time.sleep(3 * random.random())  # 模拟耗时操作
    return

# 任务总数
total_tasks = 10

# 使用 ThreadPoolExecutor 创建线程池
with ThreadPoolExecutor(max_workers=3) as executor:
    # 提交任务到线程池，并收集 Future 对象
    futures = [executor.submit(task, i) for i in range(total_tasks)]

    # 追踪每个任务的完成情况
    for future in as_completed(futures):
        result = future.result()  # 获取任务结果
        # 计算并显示进度
        done = len(list(filter(lambda f: f.done(), futures)))
        progress = (done / total_tasks) * 100
        print(f"进度: {progress:.2f}%")

print("所有任务已完成。")