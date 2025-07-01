from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

@ProcessorRegistry.register(input_type="single", output_ext=".csv")
def generate_gockpt_overhead(input_paths: InputPath, output_path: Path):
    # 创建空列表用于存储所有点的数据
    all_data = []
    
    num_points = 500

    # 1. 处理async部分
    x_async = np.linspace(0, 10, num_points, endpoint=False)
    for x, y in zip(x_async, x_async):
        all_data.append(['async', x, y])
    
    # 2. 处理overlap部分
    x_overlap = np.linspace(1, 10, num_points // 10 * 9, endpoint=False)
    for x, y in zip(x_overlap, x_overlap - 1):
        all_data.append(['overlap', x, y])

    # 3. 处理grad部分（分段定义）
    segments = [
        (1, 2, num_points // 10, lambda x: (x - 1) / 7),
        (2, 3, num_points // 10, lambda x: (2 * x - 3) / 7),
        (3, 4, num_points // 10, lambda x: (3 * x - 6) / 7),
        (4, 5, num_points // 10, lambda x: (4 * x - 10) / 7),
        (5, 6, num_points // 10, lambda x: (5 * x - 15) / 7),
        (6, 7, num_points // 10, lambda x: (6 * x - 21) / 7),
        (7, 10, num_points // 10 * 3, lambda x: x - 4)
    ]
    
    for start, end, n, func in segments:
        x_vals = np.linspace(start, end, n, endpoint=False)
        for x in x_vals:
            y = func(x)
            all_data.append(['grad', x, y])

    # 创建DataFrame并保存为CSV
    df = pd.DataFrame(all_data, columns=['solution', 'x', 'y'])
    df.to_csv(output_path, index=False)