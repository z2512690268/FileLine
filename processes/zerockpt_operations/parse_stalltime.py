# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(name="parse_stalltime", input_type="single", output_ext=".csv")
def parse_stalltime(input_path: InputPath, output_path: Path):
    """解析运行日志文件
    """
    with open(input_path.path, 'r') as f:
        content = f.read()
    
    # 解析所有的形如下面三行的日志，并记录到表格
    # 2025-04-29 14:16:36.382 | INFO     | zerockpt.ckpt.ckpt:on_update_begin:614 - Stall time: 0.000s
    # 2025-04-29 14:16:37.213 | INFO     | zerockpt.ckpt.ckpt:on_update_begin:592 - Full Stall time: 0.006s
    # 2025-04-29 14:16:37.213 | INFO     | zerockpt.ckpt.ckpt:on_update_begin:593 - Real Stall time: 1.053s


    lines = content.split('\n')
    data = []
    stalls = []
    for line in lines:
        # 先判断是否符合要求，再分类处理
        if 'Stall time' in line:
            if 'Full' in line:
                full_time = float(line.split(' ')[-1].split('s')[0])
            elif 'Real' in line:
                real_time = float(line.split(' ')[-1].split('s')[0])
                if full_time is not None:
                    data.append({'full_time': full_time,'real_time': real_time, 'stalls' : stalls.copy()})
                stalls = []
                full_time = None
                real_time = None
            else:
                stalls.append(float(line.split(' ')[-1].split('s')[0]))

    # 输出到csv文件
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)


    
