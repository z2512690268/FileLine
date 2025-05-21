# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(input_type="multi", output_ext=".csv")
def parse_multi_stalltime(input_paths: List[InputPath], output_path: Path):
    """解析运行日志文件
    """
    file_data = []
    for input_path in input_paths:
        # print(input_path)
        # basename格式： Llama-cpp_overlap-500-50.log
        tag_path = [tag for tag in input_path.tags if tag.startswith('/root/keyao')]
        basename = Path(tag_path[0]).name.split('.')[0]
        model_name, ckpt_type, _, _ = basename.split('-')

        with open(input_path.path, 'r') as f:
            content = f.read()
        
        # 解析所有的形如下面三行的日志，并记录到表格
        # 2025-04-29 14:16:36.382 | INFO     | zerockpt.ckpt.ckpt:on_update_begin:614 - Stall time: 0.000s
        # 2025-04-29 14:16:37.213 | INFO     | zerockpt.ckpt.ckpt:on_update_begin:592 - Full Stall time: 0.006s
        # 2025-04-29 14:16:37.213 | INFO     | zerockpt.ckpt.ckpt:on_update_begin:593 - Real Stall time: 1.053s


        lines = content.split('\n')
        for line in lines:
            # 先判断是否符合要求，再分类处理
            if 'Stall time' in line:
                print(line)
                if 'Full' in line:
                    full_time = float(line.split(' ')[-1].split('s')[0])
                elif 'Real' in line:
                    real_time = float(line.split(' ')[-1].split('s')[0])
                    if full_time is not None:
                        file_data.append({'model_name': model_name, 'ckpt_type': ckpt_type, 'full_time': full_time,'real_time': real_time})
                    full_time = None
                    real_time = None

    # 按照model_name和ckpt_type进行平均
    data = pd.DataFrame(file_data)
    data = data.groupby(['model_name', 'ckpt_type']).mean().reset_index()

    # 输出到csv文件
    data.to_csv(output_path, index=False)
