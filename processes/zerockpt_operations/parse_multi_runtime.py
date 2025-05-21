# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(input_type="multi", output_ext=".csv")
def parse_multi_runtime(input_paths: List[InputPath], output_path: Path, version: str = "merge"):
    """解析运行日志文件
    """
    data = []
    for input_path in input_paths:
        with open(input_path.path, 'r') as f:
            content = f.read()
    
        # 解析所有的形如下面两行的日志，并记录到表格
        # 2025-04-29 14:14:54.116 | INFO     | __main__:log_dist:69 - [Rank 0] Step 1 Time: 0.71s Forward: 0.417s Backward: 0.165s Update: 0.105s
        # 2025-04-29 14:14:54.398 | INFO     | __main__:log_dist:69 - [Rank 0] Step 2: Loss: 9.4980

        lines = content.split('\n')
        if version == "split":
            for line in lines:
                # 先判断是否符合要求，再分两类处理
                if '[Rank 0] Step' in line:
                    # 处理第一类日志
                    if 'Loss' in line:
                        # 解析loss
                        step_str = line.split('[Rank 0] Step ')[1].split(' ')[0]
                        loss_str = line.split('Loss: ')[1]
                    elif 'Time' in line:
                        # 解析时间
                        step_str = line.split('[Rank 0] Step ')[1].split(' ')[0]
                        time_str = line.split('Time: ')[1].split('s')[0]
                        forward_str = line.split('Forward: ')[1].split('s')[0]
                        backward_str = line.split('Backward: ')[1].split('s')[0]
                        update_str = line.split('Update: ')[1].split('s')[0]
                        data.append([input_path.original_path, step_str, time_str, forward_str, backward_str, update_str, loss_str, 0.0])
        elif version == "merge":
            for line in lines:
                # 先判断是否符合要求，再分两类处理
                if '[Rank 0] Step' in line:
                    step_str = line.split('[Rank 0] Step ')[1].split(' ')[0]
                    time_str = line.split('Time: ')[1].split('s')[0]
                    forward_str = line.split('Forward: ')[1].split('s')[0]
                    backward_str = line.split('Backward: ')[1].split('s')[0]
                    update_str = line.split('Update: ')[1].split('s')[0]
                    loss_str = line.split('Loss: ')[1].split(' ')[0]
                    Token_per_sec_str = line.split('Tokens/s: ')[1].split(' ')[0]
                    data.append([input_path.original_path, step_str, time_str, forward_str, backward_str, update_str, loss_str, Token_per_sec_str])
    # 保存到csv文件
    df = pd.DataFrame(data, columns=['file_path', 'step', 'time', 'forward', 'backward', 'update', 'loss', 'Token_per_sec'])
    df.to_csv(output_path, index=False)
