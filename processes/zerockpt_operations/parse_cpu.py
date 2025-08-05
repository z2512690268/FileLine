from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
import time
from .utils import get_step_timestamps

@ProcessorRegistry.register(input_type="multi", output_ext=".parquet")
def parse_cpu(input_paths: List[InputPath], output_path: Path, time_from_start: bool = False) -> pd.DataFrame:
    """解析多个运行日志文件并提取性能数据，支持AdamW Callback日志"""
    # 定义所有的正则表达式模式
    # [2025-07-04 12:14:23] cpu usage for '3005318': 198.4%
    patterns = {
        'cpu_usage': r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+cpu usage for \'(\d+)\':\s+([\d.]+)%',
    }

    all_data = []
    for input_path in input_paths:
        # 只处理.cpu文件
        if not str(input_path.original_path).endswith('.cpu'):
            print(f"Skipping non-cpu file: {input_path.original_path}")
            continue
        # print(f"Processing file: {input_path.original_path}") #debug
        basename = Path(input_path.original_path).name.split(".")[0]
        # print(f"Extracted basename: {basename}") #debug
        file_meta_parts = basename.split("-")

        file_meta = {
            'model_name': file_meta_parts[0] if len(file_meta_parts) > 0 else 'unknown',
            'ckpt_type': file_meta_parts[1] if len(file_meta_parts) > 1 else 'unknown',
            'step_num': file_meta_parts[2] if len(file_meta_parts) > 2 else 'unknown', # add filemeta infomation
            'ckpt_freq': file_meta_parts[3] if len(file_meta_parts) > 3 else 'unknown',
            'file_name': basename,
            'absolute_path': str(input_path.original_path),
        }
        # print(file_meta['step_num']) #debug
        # print(f"file absolute path: {file_meta['absolute_path']}")
        
        # TODO 暂时在这里添加筛选逻辑，但是处于后面VRAM,RAM等同需过滤，filter独立比较好，可以用tag传递信息
        # 如果解耦，两个想法：1.先过滤后parse， 2. parse后再过滤
        
        # 需要得到step0和step{step_num}的timestamp
        #2025-07-04 12:15:20.471 | INFO     | __main__:log_dist:85 - [Rank 0] Step 1 Time: 0.871s Forward: 0.406s Backward: 0.323s Update: 0.093s Loss: 10.0950 Singleloss: 10.0950 Tokens/s: 583.4128
        # --> 对应[2025-07-04 12:15:20] cpu usage for '3005318': 100.3%
        #2025-07-04 12:18:05.580 | INFO     | __main__:log_dist:85 - [Rank 0] Step 300 Time: 0.546s Forward: 0.026s Backward: 0.326s Update: 0.194s Loss: 1.5100 Singleloss: 1.4165 Tokens/s: 923.5790
        # --> 对应[2025-07-04 12:18:05] cpu usage for '3005318': 103.5%
        step_1_timestamp, step_num_timestamp = get_step_timestamps(file_meta)
        
        with open(input_path.path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    cpu_usage_match = re.match(patterns['cpu_usage'], line)
                    if cpu_usage_match:
                        timestamp = datetime.strptime(cpu_usage_match.group(1), '%Y-%m-%d %H:%M:%S')
                        pid = cpu_usage_match.group(2)
                        cpu_usage = float(cpu_usage_match.group(3))
                        if timestamp >= step_1_timestamp and (step_num_timestamp is None or timestamp <= step_num_timestamp):
                            all_data.append({
                                'timestamp': timestamp,
                                'pid': pid,
                                'cpu_usage': cpu_usage,
                            })
                except Exception as e:
                    print(f"Error parsing line: {line}\nError: {e}")

    df = pd.DataFrame(all_data) if all_data else pd.DataFrame()

    if time_from_start:
        df['time_from_start'] = df['timestamp'].apply(lambda x: (x - df['timestamp'].min()).total_seconds())

    if not df.empty:
        df.to_parquet(output_path)