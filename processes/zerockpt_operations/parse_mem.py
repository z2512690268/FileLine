from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
import time
from .utils import get_step_timestamps

@ProcessorRegistry.register(input_type="multi", output_ext=".parquet")
def parse_mem(input_paths: List[InputPath], output_path: Path, time_from_start: bool = False) -> pd.DataFrame:
    """解析多个运行日志文件并提取内存(RAM)性能数据"""
    # 定义内存使用率的正则表达式模式
    # [2025-07-04 12:15:30] mem usage: 61.74% (82823573504/134150881280)
    patterns = {
        'mem_usage': r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+mem usage:\s+([\d.]+)%\s+\((\d+)/(\d+)\)',
    }

    all_data = []
    for input_path in input_paths:
        # 只处理.mem文件
        if not str(input_path.original_path).endswith('.mem'):
            print(f"Skipping non-mem file: {input_path.original_path}")
            continue
        # print(f"Processing file: {input_path.original_path}") #debug
        basename = Path(input_path.original_path).name.split(".")[0]
        # print(f"Extracted basename: {basename}") #debug
        file_meta_parts = basename.split("-")

        file_meta = {
            'model_name': file_meta_parts[0] if len(file_meta_parts) > 0 else 'unknown',
            'ckpt_type': file_meta_parts[1] if len(file_meta_parts) > 1 else 'unknown',
            'step_num': file_meta_parts[2] if len(file_meta_parts) > 2 else 'unknown',
            'ckpt_freq': file_meta_parts[3] if len(file_meta_parts) > 3 else 'unknown',
            'file_name': basename        
        }
        
        # 尝试获取step0和step{step_num}的时间戳（逻辑与CPU解析相同）
        step_1_timestamp, step_num_timestamp = get_step_timestamps(file_meta)
        
        with open(input_path.path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    # 解析内存使用率
                    ram_usage_match = re.match(patterns['mem_usage'], line)
                    if ram_usage_match:
                        timestamp = datetime.strptime(ram_usage_match.group(1), '%Y-%m-%d %H:%M:%S')
                        ram_percent = float(ram_usage_match.group(2))
                        used_bytes = int(ram_usage_match.group(3))
                        total_bytes = int(ram_usage_match.group(4))
                        if timestamp >= step_1_timestamp and (step_num_timestamp is None or timestamp <= step_num_timestamp):
                            all_data.append({
                                'timestamp': timestamp,
                                'mem_usage': ram_percent,
                                'used_bytes': used_bytes,
                                'total_bytes': total_bytes,
                                'used_gb': used_bytes / (1024**3),  # 转换为GB
                                'total_gb': total_bytes / (1024**3),
                            })
                except Exception as e:
                    print(f"Error parsing line: {line}\nError: {e}")

    df = pd.DataFrame(all_data) if all_data else pd.DataFrame()

    if time_from_start:
        df['time_from_start'] = df['timestamp'].apply(lambda x: (x - df['timestamp'].min()).total_seconds())

    if not df.empty:
        df.to_parquet(output_path)