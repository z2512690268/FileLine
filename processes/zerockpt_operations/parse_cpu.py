from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
import time

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

        basename = Path(input_path.path).name.split(".")[0]
        file_meta_parts = basename.split("-")

        file_meta = {
            'model_name': file_meta_parts[0] if len(file_meta_parts) > 0 else 'unknown',
            'ckpt_type': file_meta_parts[1] if len(file_meta_parts) > 1 else 'unknown',
            'ckpt_freq': file_meta_parts[3] if len(file_meta_parts) > 3 else 'unknown',
            'file_name': basename        
        }

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