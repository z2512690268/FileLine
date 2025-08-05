from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
import numpy as np
from .utils import get_step_timestamps

@ProcessorRegistry.register(input_type="multi", output_ext=".parquet")
def parse_gpu(input_paths: List[InputPath], output_path: Path, time_from_start: bool = False) -> pd.DataFrame:
    """解析多个GPU监控日志文件并提取性能指标数据"""
    # 修正正则表达式：
    # 1. 时间戳支持可选的毫秒（\.\d+)?
    # 2. 字段间匹配一个或多个空格（\s+）
    # 3. 允许指标值为数字或'-'（(\d+|-）
    gpu_pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?)\s+(\d+)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)'

    # print(output_path)
    all_data = []
    for input_path in input_paths:
        # 只处理.gpumon文件
        if not str(input_path.original_path).endswith('.gpumon'):
            print(f"Skipping non-gpu file: {input_path.original_path}")
            continue
        
        basename = Path(input_path.original_path).name.split(".")[0]
        file_meta_parts = basename.split("-")

        # 提取文件元数据
        file_meta = {
            'model_name': file_meta_parts[0] if len(file_meta_parts) > 0 else 'unknown',
            'ckpt_type': file_meta_parts[1] if len(file_meta_parts) > 1 else 'unknown',
            'step_num': file_meta_parts[2] if len(file_meta_parts) > 2 else 'unknown',
            'ckpt_freq': file_meta_parts[3] if len(file_meta_parts) > 3 else 'unknown',
            'file_name': basename,
            'absolute_path': str(input_path.original_path)
        }
        # 获取时间戳范围
        step_1_timestamp, step_num_timestamp = get_step_timestamps(file_meta)
        
        with open(input_path.path, 'r') as f:
            for line in f:
                line = line.strip()
                # 跳过注释行（含#）和空行
                if not line or '#' in line:
                    continue
                
                try:
                    # 匹配GPU指标行
                    gpu_match = re.match(gpu_pattern, line)
                    if gpu_match:
                        # 提取时间戳（支持带毫秒的格式）
                        timestamp_str = gpu_match.group(1)
                        # 处理时间戳格式（带或不带毫秒）
                        if '.' in timestamp_str:
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

                        # 过滤时间戳范围
                        if timestamp >= step_1_timestamp and (step_num_timestamp is None or timestamp <= step_num_timestamp):
                            # 提取指标（将'-'转换为NaN）
                            def parse_value(val):
                                return int(val) if val != '-' else np.nan

                            data = {
                                'timestamp': timestamp,
                                'gpu_idx': int(gpu_match.group(3)),  # GPU索引
                                'sm_util': parse_value(gpu_match.group(4)),  # SM利用率(%)
                                'mem_util': parse_value(gpu_match.group(5)),  # 显存利用率(%)
                                'enc_util': parse_value(gpu_match.group(6)),  # 编码器利用率(%)
                                'dec_util': parse_value(gpu_match.group(7)),  # 解码器利用率(%)
                                'jpg_util': parse_value(gpu_match.group(8)),  # JPEG引擎利用率(%)
                                'ofa_util': parse_value(gpu_match.group(9)),  # OFA利用率(%)
                                'rx_pci': parse_value(gpu_match.group(10)),  # PCIe接收速率(MB/s)
                                'tx_pci': parse_value(gpu_match.group(11)),  # PCIe发送速率(MB/s)
                                **file_meta
                            }
                            # print(data['mem_util'])
                            all_data.append(data)
                    else:
                        # 调试用：打印不匹配的行（可删除）
                        # print(f"Skipping unrecognized line: {line}")
                        pass
                except Exception as e:
                    print(f"Error parsing line: {line}\nError: {e}")

    # 转换为DataFrame
    df = pd.DataFrame(all_data) if all_data else pd.DataFrame()

    # 计算相对开始时间
    if time_from_start and not df.empty:
        df['time_from_start'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds()

    # 保存为Parquet文件
    if not df.empty:
        df.to_parquet(output_path)
        print(f"Successfully parsed {len(df)} GPU metrics records")
    else:
        print("No valid GPU metrics data found")