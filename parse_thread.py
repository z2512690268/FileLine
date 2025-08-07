from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
import time

# @ProcessorRegistry.register(input_type="multi", output_ext=".parquet")
def parse_full_stall_to_callback(
    input_paths: List[InputPath], 
    output_path: Path,
    # 预留的实验结束标识符的正则表达式
    end_of_experiment_regex: str = r"Your end-of-experiment log pattern here"
) -> pd.DataFrame:
    """
    解析日志文件，提取每个"Full Stall"事件与紧随其后的"AdamW Callback"事件之间的时间间隔。

    Args:
        input_paths: 输入的日志文件路径列表。
        output_path: 输出的Parquet文件路径。
        end_of_experiment_regex: 用于标识实验正常结束的日志正则表达式。
                                 如果一个Full Stall后出现此标识，则该Stall记录被丢弃。
                                 例如: r"Experiment finished successfully"
    """
    # 定义需要用到的正则表达式模式
    patterns = {
        'full_stall': r'Full Stall time: [\d\.]+s, ([\d\.]+), [\d\.]+',
        'adamw_callback': r'^AdamW Callback (\d+\.\d+)'
    }
    
    all_intervals = []

    for input_path in input_paths:
        # --- 文件元数据解析 (与您的示例保持一致) ---
        basename = Path(input_path.original_path).name.split('.')[0]
        file_meta_parts = basename.split('-')
        file_meta = {
            'model_name': str(file_meta_parts[0]) if len(file_meta_parts) > 0 else 'unknown',
            'ckpt_type': str(file_meta_parts[1]) if len(file_meta_parts) > 1 else 'unknown',
            # 您可以根据需要添加更多元数据字段
            'file_name': basename
        }

        # 状态变量，用于存储最近一次遇到的Full Stall的时间戳
        last_full_stall_ts = None

        with open(input_path.path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    # 1. 检查是否为实验结束标识符
                    if re.search(end_of_experiment_regex, line):
                        # 如果检测到结束标识，意味着最后一个Full Stall没有配对，循环终止
                        break

                    # 2. 尝试匹配 "AdamW Callback"
                    callback_match = re.search(patterns['adamw_callback'], line)
                    if callback_match and last_full_stall_ts is not None:
                        # 仅当之前已捕获到一个Full Stall时，此Callback才有意义
                        callback_ts = float(callback_match.group(1))
                        
                        # 计算时间间隔
                        interval = callback_ts - last_full_stall_ts
                        
                        # 记录数据
                        record = {
                            **file_meta,
                            'interval_s': interval,
                            'full_stall_timestamp': last_full_stall_ts,
                            'callback_timestamp': callback_ts
                        }
                        all_intervals.append(record)
                        
                        # 重置状态，准备寻找下一个Full Stall
                        last_full_stall_ts = None
                        continue

                    # 3. 尝试匹配 "Full Stall"
                    full_stall_match = re.search(patterns['full_stall'], line)
                    if full_stall_match:
                        # 捕获到Full Stall的结束时间戳（第二个时间戳）
                        # 如果已有未配对的Stall，新的会覆盖旧的
                        last_full_stall_ts = float(full_stall_match.group(1))
                        continue

                except Exception as e:
                    print(f"Error parsing line {line_num} in file {basename}: {line}\nError: {e}")

    # --- 创建并保存DataFrame ---
    if not all_intervals:
        print("Warning: No 'Full Stall' to 'AdamW Callback' intervals were found.")
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(all_intervals)
    
    # 保存到Parquet
    df.to_parquet(output_path)
    print(f"Successfully parsed {len(df)} intervals and saved to {output_path}")