from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
import time

@ProcessorRegistry.register(input_type="multi", output_ext=".parquet")
def parse_multi_stalltime_with_timestamp(input_paths: List[InputPath], output_path: Path, basename_type: str = "default", record_type: Optional[str] = None) -> pd.DataFrame:
    """解析多个运行日志文件并提取性能数据，支持AdamW Callback日志"""
    # 定义所有的正则表达式模式
    patterns = {
        'log_time': r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})',
        'adamw_callback': r'^AdamW Callback (\d+\.\d+)',  # 直接匹配AdamW Callback
        'persist_callback': r'- persist_callback: ([\d\.]+)s, ([\d\.]+), ([\d\.]+)',
        'step': r'Step (\d+) Time: ([\d\.]+)s Forward: ([\d\.]+)s Backward: ([\d\.]+)s Update: ([\d\.]+)s Loss: ([\d\.]+) Singleloss: ([\d\.]+) Tokens/s: ([\d\.]+)',
        'stall': r'- Stall time: ([\d\.]+)s, ([\d\.]+), ([\d\.]+)',
        'grad_stall': r'- grad stall time: ([\d\.]+)s, ([\d\.]+), ([\d\.]+)',
        'full_stall': r'Full Stall time: ([\d\.]+)s, ([\d\.]+), ([\d\.]+)',
        'full_stall_without_timestamp': r'Full Stall time: ([\d\.]+)s',
        'real_stall': r'Real Stall time: ([\d\.]+)s, ([\d\.]+), ([\d\.]+)',
    }
    
    # 收集所有解析出的数据
    all_data = []
    
    # 处理每个输入文件
    for input_path in input_paths:
        # 从文件名提取元数据
        basename = Path(input_path.original_path).name.split('.')[0]
        file_meta_parts = basename.split('-')
        
        # 提供默认值以防解析失败
        file_meta = {
            'model_name': file_meta_parts[0] if len(file_meta_parts) > 0 else 'unknown',
            'ckpt_type': file_meta_parts[1] if len(file_meta_parts) > 1 else 'unknown',
            'ckpt_freq': file_meta_parts[3] if len(file_meta_parts) > 3 else 'unknown',
            'file_name': basename
        }
        
        # 当前文件的状态变量
        current_step = None
        stall_info = None
        grad_stall_info = None
        
        # 逐行处理文件
        with open(input_path.path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # 初始化基础数据点
                    data_point = {
                        'model_name': file_meta['model_name'],
                        'ckpt_type': file_meta['ckpt_type'],
                        'ckpt_freq': file_meta['ckpt_freq'],
                        'file_name': file_meta['file_name'],
                        'record_type': None,
                        'timestamp': None,
                        'step': current_step
                    }
                    
                    # 1. 尝试匹配AdamW Callback（无需标准时间戳前缀）
                    callback_match = re.search(patterns['adamw_callback'], line)
                    if callback_match:
                        unix_ts = float(callback_match.group(1))
                        log_time = datetime.fromtimestamp(unix_ts)
                        
                        data_point.update({
                            'record_type': 'AdamW Callback',
                            'timestamp': log_time,
                            'unix_timestamp': unix_ts
                        })
                        all_data.append(data_point)
                        continue
                    
                    # 2. 尝试匹配标准日志时间戳
                    time_match = re.search(patterns['log_time'], line)
                    if not time_match:
                        # 没有时间戳前缀且不是Callback的行，跳过
                        continue
                    
                    # 解析标准时间戳
                    log_time = datetime.strptime(time_match.group(1), '%Y-%m-%d %H:%M:%S.%f')
                    timestamp_val = time.mktime(log_time.timetuple()) + log_time.microsecond/1e6
                    data_point['timestamp'] = log_time
                    data_point['unix_timestamp'] = timestamp_val
                    
                    # 移除已匹配的时间戳部分，处理剩余内容
                    line_remaining = line[len(time_match.group(0)):].strip()
                    
                    # 3. 尝试匹配Step信息
                    step_match = re.search(patterns['step'], line_remaining)
                    if step_match:
                        step = int(step_match.group(1))
                        current_step = step
                        
                        step_record = {
                            **data_point,
                            'record_type': 'Step',
                            'step': step,
                            'total_time': float(step_match.group(2)),
                            'forward_time': float(step_match.group(3)),
                            'backward_time': float(step_match.group(4)),
                            'update_time': float(step_match.group(5)),
                            'loss': float(step_match.group(6)),
                            'single_loss': float(step_match.group(7)),
                            'tokens_per_sec': float(step_match.group(8)),
                            'stall_duration': stall_info['duration'] if stall_info else None,
                            'stall_start': stall_info['start'] if stall_info else None,
                            'stall_end': stall_info['end'] if stall_info else None,
                            'grad_stall_duration': grad_stall_info['grad_stall_duration'] if grad_stall_info else None,
                            'grad_stall_start': grad_stall_info['start'] if grad_stall_info else None,
                            'grad_stall_end': grad_stall_info['end'] if grad_stall_info else None
                        }
                        
                        all_data.append(step_record)
                        stall_info = None  # 重置stall信息
                        continue
                    
                    # 4. 处理普通Stall（仅更新状态，不创建新记录）
                    stall_match = re.search(patterns['stall'], line_remaining)
                    if stall_match:
                        stall_info = {
                            'duration': float(stall_match.group(1)),
                            'end': float(stall_match.group(2)),
                            'start': float(stall_match.group(3))
                        }
                        continue

                    grad_stall_match = re.search(patterns['grad_stall'], line_remaining)
                    if grad_stall_match:
                        grad_stall_info = {
                            'grad_stall_duration': float(grad_stall_match.group(1)),
                            'end': float(grad_stall_match.group(2)),
                            'start': float(grad_stall_match.group(3))
                        }
                        continue
                    
                    # 5. 处理Full Stall（创建独立记录）
                    full_stall_match = re.search(patterns['full_stall'], line_remaining)
                    if full_stall_match:
                        stall_record = {
                            **data_point,
                            'record_type': 'Full Stall',
                            'stall_duration': float(full_stall_match.group(1)),
                            'stall_start': float(full_stall_match.group(3)),
                            'stall_end': float(full_stall_match.group(2))
                        }
                        all_data.append(stall_record)
                        continue

                    full_stall_without_timestamp_match = re.search(patterns['full_stall_without_timestamp'], line_remaining)
                    if full_stall_without_timestamp_match:
                        stall_record = {
                            **data_point,
                            'record_type': 'Full Stall',
                            'stall_duration': float(full_stall_without_timestamp_match.group(1)),
                            'stall_start': None,
                            'stall_end': None
                        }
                        all_data.append(stall_record)
                        continue
                    
                    # 6. 处理Real Stall（创建独立记录）
                    real_stall_match = re.search(patterns['real_stall'], line_remaining)
                    if real_stall_match:
                        stall_record = {
                            **data_point,
                            'record_type': 'Real Stall',
                            'stall_duration': float(real_stall_match.group(1)),
                            'stall_start': float(real_stall_match.group(3)),
                            'stall_end': float(real_stall_match.group(2))
                        }
                        all_data.append(stall_record)
                        continue
                
                    # 7. 处理Persist Callback (创建独立记录)
                    persist_match = re.search(patterns['persist_callback'], line_remaining)
                    if persist_match:
                        persist_record = {
                            **data_point,
                            'record_type': 'Persist Callback',
                            'persist_time': float(persist_match.group(1)),
                            'persist_callback_start': float(persist_match.group(2)),
                            'persist_callback_end': float(persist_match.group(3))
                        }
                        all_data.append(persist_record)
                        continue
                        
                except Exception as e:
                    print(f"Error parsing line: {line}\nError: {str(e)}")
    
    # 创建DataFrame
    df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    # 按时间戳排序
    if not df.empty and 'unix_timestamp' in df.columns:
        df = df.sort_values('unix_timestamp')
    
    if record_type is not None:
        df = df[df['record_type'] == record_type]

    # 保存到Parquet
    if not df.empty:
        df.to_parquet(output_path)
    