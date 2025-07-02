from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from typing import List

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def rearrange_single_save(
    input_path: InputPath, 
    output_path: Path
):
    """
    将预处理后的数据表转换为绘图格式，包含相对时间信息
    
    参数:
    input_path: 预处理后的数据表路径
    output_path: 输出路径
    
    返回:
    转换后的DataFrame，包含:
        - category: 事件类别 (Compute/Transfer/Background)
        - sub_category: 事件子类型
        - start_sec: 相对起始时间(秒)
        - end_sec: 相对结束时间(秒)
        - step: 关联的步骤号
    """
  # 加载预处理后的数据
    df = pd.read_parquet(input_path.path)
    
    # 1. 处理所有时间戳，统一转换为datetime对象
    # 时间戳字段列表
    time_fields = [
        'timestamp', 'start_timestamp', 'end_timestamp',
        'stall_start', 'stall_end', 'grad_stall_start', 'grad_stall_end'
    ]
    
    # 转换时间格式
    for col in time_fields:
        if col in df.columns:
            # 跳过空值
            non_null = df[col].notnull()
            df.loc[non_null, col] = df.loc[non_null, col].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ") if isinstance(x, str) else x
            )
    
    # 2. 获取全局起始时间
    # 收集所有可能的时间点
    all_times: List[datetime] = []
    
    for col in time_fields:
        if col in df.columns:
            # 只处理datetime类型
            time_col = df[df[col].apply(lambda x: isinstance(x, datetime))][col]
            if not time_col.empty:
                all_times.extend(time_col.tolist())
    
    # 添加计算事件的时间点
    compute_times: List[datetime] = []
    if 'timestamp' in df.columns and 'duration' in df.columns:
        for _, row in df[df['event_type'] == 'Compute'].iterrows():
            if isinstance(row['timestamp'], datetime):
                start_time = row['timestamp']
                end_time = start_time + timedelta(seconds=row['duration'])
                compute_times.extend([start_time, end_time])
    
    # 添加传输事件的时间点
    transfer_times: List[datetime] = []
    if 'stall_start' in df.columns or 'grad_stall_start' in df.columns:
        for _, row in df[df['event_type'] == 'Transfer'].iterrows():
            if 'stall_start' in row and isinstance(row['stall_start'], datetime):
                transfer_times.extend([row['stall_start'], row['stall_start'] + timedelta(seconds=row['stall_duration'])])
            if 'grad_stall_start' in row and isinstance(row['grad_stall_start'], datetime):
                transfer_times.extend([row['grad_stall_start'], row['grad_stall_start'] + timedelta(seconds=row['grad_stall_duration'])])
    
    all_times.extend(compute_times)
    all_times.extend(transfer_times)
    
    # 找到最早的时间点作为全局起始点
    if not all_times:
        raise ValueError("No valid timestamps found for calculating global start time")
    
    global_start = min(all_times)
    
    # 3. 创建新数据的容器列表
    plot_records = []
    
    # 4. 处理Compute事件 - 细分为三个子类型
    compute_df = df[df['event_type'] == 'Compute']
    for _, row in compute_df.iterrows():
        if not isinstance(row['timestamp'], datetime):
            continue
            
        start_time = row['timestamp'] - timedelta(seconds=row['duration'])
        current_time = start_time
        
        # 处理前向传播
        if 'forward' in row and isinstance(row['forward'], (float, int)):
            end_time = current_time + timedelta(seconds=row['forward'])
            plot_records.append({
                "category": "Compute",
                "sub_category": "Forward",
                "start_sec": (current_time - global_start).total_seconds(),
                "end_sec": (end_time - global_start).total_seconds(),
                "step": row['step']
            })
            current_time = end_time
        
        # 处理后向传播
        if 'backward' in row and isinstance(row['backward'], (float, int)):
            end_time = current_time + timedelta(seconds=row['backward'])
            plot_records.append({
                "category": "Compute",
                "sub_category": "Backward",
                "start_sec": (current_time - global_start).total_seconds(),
                "end_sec": (end_time - global_start).total_seconds(),
                "step": row['step']
            })
            current_time = end_time
        
        # 处理参数更新
        if 'update' in row and isinstance(row['update'], (float, int)):
            end_time = current_time + timedelta(seconds=row['update'])
            plot_records.append({
                "category": "Compute",
                "sub_category": "Update",
                "start_sec": (current_time - global_start).total_seconds(),
                "end_sec": (end_time - global_start).total_seconds(),
                "step": row['step']
            })
    
    # 5. 处理Transfer事件 - 修复缺失问题
    transfer_df = df[df['event_type'] == 'Transfer']
    for _, row in transfer_df.iterrows():
        # 处理数据停滞时间
        if 'stall_duration' in row and isinstance(row['stall_duration'], (float, int)) and row['stall_duration'] > 0:
            if 'stall_start' in row and isinstance(row['stall_start'], datetime):
                start_time = row['stall_start']
                duration = row['stall_duration']
                end_time = start_time + timedelta(seconds=duration)
                
                plot_records.append({
                    "category": "Transfer",
                    "sub_category": "Data_Stall",
                    "start_sec": (start_time - global_start).total_seconds(),
                    "end_sec": (end_time - global_start).total_seconds(),
                    "step": row['step']
                })
            else:
                # 使用Compute事件的时间作为参考
                compute_row = compute_df[compute_df['step'] == row['step']]
                if not compute_row.empty and isinstance(compute_row.iloc[0]['timestamp'], datetime):
                    start_time = compute_row.iloc[0]['timestamp'] + timedelta(seconds=compute_row.iloc[0]['duration']/2)
                    duration = row['stall_duration']
                    end_time = start_time + timedelta(seconds=duration)
                    
                    plot_records.append({
                        "category": "Transfer",
                        "sub_category": "Data_Stall",
                        "start_sec": (start_time - global_start).total_seconds(),
                        "end_sec": (end_time - global_start).total_seconds(),
                        "step": row['step']
                    })
        
        # 处理梯度停滞时间
        if 'grad_stall_duration' in row and isinstance(row['grad_stall_duration'], (float, int)) and row['grad_stall_duration'] > 0:
            if 'grad_stall_start' in row and isinstance(row['grad_stall_start'], datetime):
                start_time = row['grad_stall_start']
                duration = row['grad_stall_duration']
                end_time = start_time + timedelta(seconds=duration)
                
                plot_records.append({
                    "category": "Transfer",
                    "sub_category": "Gradient_Stall",
                    "start_sec": (start_time - global_start).total_seconds(),
                    "end_sec": (end_time - global_start).total_seconds(),
                    "step": row['step']
                })
            else:
                # 使用Compute事件的时间作为参考
                compute_row = compute_df[compute_df['step'] == row['step']]
                if not compute_row.empty and isinstance(compute_row.iloc[0]['timestamp'], datetime):
                    start_time = compute_row.iloc[0]['timestamp'] + timedelta(seconds=compute_row.iloc[0]['duration']*0.8)
                    duration = row['grad_stall_duration']
                    end_time = start_time + timedelta(seconds=duration)
                    
                    plot_records.append({
                        "category": "Transfer",
                        "sub_category": "Gradient_Stall",
                        "start_sec": (start_time - global_start).total_seconds(),
                        "end_sec": (end_time - global_start).total_seconds(),
                        "step": row['step']
                    })
    
    
    # 6. 处理Background事件
    background_df = df[df['event_type'] == 'Background']
    for _, row in background_df.iterrows():
        if 'start_timestamp' in row and 'end_timestamp' in row:
            if isinstance(row['start_timestamp'], datetime) and isinstance(row['end_timestamp'], datetime):
                start_sec = (row['start_timestamp'] - global_start).total_seconds()
                end_sec = (row['end_timestamp'] - global_start).total_seconds()
                
                plot_records.append({
                    "category": "Background",
                    "sub_category": row.get('sub_type', 'Background_Operation'),
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "step": row['step']
                })
        elif 'start_time' in row and isinstance(row['start_time'], datetime) and 'end_time' in row and isinstance(row['end_time'], datetime):
            start_sec = (row['start_time'] - global_start).total_seconds()
            end_sec = (row['end_time'] - global_start).total_seconds()
            
            plot_records.append({
                "category": "Background",
                "sub_category": row.get('sub_type', 'Background_Operation'),
                "start_sec": start_sec,
                "end_sec": end_sec,
                "step": row['step']
            })
    
    # 7. 创建最终的DataFrame
    plot_data = pd.DataFrame(plot_records)
    
    # 8. 添加持续时间列
    if not plot_data.empty:
        plot_data["duration"] = plot_data["end_sec"] - plot_data["start_sec"]
    
    # 9. 按起始时间排序
    if not plot_data.empty:
        plot_data = plot_data.sort_values("start_sec").reset_index(drop=True)
    
    # 10. 保存结果
    if not plot_data.empty:
        plot_data.to_parquet(output_path)