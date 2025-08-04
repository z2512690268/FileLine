from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from typing import List

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def rearrange_single_save_halfzero(
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
    
    # 5. 处理Transfer事件 - 重构后的新逻辑
    transfer_df = df[df['event_type'] == 'Transfer'].copy()

    # 直接遍历每一条独立的Transfer记录
    for _, row in transfer_df.iterrows():
        # 确保时间戳和时长是有效数据
        if not isinstance(row['timestamp'], datetime) or not isinstance(row['duration'], (float, int)):
            continue
        # 从'record_type'列获取子类别名称，例如 'LPT', 'HPT'
        sub_category_name = row['record_type']
        # 持续时间直接从 'duration' 列获取
        duration_sec = row['duration']
        # 'timestamp' 列现在代表这个独立事件的结束时间
        end_time = row['timestamp']
        # 通过结束时间和持续时间，计算出开始时间
        start_time = end_time - timedelta(seconds=duration_sec)
        # 创建绘图记录，所有时间都转换为相对于全局起点的秒数
        plot_records.append({
            "category": "Transfer",
            "sub_category": sub_category_name,
            "start_sec": (start_time - global_start).total_seconds(),
            "end_sec": (end_time - global_start).total_seconds(),
            "step": row['step']
        })
        
        if sub_category_name == 'HPT':
            plot_records.append({
                "category": "Compute",   # <-- 类别改为Compute
                "sub_category": "HPT_1",   # <-- 子类别保持为HPT，以便在图上识别
                "start_sec": (start_time - global_start).total_seconds(), # 时间信息完全相同
                "end_sec": (end_time - global_start).total_seconds(),   # 时间信息完全相同
                "step": row['step']+1
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
            
    # --- 核心修改开始：计算HPT_1和Backward的交集 ---
    
    # 7. 将plot_records转换为DataFrame进行处理
    temp_plot_df = pd.DataFrame(plot_records)
    
    if not temp_plot_df.empty:
        # 7.1 分离出HPT_1, Backward, 和其他所有事件
        hpt1_events = temp_plot_df[temp_plot_df['sub_category'] == 'HPT_1'].copy()
        backward_events = temp_plot_df[temp_plot_df['sub_category'] == 'Backward'].copy()
        other_events = temp_plot_df[~temp_plot_df['sub_category'].isin(['HPT_1', 'Backward'])]
        
        # 7.2 为了合并，重命名Backward事件的时间列，避免列名冲突
        backward_events = backward_events[['step', 'start_sec', 'end_sec']].rename(
            columns={'start_sec': 'b_start', 'end_sec': 'b_end'}
        )
        
        # 7.3 将HPT_1事件与对应step的Backward事件合并
        # inner merge确保只保留那些在同一个step中同时存在HPT_1和Backward的记录
        merged_hpt = pd.merge(hpt1_events, backward_events, on='step', how='inner')
        
        # 7.4 计算交集
        merged_hpt['intersection_start'] = np.maximum(merged_hpt['start_sec'], merged_hpt['b_start'])
        merged_hpt['intersection_end'] = np.minimum(merged_hpt['end_sec'], merged_hpt['b_end'])
        
        # 7.5 筛选出有有效重叠的事件（交集时长 > 0）
        valid_hpt = merged_hpt[merged_hpt['intersection_start'] < merged_hpt['intersection_end']].copy()
        
        # 7.6 更新HPT_1事件的起始和结束时间为交集时间
        valid_hpt['start_sec'] = valid_hpt['intersection_start']
        valid_hpt['end_sec'] = valid_hpt['intersection_end']
        
        # 7.7 准备最终的DataFrame，只保留必要的列
        final_hpt = valid_hpt[['category', 'sub_category', 'start_sec', 'end_sec', 'step']]
        
        # 7.8 将处理后的HPT_1事件、原始的Backward事件以及所有其他事件重新组合
        # 注意：这里我们把原始的Backward事件也加回来
        final_plot_df = pd.concat([other_events, temp_plot_df[temp_plot_df['sub_category'] == 'Backward'], final_hpt])
    
    else:
        final_plot_df = pd.DataFrame()

    # --- 核心修改结束 ---

    # 8. 添加持续时间列
    if not final_plot_df.empty:
        final_plot_df["duration"] = final_plot_df["end_sec"] - final_plot_df["start_sec"]
    
    # 9. 按起始时间排序
    if not final_plot_df.empty:
        final_plot_df = final_plot_df.sort_values("start_sec").reset_index(drop=True)
    
    # 10. 保存结果
    if not final_plot_df.empty:
        final_plot_df.to_parquet(output_path)