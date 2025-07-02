from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any
import re
from datetime import datetime
import time
import numpy as np

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def filter_single_save(input_path: InputPath, output_path: Path, start_step: int, end_step: int) -> pd.DataFrame:
    """
    提取指定步骤范围内的检查点保存过程数据，并整合到一张表中
    
    参数:
    input_path: 输入的日志数据parquet文件路径
    output_path: 输出路径
    start_step: 保存过程开始的step
    end_step: 保存过程结束的step
    
    返回:
    整合后的DataFrame，包含:
        - event_type: 事件类型 (Compute/Transfer/Background)
        - step: 关联的步骤号
        - duration: 事件持续时间
        - other_info: 事件相关信息
    """
    df = pd.read_parquet(input_path.path)

    # 结果DataFrame
    out_df = pd.DataFrame()
    
    # 1. 筛选指定step范围内的记录
    df_range = df[(df['step'] >= start_step) & (df['step'] <= end_step)].copy()
    
    # 2. 处理Compute类数据
    compute_df = df_range[df_range['record_type'] == 'Step'][[
        'step', 'total_time', 'forward_time', 
        'backward_time', 'update_time', 'timestamp'
    ]].copy()
    compute_df['event_type'] = 'Compute'
    compute_df['duration'] = compute_df['total_time']  # 使用总时间作为duration
    compute_df = compute_df.rename(columns={
        'forward_time': 'forward',
        'backward_time': 'backward',
        'update_time': 'update'
    })
    
    # 3. 处理Transfer类数据
    # 使用Stall信息，如果Stall信息在Step中
    if 'stall_duration' in df_range.columns:
        transfer_df = df_range[(df_range['record_type'] == 'Step') & (df_range['stall_duration'] > 0)][[
            'step', 'stall_duration', 'stall_start', 'stall_end', 'timestamp', 'grad_stall_duration', 'grad_stall_start', 'grad_stall_end'
        ]].copy()
        transfer_df['event_type'] = 'Transfer'
        transfer_df['duration'] = transfer_df['stall_duration']

    
    # 4. 处理Background类数据
    # 提取范围内的Callback记录
    callbacks = df_range[df_range['record_type'] == 'AdamW Callback'][
        ['step', 'unix_timestamp', 'timestamp']
    ].sort_values('unix_timestamp')
    
    background_list = []
    
    # 查找AdamW Callback之前的Real Stall
    for _, callback in callbacks.iterrows():
        # 查找之前的Real Stall
        prev_real_stall = df[
            (df['record_type'] == 'Real Stall') & 
            (df['unix_timestamp'] < callback['unix_timestamp']) & 
            (df['step'] >= start_step)
        ].sort_values('unix_timestamp', ascending=False)
        
        if not prev_real_stall.empty:
            # 计算从Real Stall到AdamW Callback的时长
            real_stall = prev_real_stall.iloc[0]
            background_list.append({
                'event_type': 'Background',
                'sub_type': 'Real_Stall_to_AdamW_Callback',
                'start_step': real_stall['step'],
                'end_step': callback['step'],
                'start_timestamp': real_stall['timestamp'],
                'end_timestamp': callback['timestamp'],
                'duration': callback['unix_timestamp'] - real_stall['unix_timestamp'],
                'timestamp': callback['timestamp']  # 使用回调时间作为主要时间戳
            })
        
        # 查找之后的Persist Callback
        next_persist = df[
            (df['record_type'] == 'Persist Callback') & 
            (df['unix_timestamp'] > callback['unix_timestamp']) & 
            (df['step'] <= end_step)
        ].sort_values('unix_timestamp')
        
        if not next_persist.empty:
            # 计算从AdamW到Persist Callback的时长
            persist_callback = next_persist.iloc[0]
            background_list.append({
                'event_type': 'Background',
                'sub_type': 'AdamW_Callback_to_Persist_Callback',
                'start_step': callback['step'],
                'end_step': persist_callback['step'],
                'start_timestamp': callback['timestamp'],
                'end_timestamp': persist_callback['timestamp'],
                'duration': persist_callback['unix_timestamp'] - callback['unix_timestamp'],
                'timestamp': persist_callback['timestamp']
            })
    
    background_df = pd.DataFrame(background_list)
    
    # 5. 合并所有数据到一张表
    # 添加必要字段到Background数据
    if not background_df.empty:
        background_df['step'] = background_df['end_step'].astype(int)
    
    # 为所有数据添加通用字段
    for df_part in [compute_df, transfer_df, background_df]:
        if not df_part.empty:
            # 确保所有DataFrame都有step列
            if 'step' not in df_part.columns:
                df_part['step'] = np.nan
            
            # 为每部分添加duration和event_type
            if 'event_type' not in df_part.columns:
                df_part['event_type'] = 'Unknown'
            
            if 'duration' not in df_part.columns:
                df_part['duration'] = np.nan
            
            # 选择需要的列
            common_cols = ['event_type', 'step', 'timestamp', 'duration']
            extended_cols = [col for col in df_part.columns if col not in common_cols and col != 'sub_type']
            
            if 'sub_type' in df_part.columns:
                common_cols.append('sub_type')
            
            # 添加到结果表
            out_df = pd.concat([out_df, df_part[common_cols + extended_cols]], ignore_index=True)
    
    # 6. 按时间戳排序
    if not out_df.empty and 'timestamp' in out_df.columns:
        out_df = out_df.sort_values('timestamp')
    
    # 7. 保存结果
    out_df.to_parquet(output_path)