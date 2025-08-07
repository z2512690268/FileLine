from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List, Dict, Any, Optional

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def adjust_datastates_stall_duration(input_path: InputPath, output_path: Path) -> pd.DataFrame:
    """
    专门为 ckpt_type = 'datastates_llm' 的 'Full Stall' 记录调整 stall_duration。

    处理逻辑:
    1. 筛选出所有 record_type = 'Full Stall' 且 ckpt_type = 'datastates_llm' 的记录 r。
    2. 对每个 r, 获取其 model_name(mn), step(s), ckpt_freq(f), 和 ckpt_type(ct)。
    3. 在具有相同 model_name(mn), ckpt_type(ct) 和 ckpt_freq(f) 的 'Step' 记录中, 
       查找 step = s + 1 的记录 r1 和 step = s + 2 的记录 r2。
    4. 如果 r1 和 r2 都存在, 计算 delta = r1['total_time'] - r2['total_time']。
    5. 更新 r['stall_duration'] = r['stall_duration'] + delta。
    """
    print(f"开始处理文件: {input_path.path}")

    try:
        df = pd.read_parquet(input_path.path)
    except Exception as e:
        print(f"读取Parquet文件失败: {e}")
        return pd.DataFrame()

    # 1. 筛选'Step'记录时，同样只保留 ckpt_type = 'datastates_llm' 的
    print("正在为 ckpt_type='datastates_llm' 的 'Step' 记录创建复合键查找映射...")
    step_df = df[(df['record_type'] == 'Step') & (df['ckpt_type'] == 'datastates_llm')].copy()
    
    # 2. 【修改】创建基于四元复合键(model_name, ckpt_type, ckpt_freq, step)的快速查找字典
    index_cols = ['model_name', 'ckpt_type', 'ckpt_freq', 'step']
    step_df.dropna(subset=index_cols, inplace=True)
    step_time_map = step_df.set_index(index_cols)['total_time'].to_dict()

    # 3. 筛选出需要处理的目标记录 (逻辑不变)
    target_mask = (df['record_type'] == 'Full Stall') & (df['ckpt_type'] == 'datastates_llm')
    stall_indices = df[target_mask].index
    print(f"找到 {len(stall_indices)} 条 'Full Stall' (ckpt_type='datastates_llm') 记录需要处理。")

    update_counter = 0
    missing_counter = 0

    # 4. 遍历每一条目标记录并执行更新逻辑
    for idx in stall_indices:
        stall_record = df.loc[idx]
        stall_model = stall_record['model_name']
        stall_step = stall_record['step']
        stall_freq = stall_record['ckpt_freq']
        stall_ckpt_type = stall_record['ckpt_type']
        
        if pd.isna(stall_model) or pd.isna(stall_step) or pd.isna(stall_freq) or pd.isna(stall_ckpt_type):
            missing_counter += 1
            continue

        stall_step = int(stall_step)

        # 5. 【修改】使用四元复合键进行查找
        key1 = (stall_model, stall_ckpt_type, stall_freq, stall_step + 1)
        key2 = (stall_model, stall_ckpt_type, stall_freq, stall_step + 2)
        
        time_step1 = step_time_map.get(key1)
        time_step2 = step_time_map.get(key2)
        
        if time_step1 is not None and time_step2 is not None:
            delta = time_step1 - time_step2
            original_duration = df.loc[idx, 'stall_duration']
            print(f'original_duration: {original_duration:.3f}, delta: {delta:.3f}, step1: {time_step1:.3f}, step2: {time_step2:.3f}, ckpt_freq: {stall_freq}, step: {stall_step}')
            df.loc[idx, 'stall_duration'] = original_duration + delta
            update_counter += 1
        else:
            missing_counter += 1

    print(f"处理完成。成功更新 {update_counter} 条记录。")
    if missing_counter > 0:
        print(f"{missing_counter} 条目标记录因缺少对应的后续Step记录(具有相同的model_name, ckpt_type和ckpt_freq)而未被更新。")

    # 6. 保存修改后的DataFrame到新的Parquet文件
    try:
        df.to_parquet(output_path)
        print(f"成功将更新后的数据保存到: {output_path}")
    except Exception as e:
        print(f"保存Parquet文件失败: {e}")