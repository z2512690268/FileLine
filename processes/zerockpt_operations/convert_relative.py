from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import Dict, List, Any

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def convert_to_relative(
    input_path: InputPath, 
    output_path: Path,
    reference_conditions: Dict[str, Any],
    reference_column: str,
    target_columns: List[str],
    **kwargs: Any
):
    """
    将指定列的绝对数值转换为相对数值。

    该处理器会根据查询条件找到一个基准值，然后将目标列中的所有值
    都除以该基准值，从而实现标准化。

    参数:
    input_path: 输入的Parquet文件路径。
    output_path: 输出转换后数据的Parquet文件路径。
    reference_conditions: 一个字典，用于定位包含基准值的那唯一一行。
                          例如: {'step': 101, 'sub_category': 'Forward'}
    reference_column: 包含基准值的列名。例如: 'duration'
    target_columns: 一个列表，其中包含所有需要进行相对值转换的列名。
                      例如: ['start_sec', 'end_sec', 'duration']
    """
    # --- 1. 参数校验 ---
    if not all([reference_conditions, reference_column, target_columns]):
        raise ValueError("'reference_conditions', 'reference_column', 和 'target_columns' 都是必需参数。")

    # --- 2. 读取数据 ---
    df = pd.read_parquet(input_path.path)

    # --- 3. 查找基准值 ---
    # 根据条件构建布尔掩码
    mask = pd.Series(True, index=df.index)
    for col, val in reference_conditions.items():
        if col not in df.columns:
            raise ValueError(f"条件列 '{col}' 在数据中不存在。可用列: {df.columns.tolist()}")
        mask &= (df[col] == val)
    
    reference_rows = df[mask]
    
    # 确保只找到唯一的一行
    if len(reference_rows) == 0:
        raise ValueError(f"根据条件 {reference_conditions} 未找到任何匹配的行。")
    if len(reference_rows) > 1:
        raise ValueError(f"根据条件 {reference_conditions} 找到了多于一行({len(reference_rows)}行)，无法确定唯一基准。")
    
    # 提取基准值
    if reference_column not in df.columns:
        raise ValueError(f"基准列 '{reference_column}' 在数据中不存在。")
        
    reference_value = reference_rows.iloc[0][reference_column]

    if reference_value == 0:
        raise ValueError(f"找到的基准值为0，无法进行除法运算。")
        
    print(f"找到基准值: {reference_value} (来自 {reference_conditions} 行的 '{reference_column}' 列)")

    # --- 4. 转换目标列 ---
    print(f"将对以下目标列进行转换: {target_columns}")
    
    converted_df = df.copy()
    for col in target_columns:
        if col not in converted_df.columns:
            print(f"警告: 目标列 '{col}' 不存在，已跳过。")
            continue
        converted_df[col] = converted_df[col] / reference_value
    
    # --- 5. 保存结果 ---
    converted_df.to_parquet(output_path)