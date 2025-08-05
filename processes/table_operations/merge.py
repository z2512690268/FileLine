from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
import warnings
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="multi", output_ext=".parquet")
def merge_tables(input_paths: List[InputPath], 
                 output_path: Path,
                 on_col: str,
                 how: str = 'inner',
                 suffixes: Optional[Tuple[str, str]] = None):
    """
    将两个数据表（Parquet文件）按指定列进行直接合并。

    此函数使用 `pd.merge`，适用于合并列可以精确匹配的场景。
    如果存在除合并列外的同名列，会自动添加后缀并发出警告。

    参数:
    - input_paths: 输入的Parquet文件路径列表（必须恰好为2个）。
    - output_path: 合并后的Parquet文件输出路径。
    - on_col: 用于合并的列名 (例如 "time_from_start")。
    - how: 合并方式，同pandas.merge的how参数。默认为 'inner' (交集)。
           其他可选：'outer', 'left', 'right'。
    - suffixes: 用于重命名重复列的后缀元组。默认为('_x', '_y')。
    """
    if len(input_paths) != 2:
        raise ValueError("此版本的 merge_tables 处理器只支持合并两个输入文件。")

    # 1. 读取两个输入文件
    df_left = pd.read_parquet(input_paths[0].path)
    df_right = pd.read_parquet(input_paths[1].path)

    # 检查合并列是否存在
    if on_col not in df_left.columns or on_col not in df_right.columns:
        raise ValueError(f"指定的合并列 '{on_col}' 在两个输入文件中并非都存在。")

    # 2. 检查是否存在除on_col外的重复列，并发出警告
    left_cols = set(df_left.columns)
    right_cols = set(df_right.columns)
    conflicting_cols = (left_cols & right_cols) - {on_col} # 使用集合运算找到交集并排除on_col

    if conflicting_cols:
        warnings.warn(
            f"输入文件之间存在重复的列名: {list(conflicting_cols)}。将自动添加后缀进行重命名。"
        )

    # 3. 执行直接合并
    # pandas.merge会自动处理重复列的重命名（使用suffixes参数）
    merged_df = pd.merge(
        left=df_left,
        right=df_right,
        on=on_col,
        how=how,
        suffixes=suffixes or ('_x', '_y') # 如果用户没提供，则使用默认后缀
    )

    # 4. 保存结果
    if not merged_df.empty:
        merged_df.to_parquet(output_path, index=False)
        print(f"成功合并文件到 {output_path}，生成 {len(merged_df)} 条记录。")
    else:
        print(f"警告：使用 '{how}' 方式合并后没有生成任何数据。请检查 '{on_col}' 列的值是否能匹配。")

    return ["table_merge", f"merged_with_{how}"]