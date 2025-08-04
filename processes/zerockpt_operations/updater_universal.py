from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import Any, Optional

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def update_universal(
    input_path: InputPath, 
    output_path: Path,
    # --- 核心参数 ---
    condition: str,
    target_col: str,
    action: str,
    # --- 可选的操作值参数 (三选一) ---
    f_string: Optional[str] = None,
    expression: Optional[str] = None,
    value: Any = None,
    **kwargs: Any
):
    """
    根据指定条件，对单个目标列执行一次更新操作。
    函数参数直接暴露，使YAML配置极为清晰。

    参数:
    condition: Pandas查询表达式，用于筛选要修改的行。
    target_col: 需要被更新值的列名。
    action: 操作类型 ('format', 'eval', 'set_value')。
    f_string: 当 action 为 'format' 时使用。
    expression: 当 action 为 'eval' 时使用。
    value: 当 action 为 'set_value' 时使用。
    """
    df = pd.read_parquet(input_path.path)
    print(f"应用单列更新规则: condition='{condition}', target_col='{target_col}', action='{action}'")
    
    indices_to_update = df.query(condition).index

    if indices_to_update.empty:
        print("  -> 条件未匹配任何行，无改动。")
    else:
        print(f"  -> 找到 {len(indices_to_update)} 行需要更新。")
        if target_col not in df.columns:
            print(f"  -> 警告: 目标列 '{target_col}' 不存在，将创建新列。")

        new_values = None
        
        if action == 'format':
            if f_string is None: raise ValueError("操作 'format' 需要 'f_string' 参数。")
            def generate_formatted_value(row):
                return f_string.format(**row.to_dict())
            new_values = df.loc[indices_to_update].apply(generate_formatted_value, axis=1)

        elif action == 'eval':
            if expression is None: raise ValueError("操作 'eval' 需要 'expression' 参数。")
            all_new_values = df.eval(expression)
            new_values = all_new_values.loc[indices_to_update]

        elif action == 'set_value':
            if value is None: raise ValueError("操作 'set_value' 需要 'value' 参数。")
            df.loc[indices_to_update, target_col] = value
            # 直接赋值后即可保存，无需后续步骤
            df.to_parquet(output_path)
            return
        else:
            raise ValueError(f"不支持的操作类型: '{action}'。支持的操作: 'format', 'eval', 'set_value'。")

        df.loc[indices_to_update, target_col] = new_values

    df.to_parquet(output_path)