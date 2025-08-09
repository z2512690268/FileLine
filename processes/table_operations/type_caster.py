from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import Dict, Any

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def type_cast(
    input_path: InputPath, 
    output_path: Path,
    type_map: Dict[str, str],
    **kwargs: Any
):
    """
    将DataFrame中指定列转换为指定的类型。

    参数:
    input_path: 输入的Parquet文件路径。
    output_path: 输出转换后数据的Parquet文件路径。
    type_map: 一个字典，键是列名，值是目标类型字符串 (例如 'str', 'int', 'float')。
    """
    df = pd.read_parquet(input_path.path)
    
    print("开始转换列类型...")
    
    for col, target_type in type_map.items():
        if col in df.columns:
            print(f"  - 将列 '{col}' 转换为类型 '{target_type}'")
            try:
                # 特别处理：转换为字符串时，先将空值(NaN)填充为空字符串
                if target_type == 'str' or target_type == str:
                    df[col] = df[col].fillna('').astype(str)
                else:
                    df[col] = df[col].astype(target_type)
            except Exception as e:
                print(f"    -> 转换列 '{col}' 到 '{target_type}' 时出错: {e}")
                raise e
        else:
            print(f"  - 警告: 列 '{col}' 不存在，跳过类型转换。")
            
    df.to_parquet(output_path)