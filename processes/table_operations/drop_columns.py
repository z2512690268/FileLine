from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def drop_columns(input_path: InputPath, output_path: Path,
                 columns: List[str],
                 ignore_missing: bool = True):
    """
    从数据表格删除列
    
    参数:
        input_path: 输入文件路径
        output_path: 输出文件路径
        columns: 要删除的列名列表
        ignore_missing: 如果列不存在是否忽略（否则报错）
    
    示例:
        columns = ['temp_column', 'old_version', 'sensitive_info']
    """
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    else:
        raise ValueError("不支持的文件格式")
    original_cols = set(df.columns)
    original_count = len(df)
    
    # 查找实际存在的列
    cols_to_drop = [col for col in columns if col in df.columns]
    
    # 检查缺失列
    missing_cols = set(columns) - set(cols_to_drop)
    if missing_cols and not ignore_missing:
        raise ValueError(f"列不存在: {', '.join(missing_cols)}")
    
    # 删除列
    df = df.drop(columns=cols_to_drop)
    
    # 保存结果
    df.to_parquet(output_path, index=False)
    
    # 返回统计信息
    return [
        "drop_columns",
        f"removed:{len(cols_to_drop)}",
        f"total_columns:{len(df.columns)}",
        *[f"removed_col:{col}" for col in cols_to_drop],
        *[f"missing_col:{col}" for col in missing_cols]
    ]