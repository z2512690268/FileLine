from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def add_columns(input_path: InputPath, output_path: Path,
                new_columns: Dict[str, Union[str, int, float]],
                overwrite: bool = True):
    """
    新增列到数据表格
    
    参数:
        input_path: 输入文件路径
        output_path: 输出文件路径
        new_columns: 新增列定义字典 {列名: 表达式或值}
        overwrite: 如果列已存在是否覆盖
    
    示例:
        new_columns = {
            'full_name': '`first_name` + " " + `last_name`',  # 表达式
            'bonus': 1000,                                   # 固定值
            'adjusted_price': 'price * 0.9',                 # 计算列
            'is_adult': 'age >= 18'                          # 布尔列
        }
    """
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    else:
        raise ValueError("不支持的文件格式")
    original_cols = set(df.columns)
    
    stats = []
    for col_name, expression in new_columns.items():
        # 处理已存在列
        if col_name in df.columns:
            if not overwrite:
                stats.append(f"skipped_exists:{col_name}")
                continue
            stats.append(f"overwritten:{col_name}")
        else:
            stats.append(f"added:{col_name}")
        
        # 计算新列
        if isinstance(expression, str):
            try:
                # 使用eval计算表达式
                df[col_name] = df.eval(expression)
            except Exception as e:
                raise ValueError(f"计算列'{col_name}'失败: {str(e)}")
        else:
            # 固定值赋值
            df[col_name] = expression
    
    # 保存结果
    df.to_parquet(output_path, index=False)
    
    # 返回统计信息
    new_cols = set(df.columns) - original_cols
    overwritten_cols = set(df.columns) & original_cols & set(new_columns.keys())
    
    return [
        "add_columns",
        f"added:{len(new_cols)}",
        f"overwritten:{len(overwritten_cols)}",
        *stats
    ]