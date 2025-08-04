from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import Any

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def filter_by_condition(
    input_path: InputPath, 
    output_path: Path,
    filter_expression: str,
    **kwargs: Any # 捕获其他未使用的参数
):
    """
    根据给定的Pandas查询表达式过滤DataFrame中的行。

    这是一个通用的行过滤器，可以通过YAML中的参数进行配置。

    参数:
    input_path: 输入的Parquet文件路径。
    output_path: 输出过滤后数据的Parquet文件路径。
    filter_expression: 一个有效的Pandas查询表达式字符串。
                       例如:
                         - "category != 'Background'"
                         - "duration > 0.1"
                         - "step >= 100 and step < 200"
                         - "sub_category in ['Forward', 'Backward']"
    """
    # 1. 检查参数是否有效
    if not filter_expression or not isinstance(filter_expression, str):
        raise ValueError("参数'filter_expression'必须是一个有效的非空字符串。")

    # 2. 读取输入数据
    df = pd.read_parquet(input_path.path)
    
    # 3. 使用pandas的query方法执行过滤
    # 这是最核心的一步，非常强大且安全
    print(f"原始数据行数: {len(df)}")
    print(f"应用过滤条件: '{filter_expression}'")
    
    try:
        filtered_df = df.query(filter_expression)
    except Exception as e:
        print(f"应用查询表达式时出错: {e}")
        print("请检查列名和表达式语法是否正确。可用列: ", df.columns.tolist())
        raise e

    print(f"过滤后数据行数: {len(filtered_df)}")

    # 4. 保存过滤后的数据
    filtered_df.to_parquet(output_path)