from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def groupby_table(input_path: InputPath, output_path: Path,
                  group_keys: List[str],
                  aggregations: Dict[str, Union[str, List[str]]],
                  as_index: bool = False):
    """
    分组聚合数据表
    
    参数:
        input_path: 输入文件路径
        output_path: 输出文件路径
        group_keys: 分组列名列表
        aggregations: 聚合配置 {列名: 聚合函数(s)} 
        as_index: 是否将分组列作为索引
    
    示例:
        aggregations = {
            'sales': 'sum',              # 单聚合
            'age': ['mean', 'max'],      # 多聚合
            'city': 'count'               # 计数
        }
    """
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    else:
        raise ValueError("不支持的文件格式")
    original_rows = len(df)
    
    # 验证分组列存在
    missing_keys = [key for key in group_keys if key not in df.columns]
    if missing_keys:
        raise ValueError(f"分组列不存在: {', '.join(missing_keys)}")
    
    # 应用分组聚合
    grouped = df.groupby(group_keys, as_index=as_index)
    
    # 准备聚合配置
    agg_config = {}
    for col, funcs in aggregations.items():
        if isinstance(funcs, str):
            funcs = [funcs]
        agg_config[col] = funcs
    
    # 执行聚合
    try:
        result_df = grouped.agg(agg_config)
    except KeyError as e:
        missing_col = str(e).replace("'", "")
        if missing_col in aggregations:
            raise ValueError(f"聚合列不存在: {missing_col}")
        raise
    except Exception as e:
        raise RuntimeError(f"分组聚合失败: {str(e)}")
    
    # 处理多级列名
    if isinstance(result_df.columns, pd.MultiIndex):
        result_df.columns = ['_'.join(col).strip() for col in result_df.columns.values]
    elif not as_index:
        # 重置索引名
        result_df.columns = [f"{col}_{func}" for col, funcs in aggregations.items() for func in (funcs if isinstance(funcs, list) else [funcs])]
    
    # 保存结果
    result_df.to_parquet(output_path, index=False)
    
    # 返回统计信息
    return [
        "groupby_table",
        f"original_rows:{original_rows}",
        f"result_groups:{len(result_df)}",
        f"aggregated_cols:{len(aggregations)}",
        f"grouped_by:{','.join(group_keys)}"
    ]