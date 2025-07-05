from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".parquet")
def filter_table(input_path: InputPath, output_path: Path,
                conditions: Union[str, Dict[str, str]],
                operator: str = "and",
                tag_col: Optional[str] = None,
                keep_missing: bool = True):
    """
    通用表格过滤函数
    
    参数:
        input_path: 输入文件路径
        output_path: 输出文件路径
        conditions: 过滤条件，可以是字符串表达式或列条件字典
        operator: 多条件组合方式 ("and", "or")
        tag_col: 分组列名（用于分组过滤）
        keep_missing: 当数值缺失时是否保留该行
    
    条件字典格式示例:
        conditions = {
            'age': '>18',         # 数值比较
            'gender': "in ['M','F']",  # 包含在列表中
            'income': '>=50000',   # 另一数值条件
            'city': "== 'New York'" # 字符串匹配
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
    
    # 应用过滤条件
    if isinstance(conditions, str):
        # 单一字符串表达式
        filtered_df = df.query(conditions, engine='python')
    elif isinstance(conditions, dict):
        # 多条件字典组合
        query_parts = []
        for col, cond in conditions.items():
            if col not in df.columns:
                if keep_missing:
                    # 如果列不存在但选择保留缺失，则跳过该条件
                    continue
                else:
                    raise ValueError(f"列名 '{col}' 不存在")
            
            query_parts.append(f"`{col}` {cond}")
        
        # 构建完整的查询表达式
        if query_parts:
            join_op = " & " if operator.lower() == "and" else " | "
            full_query = join_op.join(query_parts)
            filtered_df = df.query(full_query, engine='python')
        else:
            filtered_df = df.copy()  # 没有有效条件则返回原表
    else:
        raise TypeError("conditions 应为字符串或字典类型")
    
    filtered_rows = len(filtered_df)
    
    # 分组过滤（可选）
    if tag_col and tag_col in df.columns:
        # 记录每组过滤前后的数量
        group_stats = []
        groups = filtered_df.groupby(tag_col)
        for name, group in groups:
            group_stats.append(f"{name}:{len(group)}")
        group_info = f"groups:({','.join(group_stats)})"
    else:
        group_info = ""
    
    # 保存过滤结果（保持原始格式）
    filtered_df.to_parquet(output_path, index=False)
    
    # 返回处理统计信息
    return [
        "table_filter",
        f"rows_{original_rows}_to_{filtered_rows}",
        f"ratio_{filtered_rows/max(original_rows,1):.2f}"
    ]





