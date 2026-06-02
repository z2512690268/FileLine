"""CSV 纵向拼接: 多输入 → 单输出"""
import pandas as pd
from pathlib import Path
from core.processing import ProcessorRegistry


@ProcessorRegistry.register("csv_concat", input_type="multi", output_ext=".csv")
def csv_concat(input_paths, output_path):
    """将多个 CSV 文件纵向拼接 (按行合并)"""
    dfs = [pd.read_csv(p.path) for p in input_paths]
    merged = pd.concat(dfs, ignore_index=True)
    merged.to_csv(output_path, index=False)
    return ["concatenated"]
