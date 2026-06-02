"""内置演示/测试用 processor"""

from core.processing import ProcessorRegistry


@ProcessorRegistry.register("test_split_by_col", input_type="single", output_type="multi", output_ext="")
def split_by_col(input_path, output_dir, split_col="cat"):
    """按列拆分 CSV 为多个文件（扁平多输出）"""
    import pandas as pd
    df = pd.read_csv(input_path.path)
    results = []
    for val in df[split_col].unique():
        fname = f"{val}.csv"
        df[df[split_col] == val].to_csv(output_dir / fname, index=False)
        results.append((fname, f"val:{val}"))
    return results


@ProcessorRegistry.register("test_named_split", input_type="single", output_type="multi", output_ext="")
def named_split(input_path, output_dir, split_col="cat"):
    """按列拆分 CSV, 返回命名组: {"val": [(fname, tags)]}"""
    import pandas as pd
    df = pd.read_csv(input_path.path)
    output = {}
    for val in df[split_col].unique():
        fname = f"{val}.csv"
        df[df[split_col] == val].to_csv(output_dir / fname, index=False)
        output[str(val)] = [(fname, f"val:{val}")]
    return output
