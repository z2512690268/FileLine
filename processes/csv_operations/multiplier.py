from pathlib import Path
import pandas as pd
from core.processing import ProcessorRegistry

@ProcessorRegistry.register(name="csv_multiplier", input_type="single", output_ext=".csv")
def multiply_csv_numbers(input_path: dict, output_path: Path, multiplier: float = 1.0):
    """CSV数值倍增处理
    :param multiplier: 数值倍增系数 (默认1.0)
    """
    # 读取CSV文件
    df = pd.read_csv(input_path["path"])
    
    # 定义数值转换函数
    def try_multiply(x):
        try:
            return float(x) * multiplier
        except (ValueError, TypeError):
            return x
    
    # 应用数值转换
    processed_df = df.map(try_multiply)
    
    # 保存处理结果
    processed_df.to_csv(output_path, index=False)