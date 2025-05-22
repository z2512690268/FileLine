# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(input_type="single", output_ext=".csv")
def filter_stalltime(input_path: InputPath, output_path: Path, num_steps: int = 150):
    """
    将两个文件的loss整理为用于绘制线图的形式
    """

    input_path = Path(input_path.path)
    df = pd.read_csv(input_path)
   
    # 过滤所有

    df.to_csv(output_path, index=False)