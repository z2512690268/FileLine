# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(input_type="single", output_ext=".csv")
def filter_accuracy_line(input_path: InputPath, output_path: Path):
    """
    将两个文件的loss整理为用于绘制线图的形式
    """

    input_path = Path(input_path.path)
    df = pd.read_csv(input_path)
    # df file_path列如果有before, 则tag列为before, 否则为after
    df["tag"] = df["file_path"].apply(lambda x: "before" if "before" in x else "after")
    df.to_csv(output_path, index=False)