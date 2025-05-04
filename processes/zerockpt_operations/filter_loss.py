from pathlib import Path
import pandas as pd
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(name="filter_loss", input_type="single", output_ext=".csv")
def filter_loss(input_path: InputPath, output_path: Path):
    """
    从一个多列的csv文件中，过滤只留下step和loss两列，并保存到output_path
    step列改名time列
    """
    input_path = Path(input_path.path)
    df = pd.read_csv(input_path)
    df = df[["step", "loss"]]
    df.rename(columns={"step": "time"}, inplace=True)
    df.to_csv(output_path, index=False)