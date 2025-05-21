# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(input_type="single", output_ext=".csv")
def filter_runtime(input_path: InputPath, output_path: Path, threshold: float):
    """
    将两个文件的loss整理为用于绘制线图的形式
    """

    input_path = Path(input_path.path)
    df = pd.read_csv(input_path)
    # df file_path列如果有before, 则tag列为before, 否则为after
    file_path = df['file_path']
    # basename不要后缀
    file_path = file_path.apply(lambda x: Path(x).stem)
    # 去掉后缀

    df["model"] = file_path.apply(lambda x: x.split('-')[0])
    df["ckpt_sol"] = file_path.apply(lambda x: x.split('-')[1])
    df["num_steps"] = file_path.apply(lambda x: x.split('-')[2])
    df["ckpt_steps"] = file_path.apply(lambda x: x.split('-')[3])
    df["batch_size"] = file_path.apply(lambda x: x.split('-')[4])
    df["seq_len"] = file_path.apply(lambda x: x.split('-')[5])
    df["same_length"] = file_path.apply(lambda x: x.split('-')[6])

    # 只保留batch_size * step 值为300的行
    df = df[df['batch_size'] * df['num_steps'] == 300]
    df.to_csv(output_path, index=False)