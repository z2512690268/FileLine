# from pathlib import Path
from core.processing import ProcessorRegistry, InputPath
import pandas as pd
from typing import List
from pathlib import Path

@ProcessorRegistry.register(input_type="single", output_ext=".csv")
def filter_runtime(input_path: InputPath, output_path: Path, type: str = "throughput"):
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

    # step和batch_size均视为int
    df['step'] = df['step'].astype(int)
    df['batch_size'] = df['batch_size'].astype(int)
    df['seq_len'] = df['seq_len'].astype(int)

    df['tokens_num'] = df['batch_size'] * df['seq_len']
    # 删除file_path列
    df.drop(columns=['file_path'], inplace=True)

    if type == "throughput":
        # 只保留batch_size * step 值为300的行
        df = df[df['batch_size'] * df['step'] == 300]
    elif type == "loss":
        # 只保留batch_size * step 值为300的step处前后10个step的loss的平均值
        # 分组维度（确保独立处理不同实验配置）
        group_keys = ['model', 'ckpt_sol', 'num_steps', 'ckpt_steps', 'batch_size', 'seq_len', 'same_length']
        
        # 定义实际损失还原函数
        def calculate_actual_loss(group):
            # 通过累积平均反推实际损失
            group = group.sort_values('step')  # 确保按step排序
            group['cumulative_sum'] = group['loss'] * group['step']
            group['actual_loss'] = group['cumulative_sum'] - group['cumulative_sum'].shift(1, fill_value=0)
            return group

        # 定义滑动窗口平均函数（窗口边界处理改进版）
        def window_average(series, window_size=30):
            return series.rolling(
                window=window_size,
                min_periods=1,
                center=True
            ).mean()

        # 分步处理
        df = (
            df.groupby(group_keys, group_keys=False, sort=False)
            .apply(calculate_actual_loss)
            .groupby(group_keys, group_keys=False)
            .apply(lambda g: g.assign(
                window_loss=lambda x: window_average(x['actual_loss'])
            ))
        )

        # 筛选基准点并保留有效字段
        df = df[df['batch_size'] * df['step'] == 300]
        df = df.drop(columns=['loss', 'cumulative_sum', 'actual_loss'])
    df.to_csv(output_path, index=False)