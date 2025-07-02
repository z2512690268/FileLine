from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_bar(input_path: InputPath, output_path: Path,
                    x_col: str,
                    value_col: str,
                    figsize: tuple = (10, 6),
                    bar_width: float = 0.6,
                    color: Union[str, List, Dict] = None,
                    title: str = "Bar Chart",
                    xlabel: str = "Categories",
                    ylabel: str = "Value",
                    grid: bool = True,
                    dpi: int = 300,
                    xlim: tuple = None,
                    ylim: tuple = None,
                    xticks_num: int = None,
                    yticks_num: int = None,
                    xticks_rotation: float = 0,
                    yticks_rotation: float = 0,
                    # 字体参数
                    title_fontsize: int = 14,
                    title_fontfamily: Optional[str] = None,
                    xlabel_fontsize: int = 12,
                    xlabel_fontfamily: Optional[str] = None,
                    ylabel_fontsize: int = 12,
                    ylabel_fontfamily: Optional[str] = None,
                    xticks_fontsize: Optional[int] = None,
                    xticks_fontfamily: Optional[str] = None,
                    yticks_fontsize: Optional[int] = None,
                    yticks_fontfamily: Optional[str] = None,
                    # 数据聚合参数
                    aggregate_func: str = 'mean'):
    """绘制单变量柱状图
    
    参数说明:
        x_col: 类别列名
        value_col: 数值列名
        bar_width: 柱宽度 (默认0.6)
        color: 颜色配置（支持单色、颜色列表或按类别字典）
        aggregate_func: 数据聚合方式 ('mean', 'sum', 'first'等)
    """
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    
    # 验证数据列
    for col in [x_col, value_col]:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")

    # 处理分组数据
    categories = sorted(df[x_col].unique())
    x_pos = np.arange(len(categories))
    
    # 聚合数据
    aggregated = df.groupby(x_col)[value_col].agg(aggregate_func).reindex(categories)

    # 处理颜色参数
    if isinstance(color, dict):
        colors = [color.get(cat, '#1f77b4') for cat in categories]
    elif isinstance(color, list):
        colors = color * (len(categories) // len(color) + 1)[:len(categories)]
    elif color:
        colors = [color] * len(categories)
    else:
        colors = [plt.rcParams['axes.prop_cycle'].by_key()['color'][i % 10] 
                 for i in range(len(categories))]

    # 创建画布
    plt.figure(figsize=figsize)
    ax = plt.gca()

    # 绘制柱状图
    bars = ax.bar(x_pos, aggregated, bar_width, color=colors)

    # 设置坐标轴
    ax.set_xticks(x_pos)
    ax.set_xticklabels(categories)
    
    # 设置坐标轴范围
    if xlim: ax.set_xlim(xlim)
    if ylim: ax.set_ylim(ylim)
    
    # 设置刻度数量
    if xticks_num: ax.xaxis.set_major_locator(plt.MaxNLocator(xticks_num))
    if yticks_num: ax.yaxis.set_major_locator(plt.MaxNLocator(yticks_num))

    # 设置刻度属性
    ax.tick_params(axis='x', labelsize=xticks_fontsize, rotation=xticks_rotation)
    ax.tick_params(axis='y', labelsize=yticks_fontsize, rotation=yticks_rotation)
    
    # 设置字体家族
    for label in ax.get_xticklabels():
        if xticks_fontfamily:
            label.set_family(xticks_fontfamily)
    for label in ax.get_yticklabels():
        if yticks_fontfamily:
            label.set_family(yticks_fontfamily)

    # 添加图表元素
    ax.set_title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)

    # 添加网格
    if grid: ax.grid(True, linestyle='--', alpha=0.6)

    # 自动调整刻度间距
    plt.tight_layout()

    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()

    return ["auto_plot", "single_bar", f"dpi_{dpi}"]