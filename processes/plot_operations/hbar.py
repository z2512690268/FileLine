from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_horizontal_bar(
    input_path: InputPath, 
    output_path: Path,
    y_col: str,             # 类别列（垂直轴）
    value_col: str,         # 数值列（水平轴）
    figsize: tuple = (10, 6),
    bar_height: float = 0.6,  # 条带高度（原bar_width）
    color: Union[str, List, Dict] = None,
    title: str = "Horizontal Bar Chart",
    xlabel: str = "Value",
    ylabel: str = "Categories",
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
    aggregate_func: str = 'mean',
    # 新增参数
    show_value_labels: bool = False,
    label_format: str = "{:.2f}",
    label_padding: int = 3
):
    """绘制水平方向柱状图，特别适用于breakdown数据
    
    参数说明:
        y_col: 类别列名（垂直轴）
        value_col: 数值列名（水平轴）
        bar_height: 条带高度（默认0.6）
        color: 颜色配置（支持单色、颜色列表或按类别字典）
        aggregate_func: 数据聚合方式 ('mean', 'sum', 'first'等)
        show_value_labels: 是否在条带上显示数值标签
        label_format: 数值标签格式
        label_padding: 数值标签内边距
    """
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    
    # 验证数据列
    for col in [y_col, value_col]:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")

    # 处理分组数据
    categories = sorted(df[y_col].unique())
    y_pos = np.arange(len(categories))
    
    # 聚合数据
    aggregated = df.groupby(y_col)[value_col].agg(aggregate_func).reindex(categories)

    # 处理颜色参数
    if isinstance(color, dict):
        colors = [color.get(cat, '#1f77b4') for cat in categories]
    elif isinstance(color, list):
        colors = color * (len(categories) // len(color) + 1)[:len(categories)]
    elif color:
        colors = [color] * len(categories)
    else:
        colors = plt.cm.tab10(np.linspace(0, 1, len(categories)))  # 使用调色板

    # 创建画布和坐标轴
    fig, ax = plt.subplots(figsize=figsize)

    # 绘制水平柱状图
    bars = ax.barh(y_pos, aggregated, height=bar_height, color=colors, edgecolor='black')

    # 设置坐标轴标签
    ax.set_yticks(y_pos)
    ax.set_yticklabels(categories)
    
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
    if xticks_fontfamily:
        for label in ax.get_xticklabels():
            label.set_family(xticks_fontfamily)
    if yticks_fontfamily:
        for label in ax.get_yticklabels():
            label.set_family(yticks_fontfamily)

    # 添加图表元素
    ax.set_title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)

    # 添加网格（仅显示x轴网格）
    if grid: ax.grid(axis='x', linestyle='--', alpha=0.6)

    # 添加数据标签
    if show_value_labels:
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(width + label_padding, bar.get_y() + bar.get_height() / 2,
                    label_format.format(width),
                    va='center', ha='left',
                    fontsize=xticks_fontsize if xticks_fontsize else 10)

    # 自动调整布局
    plt.tight_layout()

    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()

    return ["auto_plot", "horizontal_bar", f"dpi_{dpi}", f"categories_{len(categories)}"]