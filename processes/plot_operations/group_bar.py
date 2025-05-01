from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry

@ProcessorRegistry.register(name="plot_grouped_bar", input_type="single", output_ext=".pdf")
def plot_grouped_bar(input_path: dict, output_path: Path,
                     main_group_col: str,
                     sub_group_col: str,
                     value_col: str,
                     figsize: tuple = (10, 6),
                     bar_width: float = 0.2,
                     sub_group_gap: float = 0.05,
                     colors: Union[List, Dict] = None,
                     title: str = "Grouped Bar Chart",
                     xlabel: str = "Main Groups",
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
                     # 图例参数
                     legend_loc: Union[str, tuple] = "best",
                     legend_title: Optional[str] = None,
                     legend_fontsize: Optional[int] = None,
                     legend_fontfamily: Optional[str] = None,
                     legend_shadow: bool = False,
                     legend_frameon: bool = True,
                     legend_facecolor: Optional[str] = None,
                     legend_edgecolor: Optional[str] = None,
                     # 数据聚合参数
                     aggregate_func: str = 'mean'):
    """绘制分组柱状图（支持双分组变量和多种自定义样式）
    
    参数说明:
        main_group_col: 主分组列名
        sub_group_col: 子分组列名
        value_col: 数值列名
        bar_width: 单柱宽度 (默认0.2)
        sub_group_gap: 子分组间距 (默认0.05)
        colors: 颜色列表或字典 (按子分组分配)
        aggregate_func: 数据聚合方式 ('mean', 'sum', 'first'等)
        其他参数与样例保持一致
    """
    # 读取数据
    df = pd.read_csv(input_path["path"])
    
    # 验证数据列
    for col in [main_group_col, sub_group_col, value_col]:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")

    # 处理分组数据
    main_groups = sorted(df[main_group_col].unique())
    sub_groups = sorted(df[sub_group_col].unique())
    m = len(sub_groups)
    x_main = np.arange(len(main_groups))

    # 计算子分组偏移量
    offsets = (np.arange(m) - (m-1)/2) * (bar_width + sub_group_gap)

    # 处理颜色参数
    if colors is None:
        default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        colors = [default_colors[i % len(default_colors)] for i in range(m)]
    elif isinstance(colors, dict):
        colors = [colors.get(sg, 'gray') for sg in sub_groups]
    elif isinstance(colors, list):
        colors = colors * (m // len(colors) + 1)[:m]

    # 创建画布
    plt.figure(figsize=figsize)
    ax = plt.gca()

    # 绘制柱状图
    handles = []
    for j, sg in enumerate(sub_groups):
        color = colors[j]
        values = []
        for i, mg in enumerate(main_groups):
            mask = (df[main_group_col] == mg) & (df[sub_group_col] == sg)
            if not mask.any():
                values.append(0)
                continue
            y = df[mask][value_col].agg(aggregate_func)
            x = x_main[i] + offsets[j]
            ax.bar(x, y, bar_width, color=color)
        handles.append(plt.Rectangle((0,0), 1, 1, color=color, label=sg))

    # 设置坐标轴
    ax.set_xticks(x_main)
    ax.set_xticklabels(main_groups)
    
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
    font_settings = {
        'xtick.labels': (xticks_fontfamily,),
        'ytick.labels': (yticks_fontfamily,)
    }
    for labels, family in zip([ax.get_xticklabels(), ax.get_yticklabels()], 
                             [xticks_fontfamily, yticks_fontfamily]):
        if family:
            for label in labels:
                label.set_family(family)

    # 添加图表元素
    ax.set_title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)

    # 配置图例
    legend_params = {
        'handles': handles,
        'loc': legend_loc,
        'title': legend_title,
        'shadow': legend_shadow,
        'frameon': legend_frameon,
        'facecolor': legend_facecolor,
        'edgecolor': legend_edgecolor
    }
    if legend_fontsize or legend_fontfamily:
        legend_params['prop'] = {
            'size': legend_fontsize,
            'family': legend_fontfamily
        }
    ax.legend(**{k: v for k, v in legend_params.items() if v is not None})

    # 添加网格
    if grid: ax.grid(True, linestyle='--', alpha=0.6)

    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()

    return ["auto_plot", "grouped_bar", f"dpi_{dpi}"]