from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_grouped_bar(input_path: InputPath, output_path: Path,
                     main_group_col: str,
                     sub_group_col: str,
                     value_col: str,
                     figsize: tuple = (10, 6),
                     bar_width: float = 0.2,
                     main_group_gap: float = 0.2,
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
                     # 新增参数
                     show_main_group_separators: bool = False,
                     main_group_order: Optional[List] = None,
                     sub_group_order: Optional[List] = None,
                     main_group_labels: Optional[Dict] = None,
                     sub_group_labels: Optional[Dict] = None,
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
                     legend_bbox_to_anchor: Optional[tuple] = None,
                     legend_ncol: Optional[int] = None,
                     legend_title: Optional[str] = None,
                     legend_fontsize: Optional[int] = None,
                     legend_fontfamily: Optional[str] = None,
                     legend_shadow: bool = False,
                     legend_frameon: bool = True,
                     legend_facecolor: Optional[str] = None,
                     legend_edgecolor: Optional[str] = None,
                     # 数据聚合参数
                     aggregate_func: str = 'mean'):
    """绘制分组柱状图（支持双分组变量和自定义顺序、标签）

    参数说明:
        main_group_col: 主分组列名
        sub_group_col: 子分组列名
        value_col: 数值列名
        show_main_group_separators: 是否显示主分组分隔线
        main_group_order: 主分组的顺序（列表），若未指定则按数据中的唯一值排序
        sub_group_order: 子分组的顺序（列表），若未指定则按数据中的唯一值排序
        main_group_labels: 主分组标签的字典，将原值映射到显示的标签
        sub_group_labels: 子分组标签的字典，将原值映射到显示的标签
        bar_width: 单柱宽度 (默认0.2)
        main_group_gap: 主分组间距 (默认0.2)
        sub_group_gap: 子分组间距 (默认0.05)
        colors: 颜色列表或字典 (按子分组分配)
        aggregate_func: 数据聚合方式 ('mean', 'sum', 'first'等)
    """
    # 读取数据
    df = pd.read_csv(input_path.path)
    
    # 验证数据列
    for col in [main_group_col, sub_group_col, value_col]:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")

    # 处理主分组顺序和过滤数据
    if main_group_order is not None:
        existing_main = df[main_group_col].unique()
        missing_main = set(main_group_order) - set(existing_main)
        if missing_main:
            raise ValueError(f"main_group_order中的以下值在数据中不存在: {missing_main}")
        df = df[df[main_group_col].isin(main_group_order)]
        main_groups = list(main_group_order)
    else:
        main_groups = sorted(df[main_group_col].unique())

    # 处理子分组顺序和过滤数据
    if sub_group_order is not None:
        existing_sub = df[sub_group_col].unique()
        missing_sub = set(sub_group_order) - set(existing_sub)
        if missing_sub:
            raise ValueError(f"sub_group_order中的以下值在数据中不存在: {missing_sub}")
        df = df[df[sub_group_col].isin(sub_group_order)]
        sub_groups = list(sub_group_order)
    else:
        sub_groups = sorted(df[sub_group_col].unique())

    # 计算布局参数
    m = len(sub_groups)
    total_width_per_main_group = m * bar_width + (m - 1) * sub_group_gap
    x_main = np.arange(len(main_groups)) * (total_width_per_main_group + main_group_gap)
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
        for i, mg in enumerate(main_groups):
            mask = (df[main_group_col] == mg) & (df[sub_group_col] == sg)
            if not mask.any():
                continue
            y = df[mask][value_col].agg(aggregate_func)
            x = x_main[i] + offsets[j]
            ax.bar(x, y, bar_width, color=color)
        # 处理子分组标签
        sg_label = sub_group_labels.get(sg, sg) if sub_group_labels else sg
        handles.append(plt.Rectangle((0,0), 1, 1, color=color, label=sg_label))

    # 设置坐标轴标签和范围
    if main_group_labels:
        xtick_labels = [main_group_labels.get(mg, mg) for mg in main_groups]
    else:
        xtick_labels = main_groups
    ax.set_xticks(x_main)
    ax.set_xticklabels(xtick_labels)
    
    if xlim: ax.set_xlim(xlim)
    if ylim: ax.set_ylim(ylim)
    
    # 新增分隔线绘制（在设置坐标轴范围后）
    if show_main_group_separators and len(main_groups) > 1:
        # 计算分隔线位置（主分组间的中点）
        separators_x = [
            (x_main[i] + x_main[i+1]) / 2
            for i in range(len(main_groups)-1)
        ]
        for x in separators_x:
            ax.axvline(
                x = x,
                color = 'gray',
                linestyle = '--',
                linewidth = 0.8,
                zorder = 0  # 确保在柱状图下方
            )

    # 设置刻度和字体
    ax.tick_params(axis='x', labelsize=xticks_fontsize, rotation=xticks_rotation)
    ax.tick_params(axis='y', labelsize=yticks_fontsize, rotation=yticks_rotation)
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
    
    # 配置图例
    legend_params = {
        'handles': handles,
        'loc': legend_loc,
        'bbox_to_anchor': legend_bbox_to_anchor,
        'ncol': legend_ncol,
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

    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()

    return ["auto_plot", "grouped_bar", f"dpi_{dpi}"]