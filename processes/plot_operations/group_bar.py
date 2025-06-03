from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_grouped_bar(input_path: InputPath, output_path: Path,
                     main_group_col: str,
                     sub_group_col: str,
                     value_col: str,
                     row_col: Optional[str] = None,
                     col_col: Optional[str] = None,
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
                     row_order: Optional[List] = None,
                     col_order: Optional[List] = None,
                     main_group_labels: Optional[Dict] = None,
                     sub_group_labels: Optional[Dict] = None,
                     row_labels: Optional[Dict] = None,
                     col_labels: Optional[Dict] = None,
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
                     subplot_title_fontsize: int = 10,
                     subplot_title_fontfamily: Optional[str] = None,
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
    """绘制分组柱状图（支持四维变量：主分组、子分组、行分组和列分组）"""

    # 读取数据
    df = pd.read_csv(input_path.path)
    
    # 验证数据列
    required_cols = [main_group_col, sub_group_col, value_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")
    
    # 验证可选的分组列
    if row_col and row_col not in df.columns:
        raise ValueError(f"行分组列 '{row_col}' 不存在")
    if col_col and col_col not in df.columns:
        raise ValueError(f"列分组列 '{col_col}' 不存在")

    # 处理主分组顺序
    if main_group_order is not None:
        existing_main = df[main_group_col].unique()
        missing_main = set(main_group_order) - set(existing_main)
        if missing_main:
            raise ValueError(f"main_group_order中的以下值在数据中不存在: {missing_main}")
        main_groups = list(main_group_order)
    else:
        main_groups = sorted(df[main_group_col].unique())

    # 处理子分组顺序
    if sub_group_order is not None:
        existing_sub = df[sub_group_col].unique()
        missing_sub = set(sub_group_order) - set(existing_sub)
        if missing_sub:
            raise ValueError(f"sub_group_order中的以下值在数据中不存在: {missing_sub}")
        sub_groups = list(sub_group_order)
    else:
        sub_groups = sorted(df[sub_group_col].unique())
    
    # 处理行分组顺序
    row_groups = []
    if row_col:
        if row_order:
            existing_row = df[row_col].unique()
            missing_row = set(row_order) - set(existing_row)
            if missing_row:
                raise ValueError(f"row_order中的以下值在数据中不存在: {missing_row}")
            row_groups = list(row_order)
        else:
            row_groups = sorted(df[row_col].unique())
    else:
        row_groups = [None]
        
    # 处理列分组顺序
    col_groups = []
    if col_col:
        if col_order:
            existing_col = df[col_col].unique()
            missing_col = set(col_order) - set(existing_col)
            if missing_col:
                raise ValueError(f"col_order中的以下值在数据中不存在: {missing_col}")
            col_groups = list(col_order)
        else:
            col_groups = sorted(df[col_col].unique())
    else:
        col_groups = [None]
    
    # 创建分组颜色映射
    if colors is None:
        default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        colors = [default_colors[i % len(default_colors)] for i in range(len(sub_groups))]
    elif isinstance(colors, dict):
        colors = [colors.get(sg, 'gray') for sg in sub_groups]
    elif isinstance(colors, list):
        colors = colors * ((len(sub_groups) // len(colors)) + 1)[:len(sub_groups)]
    
    # 全局计算布局参数 - 确保所有子图使用相同的x轴位置
    m = len(sub_groups)
    total_width_per_main_group = m * bar_width + (m - 1) * sub_group_gap
    x_main_global = np.arange(len(main_groups)) * (total_width_per_main_group + main_group_gap)
    offsets_global = (np.arange(m) - (m-1)/2) * (bar_width + sub_group_gap)
    
    # 计算全局x轴范围
    x_min_global = x_main_global[0] - (total_width_per_main_group + main_group_gap) * 0.5
    x_max_global = x_main_global[-1] + (total_width_per_main_group + main_group_gap) * 0.5
    
    # 统一的x轴标签
    if main_group_labels:
        xtick_labels = [main_group_labels.get(mg, mg) for mg in main_groups]
    else:
        xtick_labels = main_groups

    # 创建图例句柄
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=colors[j], 
                      label=sub_group_labels.get(sg, sg) if sub_group_labels else sg)
        for j, sg in enumerate(sub_groups)
    ]

    # 判断是否是子图模式
    has_subplots = (row_col is not None) or (col_col is not None)
    nrows = len(row_groups)
    ncols = len(col_groups)

    if has_subplots:
        # 创建子图网格
        fig = plt.figure(figsize=figsize, dpi=dpi)
        gs = GridSpec(nrows, ncols, figure=fig)
        
        # 存储所有坐标轴对象
        axes = []
        
        # 遍历所有分组组合
        for r, row_val in enumerate(row_groups):
            for c, col_val in enumerate(col_groups):
                # 筛选当前子集的数据
                sub_df = df.copy()
                if row_col and row_val is not None:
                    sub_df = sub_df[sub_df[row_col] == row_val]
                if col_col and col_val is not None:
                    sub_df = sub_df[sub_df[col_col] == col_val]
                
                # 创建子图
                ax = fig.add_subplot(gs[r, c])
                axes.append(ax)
                
                # 为空数据添加占位符
                if sub_df.empty:
                    ax.text(0.5, 0.5, "No Data", ha='center', va='center')
                    continue
                
                # 确保所有子图有相同的x轴位置，即使某些分组数据缺失
                for j, sg in enumerate(sub_groups):
                    color = colors[j]
                    for i, mg in enumerate(main_groups):
                        # 检查数据是否存在
                        mask = (sub_df[main_group_col] == mg) & (sub_df[sub_group_col] == sg)
                        if not mask.any():
                            # 没有数据，跳过但不改变位置
                            continue
                            
                        y = sub_df[mask][value_col].agg(aggregate_func)
                        x = x_main_global[i] + offsets_global[j]
                        ax.bar(x, y, bar_width, color=color)
                
                # 在所有子图中使用相同的x轴刻度和标签
                ax.set_xticks(x_main_global)
                ax.set_xticklabels(xtick_labels, rotation=xticks_rotation)
                
                # 设置坐标轴范围
                if xlim:
                    ax.set_xlim(xlim)
                else:
                    ax.set_xlim(x_min_global, x_max_global)
                    
                if ylim:
                    ax.set_ylim(ylim)
                else:
                    # 自动调整y轴范围包含0点
                    ymin, ymax = ax.get_ylim()
                    if ymin > 0:
                        ax.set_ylim(0, ymax * 1.05)
                    elif ymax < 0:
                        ax.set_ylim(ymin * 1.05, 0)
                
                # 在所有子图中使用相同的分隔线位置
                if show_main_group_separators and len(main_groups) > 1:
                    separators_x = [
                        (x_main_global[i] + x_main_global[i+1]) / 2
                        for i in range(len(main_groups)-1)
                    ]
                    for x in separators_x:
                        ax.axvline(
                            x=x,
                            color='gray',
                            linestyle='--',
                            linewidth=0.8,
                            zorder=0
                        )
                
                # 添加网格
                if grid:
                    ax.grid(True, linestyle='--', alpha=0.6)
                
                # 添加子图标题
                title_parts = []
                if row_col and row_val is not None:
                    title_parts.append(f"{row_col}: {row_labels.get(row_val, row_val) if row_labels else row_val}")
                if col_col and col_val is not None:
                    title_parts.append(f"{col_col}: {col_labels.get(col_val, col_val) if col_labels else col_val}")
                if title_parts:
                    ax.set_title(" | ".join(title_parts), fontsize=subplot_title_fontsize, 
                                 fontfamily=subplot_title_fontfamily)
                
                # 设置标签字体
                ax.tick_params(axis='x', labelsize=xticks_fontsize)
                ax.tick_params(axis='y', labelsize=yticks_fontsize)
                if xticks_fontfamily:
                    for label in ax.get_xticklabels():
                        label.set_family(xticks_fontfamily)
                if yticks_fontfamily:
                    for label in ax.get_yticklabels():
                        label.set_family(yticks_fontfamily)
                
                # 只在边缘设置标签
                if r == nrows - 1:
                    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
                if c == 0:
                    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)
        
        # 添加整个图形的标题
        fig.suptitle(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
        
        # 添加共享图例
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
        
        # 添加图例
        fig.legend(**{k: v for k, v in legend_params.items() if v is not None})
        
        # 调整布局
        plt.tight_layout()
        fig.subplots_adjust(top=0.92 if nrows * ncols > 1 else 0.88)  # 为标题留出空间
        
    else:
        # 非子图模式 - 原逻辑
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
        
        # 使用全局布局参数
        for j, sg in enumerate(sub_groups):
            color = colors[j]
            for i, mg in enumerate(main_groups):
                mask = (df[main_group_col] == mg) & (df[sub_group_col] == sg)
                if not mask.any():
                    continue
                y = df[mask][value_col].agg(aggregate_func)
                x = x_main_global[i] + offsets_global[j]
                ax.bar(x, y, bar_width, color=color)
        
        # 使用全局标签
        ax.set_xticks(x_main_global)
        ax.set_xticklabels(xtick_labels, rotation=xticks_rotation)
        
        # 设置坐标轴范围
        if xlim: 
            ax.set_xlim(xlim)
        else:
            ax.set_xlim(x_min_global, x_max_global)
            
        if ylim: 
            ax.set_ylim(ylim)
        else:
            ymin, ymax = ax.get_ylim()
            if ymin > 0:
                ax.set_ylim(0, ymax * 1.05)
            elif ymax < 0:
                ax.set_ylim(ymin * 1.05, 0)
        
        # 添加分隔线
        if show_main_group_separators and len(main_groups) > 1:
            separators_x = [
                (x_main_global[i] + x_main_global[i+1]) / 2
                for i in range(len(main_groups)-1)
            ]
            for x in separators_x:
                ax.axvline(
                    x=x,
                    color='gray',
                    linestyle='--',
                    linewidth=0.8,
                    zorder=0
                )
        
        # 添加网格
        if grid:
            ax.grid(True, linestyle='--', alpha=0.6)
        
        # 设置图表元素
        ax.set_title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
        ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
        ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)
        
        # 设置刻度字体
        ax.tick_params(axis='x', labelsize=xticks_fontsize)
        ax.tick_params(axis='y', labelsize=yticks_fontsize)
        if xticks_fontfamily:
            for label in ax.get_xticklabels():
                label.set_family(xticks_fontfamily)
        if yticks_fontfamily:
            for label in ax.get_yticklabels():
                label.set_family(yticks_fontfamily)
        
        # 添加图例
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
        
        plt.tight_layout()
    
    # 保存图像
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()

    return ["auto_plot", "grouped_bar", f"dpi_{dpi}", f"subplots_{nrows}x{ncols}"]