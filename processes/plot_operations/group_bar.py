from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator  # 用于设置y轴刻度
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
                     hatches: Union[List, Dict] = None,
                     title: str = "Grouped Bar Chart",
                     xlabel: str = "Main Groups",
                     ylabel: str = "Value",
                     grid: bool = True,
                     dpi: int = 300,
                     xlim: tuple = None,
                     ylim: Union[tuple, Dict] = None,  # 修改为可为每个子图单独设置y轴范围
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
                     xlabel_pad: Optional[float] = None,
                     ylabel_fontsize: int = 12,
                     ylabel_fontfamily: Optional[str] = None,
                     xticks_fontsize: Optional[int] = None,
                     xticks_fontfamily: Optional[str] = None,
                     yticks_fontsize: Optional[int] = None,
                     yticks_fontfamily: Optional[str] = None,
                     subplot_title_fontsize: int = 10,
                     subplot_title_fontfamily: Optional[str] = None,
                     subplot_title_y: float = 1.01,          # 新增: 控制子图标题的Y位置
                     show_subplot_titles: bool = True,  # 新增: 是否显示子图标题
                     subplot_titles: Optional[Dict] = None,
                     figure_top_margin: Optional[float] = None, # 新增: 控制整个图表的顶部边距
                     normalization: Optional[Dict] = None,
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
                     aggregate_func: str = 'mean',
                     # 在柱子上方显示数据
                     show_bar_values: bool = False,
                     bar_value_format: str = '{:.1f}',  # 例如 '{:.1f}' 表示保留一位小数，'{:.0f}' 表示整数。
                     bar_value_fontsize: int = 8,
                     bar_value_rotation: float = 90, # 控制数字的旋转角度，90 表示垂直显示。
                     bar_value_fontfamily: Optional[str] = None, # 设置成和刻度轴字体一致
                     bar_value_offset: Optional[float] = None  # 数字距离柱顶的额外偏移量，用于微调位置。
                     ):  
    """绘制分组柱状图（支持四维变量：主分组、子分组、行分组和列分组）"""

    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    print("ckpt_type列中唯一值:", df["ckpt_type"].unique())
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
        
    
    # --- 【新增】步骤1：预先进行数据聚合 ---
    group_by_cols = [col for col in [row_col, col_col, main_group_col, sub_group_col] if col]
    aggregated_df = df.groupby(group_by_cols, as_index=False)[value_col].agg(aggregate_func)

    # --- 【新增】步骤2：执行归一化 ---
    norm_params = normalization or {}
    if norm_params.get("enabled", False):
        ref_conditions = norm_params.get("reference_conditions")
        if not ref_conditions:
            raise ValueError("启用归一化时，必须提供 'reference_conditions'。")

        mask = pd.Series(True, index=aggregated_df.index)
        for col, val in ref_conditions.items():
            mask &= (aggregated_df[col] == val)
        
        ref_rows = aggregated_df[mask]
        if len(ref_rows) != 1:
            raise ValueError(f"根据归一化条件 {ref_conditions} 找到 {len(ref_rows)} 行，必须恰好为1行。")
            
        reference_value = ref_rows.iloc[0][value_col]
        
        if reference_value == 0:
            raise ValueError("归一化的基准值为0，无法进行除法。")

        aggregated_df[value_col] = aggregated_df[value_col] / reference_value
        print(f"数据已根据基准值 {reference_value} 进行归一化。")
    
    # 创建分组颜色映射
    if colors is None:
        default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        colors = [default_colors[i % len(default_colors)] for i in range(len(sub_groups))]
    elif isinstance(colors, dict):
        colors = [colors.get(sg, 'gray') for sg in sub_groups]
    elif isinstance(colors, list):
        colors = (colors * ((len(sub_groups) // len(colors)) + 1))[:len(sub_groups)]

    hatches_list = [None] * len(sub_groups) # 默认所有柱子都没有花纹
    if isinstance(hatches, dict):
        hatches_list = [hatches.get(sg) for sg in sub_groups] # 从字典查找，找不到则为None
    elif isinstance(hatches, list):
        # 如果列表比分组少，则循环使用
        temp_hatches = hatches * ((len(sub_groups) // len(hatches)) + 1)
        hatches_list = temp_hatches[:len(sub_groups)]
    
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

    # 创建图例句柄 支持花纹和颜色
    handles = []
    for j, sg in enumerate(sub_groups):
        label_text = sub_group_labels.get(sg, sg) if sub_group_labels else sg
        
        # 构建图例方块的样式参数
        legend_swatch_kwargs = {
            'facecolor': colors[j]
        }
        current_hatch = hatches_list[j]
        if current_hatch:
            print("当前花纹：", current_hatch)
            legend_swatch_kwargs['hatch'] = current_hatch
            legend_swatch_kwargs['edgecolor'] = 'black' # 为有花纹的图例增加边框以提高清晰度
            
        handles.append(
            plt.Rectangle((0, 0), 1, 1, label=label_text, **legend_swatch_kwargs)
        )

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
                    for i, mg in enumerate(main_groups):
                        # --- 【修改】步骤3：从预聚合数据中查找值，而不是动态计算 ---
                        mask = (aggregated_df[main_group_col] == mg) & (aggregated_df[sub_group_col] == sg)
                        if row_col and row_val is not None:
                            mask &= (aggregated_df[row_col] == row_val)
                        if col_col and col_val is not None:
                            mask &= (aggregated_df[col_col] == col_val)

                        if not mask.any():
                            continue
                        
                        y = aggregated_df[mask][value_col].iloc[0] # 直接取值
                        x = x_main_global[i] + offsets_global[j]
                        bar_kwargs = {
                            'color': colors[j] 
                        }
                        current_hatch = hatches_list[j]
                        if current_hatch:
                            bar_kwargs['hatch'] = current_hatch
                            bar_kwargs['edgecolor'] = 'black'  # 为有花纹的柱子增加边框以提高清晰度
                        ax.bar(x, y, bar_width, **bar_kwargs)

                        # feat：如果需要在柱子上方显示数据
                        if show_bar_values and y is not None and y != 0:
                            # 准备要显示的文本
                            label_text = bar_value_format.format(y)
                            
                            # 计算一个小的垂直偏移量，让文本在柱子上方一点点
                            # 如果用户未指定固定偏移，则自动计算一个（Y轴最大值的1%）
                            offset = bar_value_offset if bar_value_offset is not None else ax.get_ylim()[1] * 0.01
                            y_pos = y + offset
                            
                            # 在(x, y_pos)坐标处添加文本
                            ax.text(x, y_pos, label_text,
                                    ha='center',              # 水平居中对齐
                                    va='bottom',              # 垂直底部对齐
                                    fontsize=bar_value_fontsize,
                                    rotation=bar_value_rotation,
                                    fontfamily=xticks_fontfamily) # 复用x轴标签字体
                
                # 在所有子图中使用相同的x轴刻度和标签
                ax.set_xticks(x_main_global)
                ax.set_xticklabels(xtick_labels, rotation=xticks_rotation)
                
                # 设置坐标轴范围
                if xlim:
                    ax.set_xlim(xlim)
                else:
                    ax.set_xlim(x_min_global, x_max_global)
                    
                # --- 修改2：重构Y轴范围设置逻辑 ---
                subplot_ylim_to_set = None
                if isinstance(ylim, dict):
                    # 字典模式：为特定子图查找Y轴范围
                    key = None
                    if row_col and col_col:
                        key = (row_val, col_val)
                    elif row_col:
                        key = row_val
                    elif col_col:
                        key = col_val
                    
                    subplot_ylim_to_set = ylim.get(key) # 使用.get()安全地获取值
                
                elif isinstance(ylim, (tuple, list)):
                    # 元组/列表模式：为所有子图设置统一范围 (保持向后兼容)
                    subplot_ylim_to_set = ylim

                # 步骤2.1：如果找到了要设置的ylim，则应用它
                if subplot_ylim_to_set:
                    ax.set_ylim(subplot_ylim_to_set)
                else:
                    # 步骤2.2：否则，执行自动范围调整逻辑
                    current_ymin, current_ymax = ax.get_ylim()
                    if current_ymin > 0:
                        ax.set_ylim(0, current_ymax * 1.05)
                    elif current_ymax < 0:
                        ax.set_ylim(current_ymin * 1.05, 0)

                if yticks_num:
                    ax.yaxis.set_major_locator(MaxNLocator(nbins=yticks_num))
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
                
                if show_subplot_titles:
                    title_to_set = None

                    # 步骤1: 优先从 subplot_titles 字典中查找自定义标题
                    if isinstance(subplot_titles, dict):
                        key = None
                        if row_col and col_col:
                            key = (row_val, col_val)
                        elif row_col:
                            key = row_val
                        elif col_col:
                            key = col_val
                        
                        if key is not None:
                            title_to_set = subplot_titles.get(key)
                    
                    # 步骤2: 如果没有找到自定义标题，则回退到自动生成
                    if title_to_set is None:
                        title_parts = []
                        if row_col and row_val is not None:
                            row_display = row_labels.get(row_val, row_val) if row_labels else row_val
                            title_parts.append(f"{row_col}: {row_display}")
                        if col_col and col_val is not None:
                            col_display = col_labels.get(col_val, col_val) if col_labels else col_val
                            title_parts.append(f"{col_col}: {col_display}")
                        
                        if title_parts:
                            title_to_set = " | ".join(title_parts)

                    # 步骤3: 如果最终确定了标题，则设置它
                    if title_to_set is not None:
                        ax.set_title(title_to_set, 
                                     fontsize=subplot_title_fontsize,
                                     fontfamily=subplot_title_fontfamily, 
                                     y=subplot_title_y)
                
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
                    # 设置x轴标签
                    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily, labelpad=xlabel_pad)
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
        }

        has_any_hatch = any(h for h in hatches_list)

        print("是否有花纹：", has_any_hatch)

        if not has_any_hatch:
            legend_params['facecolor'] = legend_facecolor
            legend_params['edgecolor'] = legend_edgecolor

        if legend_fontsize or legend_fontfamily:
            legend_params['prop'] = {
                'size': legend_fontsize,
                'family': legend_fontfamily
            }
        
        # 添加图例
        fig.legend(**{k: v for k, v in legend_params.items() if v is not None})
        
        # 调整布局
        plt.tight_layout()
        if figure_top_margin:
             fig.subplots_adjust(top=figure_top_margin)
        else:
            # 保留默认行为
            #  fig.subplots_adjust(top=0.92 if legend_loc != "upper center" else 0.88)
            fig.subplots_adjust(top=0.92 if nrows * ncols > 1 else 0.88)  # 为标题留出空间
        
    else:
        # 非子图模式 - 原逻辑
        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
        
        # 使用全局布局参数
        for j, sg in enumerate(sub_groups):
            for i, mg in enumerate(main_groups):
                # --- 【修改】步骤3：从预聚合数据中查找值 ---
                mask = (aggregated_df[main_group_col] == mg) & (aggregated_df[sub_group_col] == sg)
                if not mask.any():
                    continue
                y = aggregated_df[mask][value_col].iloc[0] # 直接取值
                x = x_main_global[i] + offsets_global[j]
                bar_kwargs = {
                    'color': colors[j] 
                }
                if hatches_list[j]:
                    bar_kwargs['hatch'] = hatches_list[j]
                    bar_kwargs['edgecolor'] = 'black'  # 为有花纹的柱子增加边框以提高清晰度
                ax.bar(x, y, bar_width, **bar_kwargs)

                # feat：如果需要在柱子上方显示数据
                if show_bar_values and y is not None and y != 0:
                            # 准备要显示的文本
                            label_text = bar_value_format.format(y)
                            
                            # 计算一个小的垂直偏移量，让文本在柱子上方一点点
                            # 如果用户未指定固定偏移，则自动计算一个（Y轴最大值的1%）
                            offset = bar_value_offset if bar_value_offset is not None else ax.get_ylim()[1] * 0.01
                            y_pos = y + offset
                            
                            # 在(x, y_pos)坐标处添加文本
                            ax.text(x, y_pos, label_text,
                                    ha='center',              # 水平居中对齐
                                    va='bottom',              # 垂直底部对齐
                                    fontsize=bar_value_fontsize,
                                    rotation=bar_value_rotation,
                                    fontfamily=xticks_fontfamily) # 复用x轴标签字体
        
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
        if yticks_num:
            ax.yaxis.set_major_locator(MaxNLocator(nbins=yticks_num))
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
        ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily, labelpad=xlabel_pad)
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
        }

        has_any_hatch = any(h for h in hatches_list)
        
        print("是否有花纹：", has_any_hatch)
        if not has_any_hatch:
            legend_params['facecolor'] = legend_facecolor
            legend_params['edgecolor'] = legend_edgecolor
        
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