from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_dual_axis_line(input_path: InputPath, output_path: Path,
                        # --- 核心参数 ---
                        time_col: str,
                        value_cols_y1: List[str], # 左Y轴的数据列名列表
                        value_cols_y2: List[str], # 右Y轴的数据列名列表
                        
                        # --- 基础设置 ---
                        figsize: tuple = (10, 6),
                        title: str = "Dual Axis Line Plot",
                        xlabel: str = "Time (s)",
                        grid: bool = True,
                        dpi: int = 300,

                        # --- 左Y轴(Y1)定制 ---
                        ylabel_y1: str = "Primary Y-axis",
                        ylim_y1: Optional[tuple] = None,
                        yticks_num_y1: Optional[int] = None,
                        colors_y1: Optional[List[str]] = ["#1f77b4"],
                        line_styles_y1: Union[str, List[str]] = "-",

                        # --- 右Y轴(Y2)定制 ---
                        ylabel_y2: str = "Secondary Y-axis",
                        ylim_y2: Optional[tuple] = None,
                        yticks_num_y2: Optional[int] = None,
                        colors_y2: Optional[List[str]] = ["#FF0000"],
                        line_styles_y2: Union[str, List[str]] = "-",

                        # --- X轴定制 ---
                        xlim: Optional[tuple] = None,
                        xticks_num: Optional[int] = None,
                        xticks_rotation: float = 0,
                        
                        # --- 字体参数 ---
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

                        # --- 图例参数 ---
                        legend_loc: Union[str, tuple] = "best",
                        legend_title: Optional[str] = None,
                        legend_fontsize: Optional[int] = None,
                        legend_fontfamily: Optional[str] = None,
                        legend_shadow: bool = False,
                        legend_frameon: bool = True,
                        legend_facecolor: Optional[str] = None,
                        legend_edgecolor: Optional[str] = None):
    """
    绘制双Y轴折线图。
    左右两个Y轴可以分别绘制来自不同数据列的一条或多条曲线。
    """
    
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    
    # 验证必要列存在
    required_cols = [time_col] + value_cols_y1 + value_cols_y2
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"文件中缺少必要列: {col}")

    # --- 1. 创建画布和主Y轴(ax1) ---
    fig, ax1 = plt.subplots(figsize=figsize, dpi=dpi)
    
    # --- 2. 创建共享X轴的次Y轴(ax2) ---
    ax2 = ax1.twinx()

    # 准备颜色和线型
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
    
    def get_styles(num_items, colors, line_styles):
        final_colors = colors or [color_cycle[i % len(color_cycle)] for i in range(num_items)]
        if len(final_colors) < num_items:
            final_colors *= (num_items // len(final_colors) + 1)

        final_line_styles = [line_styles] * num_items if isinstance(line_styles, str) else line_styles
        if len(final_line_styles) < num_items:
             final_line_styles *= (num_items // len(final_line_styles) + 1)

        return final_colors[:num_items], final_line_styles[:num_items]

    colors_y1, line_styles_y1 = get_styles(len(value_cols_y1), colors_y1, line_styles_y1)
    colors_y2, line_styles_y2 = get_styles(len(value_cols_y2), colors_y2, line_styles_y2)

    # --- 3. 在左Y轴(ax1)上绘图 ---
    for i, col in enumerate(value_cols_y1):
        ax1.plot(df[time_col], df[col],
                 color=colors_y1[i],
                 linestyle=line_styles_y1[i],
                 label=col)

    # --- 4. 在右Y轴(ax2)上绘图 ---
    for i, col in enumerate(value_cols_y2):
        ax2.plot(df[time_col], df[col],
                 color=colors_y2[i],
                 linestyle=line_styles_y2[i],
                 label=col)

    # --- 5. 配置坐标轴 ---
    # X轴
    ax1.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    if xlim:
        ax1.set_xlim(xlim)
    if xticks_num:
        ax1.xaxis.set_major_locator(plt.MaxNLocator(xticks_num))
    ax1.tick_params(axis='x', rotation=xticks_rotation, labelsize=xticks_fontsize)
    if xticks_fontfamily:
        for label in ax1.get_xticklabels():
            label.set_fontfamily(xticks_fontfamily)

    # 左Y轴(Y1)
    ax1.set_ylabel(ylabel_y1, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily, color=colors_y1[0])
    if ylim_y1:
        ax1.set_ylim(ylim_y1)
    if yticks_num_y1:
        ax1.yaxis.set_major_locator(plt.MaxNLocator(yticks_num_y1))
    ax1.tick_params(axis='y', labelcolor=colors_y1[0], labelsize=yticks_fontsize)
    if yticks_fontfamily:
        for label in ax1.get_yticklabels():
            label.set_fontfamily(yticks_fontfamily)

    # 右Y轴(Y2)
    ax2.set_ylabel(ylabel_y2, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily, color=colors_y2[0])
    if ylim_y2:
        ax2.set_ylim(ylim_y2)
    if yticks_num_y2:
        ax2.yaxis.set_major_locator(plt.MaxNLocator(yticks_num_y2))
    ax2.tick_params(axis='y', labelcolor=colors_y2[0], labelsize=yticks_fontsize)
    if yticks_fontfamily:
        for label in ax2.get_yticklabels():
            label.set_fontfamily(yticks_fontfamily)
    
    # --- 6. 配置全局元素 ---
    plt.title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    if grid:
        ax1.grid(True, linestyle='--', alpha=0.6)

    # --- 7. 创建统一图例 ---
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    all_handles = handles1 + handles2
    all_labels = labels1 + labels2
    
    legend_params = {
        'handles': all_handles,
        'labels': all_labels,
        'loc': legend_loc,
        'title': legend_title,
        'shadow': legend_shadow,
        'frameon': legend_frameon,
        'facecolor': legend_facecolor,
        'edgecolor': legend_edgecolor
    }
    font_props = {}
    if legend_fontsize: font_props['size'] = legend_fontsize
    if legend_fontfamily: font_props['family'] = legend_fontfamily
    if font_props: legend_params['prop'] = font_props
    
    # 在主轴上创建图例
    ax1.legend(**{k: v for k, v in legend_params.items() if v is not None})

    # 调整布局以防止标签重叠
    fig.tight_layout()
    
    # 保存图像
    plt.savefig(output_path, dpi=dpi)
    plt.close()

    return ["auto_plot", "dual_axis_line_chart", f"dpi_{dpi}"]