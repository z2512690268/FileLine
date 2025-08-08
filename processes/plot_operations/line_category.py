from pathlib import Path
from typing import Optional, Union, Dict, List
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_line_categorical(input_path: Union[str, Path], output_path: Path,
                      figsize: tuple = (10, 6),
                      x_col: str = "category",
                      value_col: str = "value",
                      tag_col: Optional[str] = None,
                      x_order: Optional[List] = None,
                      x_labels: Optional[Dict] = None,
                      marker: Union[str, Dict[str, str]] = 'o',
                      line_width: float = 1.5,
                      colors: Union[List, Dict] = None,
                      tag_line_styles: Union[str, Dict[str, str]] = "-",
                      title: str = "Line Plot",
                      xlabel: str = "Category",
                      ylabel: str = "Value",
                      grid: bool = True,
                      dpi: int = 300,
                      xlim: tuple = None,
                      ylim: tuple = None,
                      yticks_num: int = None,
                      xticks_rotation: float = 0,
                      yticks_rotation: float = 0,
                      title_fontsize: int = 14,
                      # 【新增】字体参数
                      title_fontfamily: Optional[str] = None,
                      xlabel_fontsize: int = 12,
                      xlabel_fontfamily: Optional[str] = None,
                      ylabel_fontsize: int = 12,
                      ylabel_fontfamily: Optional[str] = None,
                      xticks_fontsize: Optional[int] = None,
                      xticks_fontfamily: Optional[str] = None,
                      yticks_fontsize: Optional[int] = None,
                      yticks_fontfamily: Optional[str] = None,
                      # 【新增】图例显示开关
                      show_legend: bool = True,
                      legend_loc: Union[str, tuple] = "best",
                      legend_title: Optional[str] = None,
                      legend_fontsize: Optional[int] = None,
                      legend_fontfamily: Optional[str] = None
                      ):
    """
    绘制将X轴视为离散类别的折线图 (类似柱状图顶点连线的效果)。
    """
    # --- 数据读取和验证 ---
    input_path_obj = Path(input_path.path) if hasattr(input_path, 'path') else Path(input_path)
    if ".csv" in str(input_path_obj): df = pd.read_csv(input_path_obj)
    elif ".parquet" in str(input_path_obj): df = pd.read_parquet(input_path_obj)
    
    if x_col in df.columns:
        df[x_col] = df[x_col].astype(str)

    required_cols = [x_col, value_col]
    if tag_col: required_cols.append(tag_col)
    for col in required_cols:
        if col not in df.columns: raise ValueError(f"文件中缺少必要列: {col}")

    # --- 处理X轴类别和顺序 ---
    if x_order:
        x_categories = [str(item) for item in x_order]
        df = df[df[x_col].isin(x_categories)]
    else:
        x_categories = sorted(df[x_col].unique())
    
    df[x_col] = pd.Categorical(df[x_col], categories=x_categories, ordered=True)
    df = df.sort_values(by=[tag_col, x_col] if tag_col else x_col)

    # --- 创建画布 ---
    plt.figure(figsize=figsize)
    ax = plt.gca()

    # --- 绘图逻辑 (不变) ---
    if tag_col:
        unique_tags = sorted(df[tag_col].unique())
        color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
        color_map = {tag: color_cycle[i % len(color_cycle)] for i, tag in enumerate(unique_tags)}
        if isinstance(colors, dict): color_map.update(colors)
        elif isinstance(colors, list): color_map = {tag: colors[i % len(colors)] for i, tag in enumerate(unique_tags)}
        style_map = {tag: '-' for tag in unique_tags}
        if isinstance(tag_line_styles, dict): style_map.update(tag_line_styles)
        elif isinstance(tag_line_styles, str): style_map = {tag: tag_line_styles for tag in unique_tags}
        marker_map = {tag: 'o' for tag in unique_tags}
        if isinstance(marker, dict): marker_map.update(marker)
        elif isinstance(marker, str): marker_map = {tag: marker for tag in unique_tags}
        for tag_value, group in df.groupby(tag_col):
            ax.plot(group[x_col], group[value_col], color=color_map.get(tag_value), linestyle=style_map.get(tag_value, "-"),
                    marker=marker_map.get(tag_value, 'o'), linewidth=line_width, label=str(tag_value))
    else:
        ax.plot(df[x_col], df[value_col], color=colors[0] if isinstance(colors, list) else (colors if isinstance(colors, str) else None),
                linestyle=tag_line_styles, marker=marker, linewidth=line_width, label=value_col)

    # --- 坐标轴和图表元素设置 ---
    if xlim: ax.set_xlim(xlim)
    if ylim: ax.set_ylim(ylim)
    if yticks_num: ax.yaxis.set_major_locator(MaxNLocator(yticks_num))
    
    if x_labels:
        str_x_labels = {str(k): v for k, v in x_labels.items()}
        ax.set_xticks(range(len(x_categories)))
        ax.set_xticklabels([str_x_labels.get(cat, cat) for cat in x_categories], 
                             rotation=xticks_rotation, fontsize=xticks_fontsize)
    else:
        ax.tick_params(axis='x', rotation=xticks_rotation, labelsize=xticks_fontsize)

    ax.tick_params(axis='y', rotation=yticks_rotation, labelsize=yticks_fontsize)

    # 【修改】应用字体设置
    if xticks_fontfamily:
        for label in ax.get_xticklabels():
            label.set_fontfamily(xticks_fontfamily)
    if yticks_fontfamily:
        for label in ax.get_yticklabels():
            label.set_fontfamily(yticks_fontfamily)
    
    ax.set_title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)
    
    if grid: ax.grid(True, linestyle='--', alpha=0.6)
    
    # 【修改】根据 show_legend 参数决定是否显示图例
    if show_legend:
        legend_params = {
            'loc': legend_loc,
            'title': legend_title,
        }
        font_props = {}
        if legend_fontsize: font_props['size'] = legend_fontsize
        if legend_fontfamily: font_props['family'] = legend_fontfamily
        if font_props: legend_params['prop'] = font_props
        
        ax.legend(**{k: v for k, v in legend_params.items() if v is not None})

    # --- 保存图像 ---
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()

    return ["auto_plot", "line_chart_categorical", f"dpi_{dpi}"]