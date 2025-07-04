from pathlib import Path
from typing import Optional, Union, Dict
import pandas as pd
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_line(input_path: InputPath, output_path: Path,
              figsize: tuple = (10, 6),
              time_col: str = "time",
              value_col: str = "loss",  # 更通用的Y轴列名
              tag_col: Optional[str] = None,
              tag_colors: Union[str, Dict[str, str]] = None,  # 默认改为None
              tag_line_styles: Union[str, Dict[str, str]] = "-",
              title: str = "Line Plot",  # 更通用的标题
              xlabel: str = "Time (s)",
              ylabel: str = "Value",   # 更通用的Y轴标签
              grid: bool = True,
              dpi: int = 300,
              xlim: tuple = None,
              ylim: tuple = None,
              xticks_num: int = None,
              yticks_num: int = None,
              xticks_rotation: float = 0,
              yticks_rotation: float = 0,
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
              legend_loc: Union[str, tuple] = "best",
              legend_title: Optional[str] = None,
              legend_fontsize: Optional[int] = None,
              legend_fontfamily: Optional[str] = None,
              legend_shadow: bool = False,
              legend_frameon: bool = True,
              legend_facecolor: Optional[str] = None,
              legend_edgecolor: Optional[str] = None):
    """绘制通用线图，支持分组和样式自定义"""
    
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    
    # 验证必要列存在
    required_cols = [time_col, value_col]
    if tag_col:
        required_cols.append(tag_col)
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"文件中缺少必要列: {col}")

    # 创建画布
    plt.figure(figsize=figsize)
    
    # 处理分组逻辑
    if tag_col:
        unique_tags = df[tag_col].unique()
        
        # 自动生成颜色映射
        if tag_colors == "auto" or tag_colors is None:
            color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
            tag_colors = {tag: color_cycle[i % len(color_cycle)] 
                          for i, tag in enumerate(unique_tags)}
        elif isinstance(tag_colors, str):
            tag_colors = {tag: tag_colors for tag in unique_tags}
        # 检查字典完整性
        if isinstance(tag_colors, dict):
            tag_colors = {tag: tag_colors.get(tag, None) for tag in unique_tags}
            
        # 统一线型处理
        if isinstance(tag_line_styles, str):
            tag_line_styles = {tag: tag_line_styles for tag in unique_tags}
        elif isinstance(tag_line_styles, dict):
            tag_line_styles = {tag: tag_line_styles.get(tag, "-") for tag in unique_tags}
        
        # 分组绘制曲线
        for tag_value, group in df.groupby(tag_col):
            plt.plot(group[time_col], group[value_col],
                     color=tag_colors.get(tag_value, None),
                     linestyle=tag_line_styles.get(tag_value, "-"),
                     label=f"{tag_value}")
    else:
        # 单线模式 - 修复颜色处理
        color = tag_colors if (isinstance(tag_colors, str) and tag_colors != "auto") else None
        linestyle = tag_line_styles
        
        plt.plot(df[time_col], df[value_col],
                 color=color,
                 linestyle=linestyle,
                 label=f"{value_col} curve")

    # 坐标轴设置 [保持与原代码相同的配置逻辑]
    if xlim is not None:
        plt.xlim(xlim[0], xlim[1])
    if ylim is not None:
        plt.ylim(ylim[0], ylim[1])
    
    ax = plt.gca()
    if xticks_num is not None:
        ax.xaxis.set_major_locator(plt.MaxNLocator(xticks_num))
    if yticks_num is not None:
        ax.yaxis.set_major_locator(plt.MaxNLocator(yticks_num))
    
    ax.tick_params(axis='x', 
                   labelsize=xticks_fontsize,
                   rotation=xticks_rotation)
    ax.tick_params(axis='y',
                   labelsize=yticks_fontsize,
                   rotation=yticks_rotation)
    
    if xticks_fontfamily:
        for label in ax.get_xticklabels():
            label.set_fontfamily(xticks_fontfamily)
    if yticks_fontfamily:
        for label in ax.get_yticklabels():
            label.set_fontfamily(yticks_fontfamily)

    # 添加图表元素
    plt.title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    plt.xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    plt.ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)
    
    # 配置图例
    legend_params = {
        'loc': legend_loc,
        'title': legend_title,
        'shadow': legend_shadow,
        'frameon': legend_frameon,
        'facecolor': legend_facecolor,
        'edgecolor': legend_edgecolor
    }
    
    font_props = {}
    if legend_fontsize:
        font_props['size'] = legend_fontsize
    if legend_fontfamily:
        font_props['family'] = legend_fontfamily
    if font_props:
        legend_params['prop'] = font_props
    
    plt.legend(**{k: v for k, v in legend_params.items() if v is not None})
    
    if grid:
        plt.grid(True, linestyle='--', alpha=0.6)
    
    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    
    return ["auto_plot", "line_chart", f"dpi_{dpi}"]