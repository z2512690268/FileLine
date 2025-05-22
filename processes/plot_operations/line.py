from pathlib import Path
from typing import Optional, Union, Dict
import pandas as pd
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_loss_curve(input_path: InputPath, output_path: Path,
                   figsize: tuple = (10, 6),
                   time_col: str = "time",
                   loss_col: str = "loss",
                   tag_col: Optional[str] = None,  # 新增分组列名参数
                   tag_colors: Union[str, Dict[str, str]] = "auto",  # 颜色字典或预设值
                   tag_line_styles: Union[str, Dict[str, str]] = "-",  # 线型字典或预设值
                   title: str = "Training Loss Curve",
                   xlabel: str = "Time (s)",
                   ylabel: str = "Loss",
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
    """绘制分组Loss-Time曲线
    
    新增参数说明:
        tag_col: 用于分组的列名（如不同实验组）
        tag_colors: 颜色配置（"auto"使用默认颜色循环，或传入{tag值:颜色}字典）
        tag_line_styles: 线型配置（统一线型或{tag值:线型}字典）
    """
    # 读取CSV数据
    df = pd.read_csv(input_path.path)
    
    # 验证必要列存在
    required_cols = [time_col, loss_col]
    if tag_col:
        required_cols.append(tag_col)
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")

    # 创建画布
    plt.figure(figsize=figsize)
    
    # 处理分组逻辑
    if tag_col:
        # 自动生成颜色映射
        if tag_colors == "auto":
            unique_tags = df[tag_col].unique()
            color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
            tag_colors = {tag: color_cycle[i % len(color_cycle)] 
                        for i, tag in enumerate(unique_tags)}
        # 统一颜色处理
        elif isinstance(tag_colors, str):
            tag_colors = {tag: tag_colors for tag in df[tag_col].unique()}
            
        # 统一线型处理
        if isinstance(tag_line_styles, str):
            tag_line_styles = {tag: tag_line_styles for tag in df[tag_col].unique()}

        # 分组绘制曲线
        for tag_value, group in df.groupby(tag_col):
            plt.plot(group[time_col], group[loss_col],
                    color=tag_colors.get(tag_value, None),
                    linestyle=tag_line_styles.get(tag_value, "-"),
                    label=f"{tag_value}")
    else:
        # 单线模式
        plt.plot(df[time_col], df[loss_col],
                 color=tag_colors if isinstance(tag_colors, str) else None,
                 linestyle=tag_line_styles,
                 label=f"{loss_col} curve")

    # 设置坐标轴范围
    if xlim is not None:
        plt.xlim(xlim[0], xlim[1])
    if ylim is not None:
        plt.ylim(ylim[0], ylim[1])
    
    # 设置刻度数量
    ax = plt.gca()
    if xticks_num is not None:
        ax.xaxis.set_major_locator(plt.MaxNLocator(xticks_num))
    if yticks_num is not None:
        ax.yaxis.set_major_locator(plt.MaxNLocator(yticks_num))
    
    # 设置刻度标签属性
    ax.tick_params(axis='x', which='major', 
                   labelsize=xticks_fontsize,
                   rotation=xticks_rotation)
    ax.tick_params(axis='y', which='major',
                   labelsize=yticks_fontsize,
                   rotation=yticks_rotation)
    
    # 设置字体家族（需要单独处理）
    if xticks_fontfamily:
        for label in ax.get_xticklabels():
            label.set_fontfamily(xticks_fontfamily)
    if yticks_fontfamily:
        for label in ax.get_yticklabels():
            label.set_fontfamily(yticks_fontfamily)

    # 添加图表元素
    plt.title(title, 
             fontsize=title_fontsize, 
             fontfamily=title_fontfamily)
    plt.xlabel(xlabel, 
              fontsize=xlabel_fontsize, 
              fontfamily=xlabel_fontfamily)
    plt.ylabel(ylabel, 
              fontsize=ylabel_fontsize, 
              fontfamily=ylabel_fontfamily)
    
    # 配置图例参数
    legend_params = {
        'loc': legend_loc,
        'title': legend_title,
        'shadow': legend_shadow,
        'frameon': legend_frameon,
        'facecolor': legend_facecolor,
        'edgecolor': legend_edgecolor
    }
    
    # 处理字体属性
    font_props = {}
    if legend_fontsize:
        font_props['size'] = legend_fontsize
    if legend_fontfamily:
        font_props['family'] = legend_fontfamily
    if font_props:
        legend_params['prop'] = font_props
    
    # 过滤空值并添加图例
    legend_params = {k: v for k, v in legend_params.items() if v is not None}
    plt.legend(**legend_params)
    
    if grid:
        plt.grid(True, linestyle='--', alpha=0.6)
    
    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    
    return ["auto_plot", "loss_curve", f"dpi_{dpi}"]