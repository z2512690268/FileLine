from pathlib import Path
from typing import Optional, Union
import pandas as pd
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry

@ProcessorRegistry.register(name="plot_loss_curve", input_type="single", output_ext=".pdf")
def plot_loss_curve(input_path: dict, output_path: Path,
                   figsize: tuple = (10, 6),
                   time_col: str = "time",
                   loss_col: str = "loss",
                   title: str = "Training Loss Curve",
                   xlabel: str = "Time (s)",
                   ylabel: str = "Loss",
                   line_color: str = "blue",
                   line_style: str = "-",
                   grid: bool = True,
                   dpi: int = 300,
                   xlim: tuple = None,
                   ylim: tuple = None,
                   xticks_num: int = None,
                   yticks_num: int = None,
                   xticks_rotation: float = 0,
                   yticks_rotation: float = 0,
                   # 新增字体参数
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
                   # 新增图例参数
                   legend_loc: Union[str, tuple] = "best",
                   legend_title: Optional[str] = None,
                   legend_fontsize: Optional[int] = None,
                   legend_fontfamily: Optional[str] = None,
                   legend_shadow: bool = False,
                   legend_frameon: bool = True,
                   legend_facecolor: Optional[str] = None,
                   legend_edgecolor: Optional[str] = None):
    """绘制Loss-Time曲线（支持字体和图例自定义）
    
    新增参数说明:
        title_fontsize/family: 标题字号和字体
        xlabel/ylabel_fontsize/family: 坐标轴标签字号字体
        xticks/yticks_fontsize/family: 刻度标签字号字体
        legend_loc: 图例位置（'best', 'upper right'等）
        legend_title: 图例标题
        legend_fontsize/family: 图例文字字号字体
        legend_shadow: 是否显示阴影
        legend_frameon: 是否显示边框
        legend_face/edgecolor: 图例背景/边框颜色
    """
    # 读取CSV数据
    df = pd.read_csv(input_path["path"])
    
    # 验证数据列存在
    for col in [time_col, loss_col]:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")
    
    # 创建画布
    plt.figure(figsize=figsize)
    
    # 绘制曲线
    plt.plot(df[time_col], df[loss_col],
             color=line_color,
             linestyle=line_style,
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