from pathlib import Path
from typing import Optional, Union, Dict, List
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
              legend_labels: Optional[Dict[str, str]] = None,  # 自定义图例标签
              legend_order: Optional[List[str]] = None,  # 新增：自定义图例顺序
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
              legend_edgecolor: Optional[str] = None,
              legend_bbox_to_anchor: Optional[tuple] = None,
              legend_ncol: int = 1,
              global_font_family: Optional[str] = None,
              show_restart_annotation: bool = True,
              annotation_text: str = "Resume",
              annotation_fontsize: int = 12,
              arrow_params: Optional[dict] = None,
              annotation_offset_ratio: float = 0.1,
              show_restart_vertical_line: bool = False,
              restart_vertical_line_params: Optional[dict] = None,
              show_restart_point: bool = False,
              restart_point_params: Optional[dict] = None):
    """绘制通用线图，支持分组和样式自定义
    
    Args:
        legend_labels: 自定义图例标签映射字典。对于分组模式，键为tag_col的原始值；
                      对于单线模式，键为value_col列名。值为要显示的自定义标签。
                      示例: {"model_a": "模型A (最优)", "train_loss": "训练损失"}
        legend_order: 自定义图例顺序列表。对于分组模式，列表元素为tag_col的原始值；
                      对于单线模式，可忽略此参数。图例会按照此列表顺序显示。
                      示例: ["model_b", "model_a"]  # 先显示model_b，再显示model_a
    """
    
    # 设置全局字体
    if global_font_family:
        plt.rcParams['font.family'] = global_font_family

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
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    
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
        
        # 处理图例顺序
        if legend_order:
            # 验证legend_order中的标签是否都存在
            valid_tags = []
            for tag in legend_order:
                if tag in unique_tags:
                    valid_tags.append(tag)
                else:
                    print(f"警告: legend_order中的标签 '{tag}' 在数据中不存在，已忽略")
            
            # 添加legend_order中未包含但数据中存在的标签
            remaining_tags = [tag for tag in unique_tags if tag not in valid_tags]
            final_order = valid_tags + remaining_tags
        else:
            # 无自定义顺序，使用原始顺序
            final_order = unique_tags
        
        # 分组绘制曲线 - 支持自定义标签和顺序
        lines = []  # 存储线条对象和标签
        for tag_value in final_order:
            group = df[df[tag_col] == tag_value]
            if group.empty:
                continue
                
            # 确定标签：优先使用自定义标签，否则使用原始值
            if legend_labels and tag_value in legend_labels:
                label = legend_labels[tag_value]
            elif legend_labels and str(tag_value) in legend_labels:
                label = legend_labels[str(tag_value)]
            else:
                label = f"{tag_value}"
            
            # 绘制线条并保存引用
            line = ax.plot(group[time_col], group[value_col],
                           color=tag_colors.get(tag_value, None),
                           linestyle=tag_line_styles.get(tag_value, "-"),
                           label=label)
            lines.append((line[0], label))
    else:
        # 单线模式 - 修复颜色处理，支持自定义标签
        color = tag_colors if (isinstance(tag_colors, str) and tag_colors != "auto") else None
        linestyle = tag_line_styles
        
        # 确定单线模式的标签
        if legend_labels and value_col in legend_labels:
            label = legend_labels[value_col]
        elif legend_labels and "single_line" in legend_labels:  # 备用键
            label = legend_labels["single_line"]
        else:
            label = f"{value_col} curve"
        
        # 绘制单线
        line = ax.plot(df[time_col], df[value_col],
                       color=color,
                       linestyle=linestyle,
                       label=label)
        lines = [(line[0], label)]

    # 坐标轴设置
    if xlim is not None:
        ax.set_xlim(xlim[0], xlim[1])
    if ylim is not None:
        ax.set_ylim(ylim[0], ylim[1])
    
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
    if title:
        ax.set_title(title, fontsize=title_fontsize, fontfamily=title_fontfamily)
    ax.set_xlabel(xlabel, fontsize=xlabel_fontsize, fontfamily=xlabel_fontfamily)
    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize, fontfamily=ylabel_fontfamily)
    
    # 配置图例 - 使用手动创建的图例以确保顺序
    if len(lines) > 0:
        # 提取线条对象和标签
        line_objects, labels = zip(*lines)
        
        legend_params = {
            'loc': legend_loc,
            'title': legend_title,
            'shadow': legend_shadow,
            'frameon': legend_frameon,
            'facecolor': legend_facecolor,
            'edgecolor': legend_edgecolor,
            'bbox_to_anchor': legend_bbox_to_anchor,
            'ncol': legend_ncol
        }
        
        font_props = {}
        if legend_fontsize:
            font_props['size'] = legend_fontsize
        if legend_fontfamily:
            font_props['family'] = legend_fontfamily
        if font_props:
            legend_params['prop'] = font_props
        
        # 创建手动图例
        ax.legend(line_objects, labels, 
                  **{k: v for k, v in legend_params.items() if v is not None})
    
    if grid:
        ax.grid(True, linestyle='--', alpha=0.6)

    # 绘制检查点恢复箭头
    if "restart_point" in df.columns:
        restart_points = df[df["restart_point"] == True]
        
        # 默认箭头样式
        default_arrow_props = dict(facecolor='black', shrink=0.05)
        if arrow_params:
            default_arrow_props.update(arrow_params)
            
        # 默认垂直线样式
        default_vline_props = dict(linestyle='--', color='gray', alpha=0.7)
        if restart_vertical_line_params:
            default_vline_props.update(restart_vertical_line_params)

        # 默认恢复点样式
        default_point_props = dict(color='black', marker='o', s=30, zorder=5)
        if restart_point_params:
            default_point_props.update(restart_point_params)

        # 捕获当前的Y轴范围
        current_ylim = ax.get_ylim()
        
        for _, row in restart_points.iterrows():
            # 绘制垂直线
            if show_restart_vertical_line:
                ax.vlines(x=row[time_col], ymin=current_ylim[0] - (current_ylim[1]-current_ylim[0]), ymax=row[value_col], **default_vline_props)
            
            # 绘制恢复点
            if show_restart_point:
                ax.scatter(row[time_col], row[value_col], **default_point_props)

            # 绘制注释
            if show_restart_annotation:
                ax.annotate(annotation_text, 
                             xy=(row[time_col], row[value_col]), 
                             xytext=(row[time_col], row[value_col] + (df[value_col].max() - df[value_col].min()) * annotation_offset_ratio),
                             arrowprops=default_arrow_props,
                             fontsize=annotation_fontsize,
                             horizontalalignment='center')
        
        # 恢复Y轴范围
        ax.set_ylim(current_ylim)
    
    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    
    return ["auto_plot", "line_chart", f"dpi_{dpi}"]