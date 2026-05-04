from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_line(input_path: InputPath, output_path: Path,
              figsize: tuple = (10, 6),
              time_col: str = "time",
              value_col: str = "loss",
              tag_col: Optional[str] = None,
              tag_colors: Union[str, Dict[str, str]] = None,
              tag_line_styles: Union[str, Dict[str, str]] = "-",
              legend_labels: Optional[Dict[str, str]] = None,
              legend_order: Optional[List[str]] = None,
              title: str = "Line Plot",
              xlabel: str = "Time (s)",
              ylabel: str = "Value",
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
              # 新的通用标注参数
              annotations: Optional[List[Dict]] = None):
    """绘制通用线图，支持分组和样式自定义
    
    Args:
        legend_labels: 自定义图例标签映射字典
        legend_order: 自定义图例顺序列表
        annotations: 通用标注配置列表，每个标注包含：
            - marker_col: 标识标注点的列名（布尔型）
            - text: 标注文本
            - fontsize: 文本字体大小，默认12
            - arrow_params: 箭头样式参数字典
            - vertical_line: 是否显示垂直线，默认False
            - vertical_line_params: 垂直线样式参数字典
            - point: 是否显示标注点，默认False
            - point_params: 标注点样式参数字典
            - offset_ratio: 文本偏移比例，默认0.1
            - show_tick: 是否在x轴上显示该标注点的刻度，默认False
            - tick_label: 自定义刻度标签，默认使用x轴值
            - tick_fontsize: 刻度标签字体大小
            - tick_rotation: 刻度标签旋转角度
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

    # 通用标注功能
    if annotations:
        # 捕获当前的Y轴范围
        current_ylim = ax.get_ylim()
        y_range = current_ylim[1] - current_ylim[0]
        
        # 收集需要显示刻度的x值
        tick_positions = []
        tick_labels = []
        
        for annotation_config in annotations:
            marker_col = annotation_config.get("marker_col")
            if not marker_col or marker_col not in df.columns:
                print(f"警告: 标注列 '{marker_col}' 不存在，跳过该标注")
                continue
            
            # 获取标注点
            marker_points = df[df[marker_col] == True]
            if marker_points.empty:
                continue
            
            # 默认配置
            default_arrow_props = dict(facecolor='black', shrink=0.05)
            default_vline_props = dict(linestyle='--', color='gray', alpha=0.7)
            default_point_props = dict(color='black', marker='o', s=30, zorder=5)
            
            # 合并用户配置
            arrow_params = {**default_arrow_props, **annotation_config.get("arrow_params", {})}
            vertical_line_params = {**default_vline_props, **annotation_config.get("vertical_line_params", {})}
            point_params = {**default_point_props, **annotation_config.get("point_params", {})}
            
            text = annotation_config.get("text", "标注")
            fontsize = annotation_config.get("fontsize", 12)
            offset_ratio = annotation_config.get("offset_ratio", 0.1)
            show_vertical_line = annotation_config.get("vertical_line", False)
            show_point = annotation_config.get("point", False)
            show_tick = annotation_config.get("show_tick", False)
            tick_label = annotation_config.get("tick_label")
            tick_fontsize = annotation_config.get("tick_fontsize", xticks_fontsize)
            tick_rotation = annotation_config.get("tick_rotation", xticks_rotation)
            
            for _, row in marker_points.iterrows():
                x_val = row[time_col]
                
                # 绘制垂直线
                if show_vertical_line:
                    ax.vlines(x=x_val, 
                             ymin=current_ylim[0], 
                             ymax=row[value_col], 
                             **vertical_line_params)
                
                # 绘制标注点
                if show_point:
                    ax.scatter(x_val, row[value_col], **point_params)

                # 绘制箭头和文本
                ax.annotate(text, 
                           xy=(x_val, row[value_col]), 
                           xytext=(x_val, row[value_col] + y_range * offset_ratio),
                           arrowprops=arrow_params,
                           fontsize=fontsize,
                           horizontalalignment='center')
                
                # 收集需要显示刻度的x值
                if show_tick:
                    tick_positions.append(x_val)
                    tick_labels.append(tick_label if tick_label else f"{x_val}")
        
        # 如果有需要显示的刻度，则添加到x轴
        if tick_positions:
            # 获取当前x轴刻度
            current_ticks = list(ax.get_xticks())
            current_labels = [tick.get_text() for tick in ax.get_xticklabels()]
            
            # 合并当前刻度和标注点刻度
            all_ticks = sorted(set(current_ticks + tick_positions))
            
            # 创建标签列表
            all_labels = []
            for tick in all_ticks:
                if tick in tick_positions:
                    idx = tick_positions.index(tick)
                    all_labels.append(tick_labels[idx])
                else:
                    # 保持原有标签
                    if tick in current_ticks:
                        idx = current_ticks.index(tick)
                        if idx < len(current_labels):
                            all_labels.append(current_labels[idx])
                        else:
                            all_labels.append(f"{tick}")
                    else:
                        all_labels.append(f"{tick}")
            
            # 设置x轴刻度
            ax.set_xticks(all_ticks)
            ax.set_xticklabels(all_labels, rotation=tick_rotation, fontsize=tick_fontsize)
        
        # 恢复Y轴范围
        ax.set_ylim(current_ylim)
    
    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    
    return ["auto_plot", "line_chart", f"dpi_{dpi}"]