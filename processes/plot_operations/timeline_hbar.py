from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple, Callable
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba, ListedColormap
import matplotlib.cm as cm
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(input_type="single", output_ext=".pdf")
def plot_timeline_hbar(
    input_path: InputPath, 
    output_path: Path,
    # 核心时间线参数
    category_col: str,
    sub_category_col: str,
    start_col: str,
    end_col: str,
    hbar_height: float = 0.6,
    # 可选参数
    step_col: Optional[str] = None,
    label_col: Optional[str] = None,
    duration_col: Optional[str] = None,
    # 布局参数
    figsize: Tuple[int, int] = (10, 6),
    dpi: int = 300,
    title: Optional[str] = "Timeline Visualization",
    xlabel: str = "Time",
    ylabel: Optional[str] = None,
    ylim: tuple = None,
    grid: bool = True,
    # 颜色配置 - 修改为每个子类别自动分配唯一颜色
    color_map: Optional[Dict[str, str]] = None,
    category_colors: Optional[Dict[str, str]] = None,
    # Y轴配置
    category_order: Optional[List[str]] = None,
    category_labels: Optional[Dict[str, str]] = None,
    # 标签与格式
    min_label_duration_ratio: float = 0.01,
    max_label_length: int = 15,
    label_format: Callable[[pd.Series], str] = None,
    # 字体参数
    title_fontsize: int = 16,
    title_fontfamily: Optional[str] = None,
    xlabel_fontsize: int = 12,
    xlabel_fontfamily: Optional[str] = None,
    ylabel_fontsize: int = 12,
    ylabel_fontfamily: Optional[str] = None,
    tick_fontsize: int = 10,
    tick_fontfamily: Optional[str] = None,
    # 透明度
    axhline_aplha: float = 0.1,
    barh_alpha: float = 1.00,
    # 不需要的图例
    exclude_from_legend: Optional[List[str]] = None,
    # 其他杂项
    edge_color: str = "black",  # 事件块边框颜色
    legend_rename_map: Optional[Dict[str, str]] = None, # 重命名表
    **kwargs  # 捕获其他未使用的参数
) -> List[str]:
    """通用时间线绘图函数 - 每个子类别分配唯一颜色
    
    参数说明:
    - category_col: 类别列名 (Y轴分类)
    - sub_category_col: 子类别列名 (每个子类别自动分配唯一颜色)
    - start_col: 事件开始时间列名
    - end_col: 事件结束时间列名
    
    颜色配置:
    默认自动为每个子类别分配唯一的颜色
    """
    # 读取数据
    if ".csv" in str(input_path.path):
        df = pd.read_csv(input_path.path)
    elif ".parquet" in str(input_path.path):
        df = pd.read_parquet(input_path.path)
    
    # 验证数据列
    required_cols = [category_col, sub_category_col, start_col, end_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"数据文件中缺少必要列: {col}")
    
    # 计算持续时间（如果未提供）
    if duration_col and duration_col in df.columns:
        df["_duration"] = df[duration_col]
    else:
        df["_duration"] = df[end_col] - df[start_col]
    
    # 处理类别顺序和标签
    if category_order is None:
        # 根据数据频率排序
        category_counts = df[category_col].value_counts()
        category_order = category_counts.index.tolist()
    else:
        # 确保所有类别都在排序中
        unique_categories = set(df[category_col].unique())
        category_order = [c for c in category_order if c in unique_categories]
        category_order.extend(sorted(unique_categories - set(category_order)))
    
    # 创建Y轴位置映射
    category_positions = {cat: idx for idx, cat in enumerate(category_order)}
    
    # 处理类别标签
    if category_labels is None:
        category_labels = {cat: cat for cat in category_order}
    
    # === 修改颜色映射逻辑 ===
    # 1. 获取所有独特的子类别
    all_sub_categories = df[sub_category_col].unique().tolist()
    
    # 2. 创建子类别到颜色的映射
    # 使用viridis色彩图，支持无限数量的颜色
    sub_category_colors = {}
    viridis = cm.get_cmap('viridis', len(all_sub_categories))
    
    for idx, sub_cat in enumerate(all_sub_categories):
        sub_category_colors[sub_cat] = viridis(idx)  # 分配唯一颜色
    
    # 3. 如果提供了color_map，覆盖部分映射
    if color_map:
        for key, color in color_map.items():
            # 支持两种形式: 子类别名 或 (类别名, 子类别名)
            if isinstance(key, tuple) and key[1] in sub_category_colors:
                sub_category_colors[key[1]] = color
            elif key in sub_category_colors:
                sub_category_colors[key] = color
    
    # 4. 如果提供了category_colors，则按类别统一颜色
    if category_colors:
        for cat, color in category_colors.items():
            # 获取该类别下的所有子类别
            cat_sub_categories = df[df[category_col] == cat][sub_category_col].unique()
            for sub_cat in cat_sub_categories:
                sub_category_colors[sub_cat] = color
    # === 结束颜色修改 ===
    
    # 创建图表
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    
    if ylim is not None:
        ax.set_ylim(ylim)
    else:
        ax.set_ylim(-1, len(category_order) - 1)
    # 获取全局时间范围
    min_time = df[start_col].min()
    max_time = df[end_col].max()
    total_time = max_time - min_time
    
    # 为每个类别添加水平线
    for cat, ypos in category_positions.items():
        ax.axhline(y=ypos, color='gray', linestyle='-', alpha=axhline_aplha)
    
    # 绘制每个事件
    for _, row in df.iterrows():
        cat = row[category_col]
        sub_cat = row[sub_category_col]
        ypos = category_positions[cat]
        start_val = row[start_col]
        end_val = row[end_col]
        duration_val = row["_duration"]
        
        # 计算起始位置和长度（考虑时间单位不同）
        if min_time > 1000:  # 可能是毫秒级时间戳
            start_pos = (start_val - min_time) / 1000
            duration = (end_val - start_val) / 1000
        else:
            start_pos = start_val - min_time
            duration = duration_val
        
        # 获取颜色 - 直接使用子类别映射
        color = sub_category_colors[sub_cat]
        
        # 绘制时间块
        height = hbar_height
        ax.barh(
            y=ypos, 
            width=duration, 
            left=start_pos, 
            height=height, 
            color=color, 
            alpha=barh_alpha,
            edgecolor=edge_color
        )
        
        # 确定标签文本
        if label_format:
            label_text = label_format(row)
        elif label_col and label_col in row:
            label_text = str(row[label_col])
        else:
            # 默认标签为子类别
            label_text = sub_cat
        
        # 简短标签文本
        if len(label_text) > max_label_length:
            label_text = label_text[:max_label_length-3] + "..."
        
        # 添加标签（仅限持续时间足够长的事件）
        if duration > total_time * min_label_duration_ratio:
            center_x = start_pos + duration / 2
            
            # 确定文本颜色（基于背景色亮度）
            r, g, b, _ = to_rgba(color)
            brightness = 0.299 * r + 0.587 * g + 0.114 * b
            text_color = "white" if brightness < 0.6 else "black"
            
            ax.text(
                center_x, ypos + height/2,
                label_text,
                ha="center", va="center",
                fontsize=tick_fontsize,
                fontfamily=tick_fontfamily,
                color=text_color
            )
    
    # 设置Y轴
    y_ticks = list(category_positions.values())
    y_ticklabels = [category_labels.get(cat, cat) for cat in category_order]
    
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(
        y_ticklabels, 
        fontsize=ylabel_fontsize, 
        fontfamily=ylabel_fontfamily
    )
    
    # 设置X轴
    if min_time > 1000:  # 毫秒级时间戳
        ax.set_xlabel(f"{xlabel} (seconds from start)")
    else:
        ax.set_xlabel(f"{xlabel} (units from start)")
    
    if xlabel_fontsize:
        ax.xaxis.label.set_fontsize(xlabel_fontsize)
    if xlabel_fontfamily:
        ax.xaxis.label.set_fontfamily(xlabel_fontfamily)
    
    # 添加图例
    if len(sub_category_colors) < 20:  # 避免图例太多    
        legend_handles = []
        # 如果 exclude_from_legend 未设置，则视为空列表
        exclude_list = exclude_from_legend or []
        rename_map = legend_rename_map or {} # 如果未提供，视为空字典
        for sub_cat, color in sub_category_colors.items():
            # 如果子类别在排除列表中，则跳过，不为它创建图例
            if sub_cat in exclude_list:
                print(f"Skipping legend for excluded sub-category: {sub_cat}")
                continue
            display_label = rename_map.get(sub_cat, sub_cat)
            legend_handles.append(plt.Rectangle((0,0),1,1, fc=color, label=display_label))

        ax.legend(
            handles=legend_handles, 
            title="Sub Categories",
            bbox_to_anchor=(1.05, 1),
            loc='upper left',
            fontsize=max(8, tick_fontsize-2)
        )
    
    # 添加网格
    if grid:
        ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    # 设置标题
    if title:
        ax.set_title(
            title, 
            fontsize=title_fontsize, 
            fontfamily=title_fontfamily,
            pad=20
        )
    
    # 设置刻度标签字体
    if tick_fontsize:
        ax.tick_params(axis='both', labelsize=tick_fontsize)
    if tick_fontfamily:
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_family(tick_fontfamily)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图像
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    
    return ["generic_timeline", f"size_{figsize[0]}x{figsize[1]}", f"dpi_{dpi}"]