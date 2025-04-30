from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from core.processing import ProcessorRegistry

@ProcessorRegistry.register(name="plot_loss_curve", input_type="single", output_ext=".pdf")
def plot_loss_curve(input_path: dict, output_path: Path, 
                   time_col: str = "time", 
                   loss_col: str = "loss",
                   title: str = "Training Loss Curve",
                   xlabel: str = "Time (s)",
                   ylabel: str = "Loss",
                   line_color: str = "blue",
                   line_style: str = "-",
                   grid: bool = True,
                   dpi: int = 300):
    """绘制Loss-Time曲线
    
    Args:
        time_col: 时间列名 (默认'time')
        loss_col: Loss列名 (默认'loss')
        title: 图表标题 (默认'Training Loss Curve')
        xlabel: X轴标签 (默认'Time (s)')
        ylabel: Y轴标签 (默认'Loss')
        line_color: 线条颜色 (默认'blue')
        line_style: 线条样式 (默认'-')
        grid: 是否显示网格 (默认True)
        dpi: 输出分辨率 (默认300)
    """
    # 读取CSV数据
    df = pd.read_csv(input_path["path"])
    
    # 验证数据列存在
    for col in [time_col, loss_col]:
        if col not in df.columns:
            raise ValueError(f"CSV文件中缺少必要列: {col}")
    
    # 创建画布
    plt.figure(figsize=(10, 6))
    
    # 绘制曲线
    plt.plot(df[time_col], df[loss_col], 
            color=line_color, 
            linestyle=line_style,
            label=f"{loss_col} curve")
    
    # 添加图表元素
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend()
    
    if grid:
        plt.grid(True, linestyle='--', alpha=0.6)
    
    # 保存图像
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    
    # 返回自动生成的标签
    return ["auto_plot", "loss_curve", f"dpi_{dpi}"]