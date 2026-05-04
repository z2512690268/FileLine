# GradCkpt Figure 7 画图经验总结

## 1. 数据与路径

### 数据来源
- 远程服务器：`instance-ck71enw2.yc.smartml.cn:11014`，路径 `/home/zky/ZeroCkpt/scripts/data/`
- 本地拷贝路径：`/home/keyao/cloudcli/FileLine/data/const_freq/`
- 共 48 个 `.log` 文件，覆盖 Llama3.2-1B 模型

### 文件命名规则
```
{model_name}-{ckpt_type}-{total_steps}-{ckpt_freq}-{batch_size}-{max_seqlen}-True.log
例：Llama1B-grad-2000-50-1-3072-True.log
```

## 2. Pipeline 画图框架

### 2.1 核心架构

```
┌─────────────┐    ┌──────────────┐    ┌───────────┐    ┌─────────┐
│ initial_load │ → │ parse_*      │ → │ filter_*  │ → │ plot_*  │ → final_output
│ (加载日志)    │    │ (解析数据)    │    │ (过滤数据)  │    │ (画图)    │    (导出PDF)
└─────────────┘    └──────────────┘    └───────────┘    └─────────┘
         ↑               ↑                  ↑               ↑
    raw log files    .parquet 中间格式    .parquet        .pdf
```

### 2.2 运行方式

```bash
# 1. 创建实验
python main.py experiment create <exp_name>

# 2. 运行流水线（带全局变量）
python main.py --experiment <exp_name> pipeline run \
  --global-config ./FileLine-Pipelines/zerockpt/zerockpt.global \
  ./FileLine-Pipelines/zerockpt_local/<config>.yaml
```

### 2.3 全局变量（zerockpt.global）

全局配置文件定义了颜色、标签等变量，YAML 中通过 `${VAR_NAME}` 引用：

```ini
GRAD_COLOR = "#4956A2"
HALF_ZERO_COLOR = "#D91A26"
GRAD = 'GoCkpt'
HALF_ZERO = 'GoCkpt-O'
...
```

### 2.4 YAML 配置文件结构

```yaml
initial_load:
  include:
    - path: "/path/to/data/*.log"    # 数据路径（支持 glob）
  type: raw
  global_tags: [origin]

steps:
  - processor: parser_name           # 步骤1：解析
    inputs: initial
    output: parsed

  - processor: filter_name           # 步骤2：过滤
    inputs: parsed
    output: filtered
    params:
      conditions: {...}

  - processor: plot_name             # 步骤3：绘图
    inputs: filtered
    output: figure_name
    params: {...}

final_output:
  - name: figure_name
    export: output.pdf               # 导出文件名
```

## 3. 关键 Processor

### 3.1 数据解析

| Processor | 输入 | 输出 | 说明 |
|-----------|------|------|------|
| `parse_multi_stalltime_with_timestamp` | 多个 .log 文件 | .parquet | **核心解析器**，从日志中提取 Step、Full Stall、Real Stall 等所有记录 |

**解析的字段**（共 24 列）：
- **元数据**：`model_name`, `ckpt_type`, `total_steps`, `ckpt_freq`, `batch_size`, `file_name`
- **Step 记录**：`step`, `total_time`, `forward_time`, `backward_time`, `update_time`, `loss`, `tokens_per_sec`
- **Stall 记录**：`stall_duration`, `stall_start`, `stall_end`
- **其他**：`record_type` (Step/Full Stall/Real Stall/LPT/HPT/Persist Callback/AdamW Callback)

### 3.2 数据过滤

| Processor | 说明 |
|-----------|------|
| `filter_table` | **条件过滤**，支持 `conditions: {col: in [values]}` 语法 |
| `filter_by_condition` | **表达式过滤**，支持 pandas query 语法如 `"step > total_steps - 8"` |
| `adjust_datastates_stall_duration` | **修正 Datastates-LLM 的 stall 时间**（计算 next step 时间差） |

### 3.3 绘图

| Processor | 输出类型 | 适用场景 |
|-----------|----------|----------|
| `plot_grouped_bar` | 分组柱状图 | **最常用**，支持 4 维分组 (main_group, sub_group, row, col) |
| `plot_line` | 线图 | 时间序列 |
| `plot_timeline_hbar` | 水平时间线 | 单次保存时间线分解 |
| `plot_dual_axis_line` | 双轴图 | CPU/GPU 同时显示 |

## 4. plot_grouped_bar 核心参数

### 4.1 数据分组

| 参数 | 说明 |
|------|------|
| `main_group_col` | X 轴主分组 |
| `sub_group_col` | 每个主分组内的子分组（不同颜色的柱子） |
| `col_col` | 列方向分面子图（类似 facet） |
| `row_col` | 行方向分面子图 |
| `value_col` | 柱子的数值列 |

### 4.2 排序与标签

| 参数 | 说明 |
|------|------|
| `main_group_order` | 主分组的显示顺序 |
| `sub_group_order` | 子分组（柱子）的显示顺序 |
| `main_group_labels` | 主分组自定义标签（dict） |
| `sub_group_labels` | 子分组自定义标签（dict） |
| `subplot_titles` | 子图标题（dict，key 为 col 值） |

### 4.3 样式控制

| 参数 | 说明 |
|------|------|
| `colors` | 颜色映射 (dict) |
| `figsize` | 图像大小 `[width, height]` |
| `bar_width` | 柱子宽度 (默认 0.2) |
| `main_group_gap` | 主分组间距 |
| `sub_group_gap` | 子分组间距 |
| `ylim` | Y 轴范围（可统一设置或按子图 dict 设置） |
| `show_bar_values` | 是否在柱子上方显示数值 |
| `aggregate_func` | 聚合函数 (`max`, `mean`) |
| `show_main_group_separators` | 主分组间虚线分隔 |

### 4.4 图例

| 参数 | 说明 |
|------|------|
| `legend_loc` | 图例位置 |
| `legend_bbox_to_anchor` | 图例偏移 `[x, y]` |
| `legend_ncol` | 图例列数 |
| `legend_frameon` | 图例边框 |
| `subplot_title_y` | 子图标题 Y 偏移（负数=放到下方） |
| `figure_top_margin` | 顶部留白（给图例留空间） |

## 5. 图7 的具体实现

### 5.1 Throughput 对比图 (`fig7_throughput.yaml`)

**数据流程**：
```
48个 const_freq/*.log 文件
  → parse_multi_stalltime_with_timestamp (解析为 parquet)
  → filter_table (ckpt_freq in [50, 200])
  → filter_by_condition (step > total_steps - 8, 取稳态数据)
  → plot_grouped_bar (main=ckpt_freq, sub=ckpt_type, col=model_name)
  → throughput_fig7.pdf
```

**关键配置**：
- 主分组：`ckpt_freq` (50, 200) — 对比低频/高频
- 子分组：`ckpt_type` — 11 种检查点引擎
- 数值列：`tokens_per_sec` — 吞吐量
- 聚合：`max` — 取最大吞吐量

**数据洞察**：
- GoCkpt-O (half_zero) 吞吐量最高（11190 tok/s @ freq=200），接近 Ideal (11208 tok/s)
- GoCkpt (grad) 紧随其后（11151 tok/s @ freq=200）
- 同步 checkpointers (deepspeed, DCP, torch_snapshot) 吞吐量衰减严重
- 低频 (freq=50) 时同步方法吞吐量仅 ~3000 tok/s，约为异步方法的 1/3

### 5.2 Stall Time 对比图 (`fig7_stalltime.yaml`)

**数据流程**：
```
48个 const_freq/*.log 文件
  → parse_multi_stalltime_with_timestamp
  → adjust_datastates_stall_duration (修正 Datastates-LLM stall 时间)
  → filter_table (Full Stall + ckpt_freq=50 + 3种 ckpt_type)
  → plot_grouped_bar (main=model_name, sub=ckpt_type)
  → stalltime_fig7.pdf
```

**关键配置**：
- 过滤：`record_type: "Full Stall"`, `ckpt_freq: 50`, 仅 3 种异步方法
- 排序：Async-O → GoCkpt → GoCkpt-O

**数据洞察**：
- GoCkpt-O: 0.328s — 最低的 stall 时间
- GoCkpt: 0.749s — 约是 GoCkpt-O 的 2.3 倍
- Async-O (cpp_overlap): 1.129s — 约是 GoCkpt-O 的 3.4 倍

## 6. 注意事项

### 6.1 数据兼容性
- `py_async` (Async) 使用 `Persist Stall time` 而非 `Full Stall time`，当前 parser 不支持该格式
- `adjust_datastates_stall_duration` 修正了 Datastates-LLM 的 stall 时间计算偏差

### 6.2 路径修改
原始 YAML 中的数据路径为 `/root/keyao/Checkpoints/ZeroCkpt/ZeroCkpt2/scripts/data/const_freq/`，需要修改为实际数据路径后重新运行。

### 6.3 实验管理
- 每个实验有独立的 SQLite 数据库存储中间结果
- 同一路径的文件修改时间不变时会使用缓存
- 使用 `--debug` 标志可查看详细执行过程

### 6.4 输出位置
- 中间结果（parquet/pdf）：`experiments/<exp_name>/processed/`
- 最终导出：`experiments/<exp_name>/exports/<filename>.pdf`
