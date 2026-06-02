# ZeroCkpt 流水线文档

本文档描述了 `pipelines/zerockpt/` 目录下所有YAML流水线配置文件的功能和组件流程。


运行：指定全局文件zerockpt.global
```bash
(llama) [root@localhost expr_manager]# python main.py pipeline run --global-config ./pipelines/zerockpt/zerockpt.global ./pipelines/zerockpt/multi_card.yaml
```

## 📊 YAML文件与输出文件对应关系

| YAML文件 | 输出文件 | 功能描述 |
|----------|----------|----------|
| [`zerockpt/accuracy_line.yaml`](zerockpt/accuracy_line.yaml) | `loss_curve.pdf` | 训练损失曲线图 |
| [`zerockpt/breakdown_grad.yaml`](zerockpt/breakdown_grad.yaml) | `breakdown_grad.pdf` | **Grad方案时间线分解图** |
| [`zerockpt/breakdown_halfzero.yaml`](zerockpt/breakdown_halfzero.yaml) | `breakdown_halfzero.pdf` | **Half-zero方案时间线分解图** |
| [`zerockpt/cpu_time.yaml`](zerockpt/cpu_time.yaml) | `plot_cpu.pdf` | CPU使用率时间序列图 |
| [`zerockpt/crash_expr_bar.yaml`](zerockpt/crash_expr_bar.yaml) | `crash_test.pdf` | 崩溃实验吞吐量柱状图 |
| [`zerockpt/dual_axis_line.yaml`](zerockpt/dual_axis_line.yaml) | `dual_axis_cpu_gpu_grad.pdf` | **CPU与GPU双轴线图** |
| [`zerockpt/gpu_use.yaml`](zerockpt/gpu_use.yaml) | `plot_gpu_tx.pdf` | GPU使用率时间序列图 |
| [`zerockpt/mem_use.yaml`](zerockpt/mem_use.yaml) | `plot_mem.pdf` | 内存使用率时间序列图 |
| [`zerockpt/multi_card.yaml`](zerockpt/multi_card.yaml) | `multi_card_test.pdf` | **多卡实验吞吐量对比** |
| [`zerockpt/stalltime_groupbar_1x3.yaml`](zerockpt/stalltime_groupbar_1x3.yaml) | `stalltime_1x3.pdf` | 1x3布局停滞时间对比 |
| [`zerockpt/stalltime_groupbar_eg1x2.yaml`](zerockpt/stalltime_groupbar_eg1x2.yaml) | `stalltime_eg1.pdf` | 1x2布局停滞时间示例 |
| [`zerockpt/stalltime_groupbar_eg2.yaml`](zerockpt/stalltime_groupbar_eg2.yaml) | `stalltime_eg2.pdf` | 停滞时间对比示例2 |
| [`zerockpt/stalltime_groupbar_eg2_corr.yaml`](zerockpt/stalltime_groupbar_eg2_corr.yaml) | `stalltime_eg2.pdf` | **校正后停滞时间对比** |
| [`zerockpt/stalltime_statistics.yaml`](zerockpt/stalltime_statistics.yaml) | `full_stall.parquet`, `max_stall_by_file.parquet` | 停滞时间统计数据 |
| [`zerockpt/thread_test.yaml`](zerockpt/thread_test.yaml) | `update_time.pdf` | **线程数量与更新时间关系** |
| [`zerockpt/throughput_groupbar_1x3.yaml`](zerockpt/throughput_groupbar_1x3.yaml) | `test8.pdf` | 1x3布局吞吐量对比 |
| [`zerockpt/throughput_groupbar_1x3 copy.yaml`](zerockpt/throughput_groupbar_1x3%20copy.yaml) | `test7.pdf` | 归一化吞吐量对比 |
| [`zerockpt/throughput_groupbar_eg1x2.yaml`](zerockpt/throughput_groupbar_eg1x2.yaml) | `throughput_eg1.pdf` | 1x2布局吞吐量示例 |
| [`zerockpt/throughput_groupbar_eg2.yaml`](zerockpt/throughput_groupbar_eg2.yaml) | `throughput_eg2-200.pdf` | 吞吐量对比示例2 |
| [`zerockpt/throughput_groupbar_eg3.yaml`](zerockpt/throughput_groupbar_eg3.yaml) | `throughput_eg3.pdf` | **三模型吞吐量对比** |

## 🔄 流水线组件流程

### 1. 准确率与损失分析流水线

#### [`accuracy_line.yaml`](zerockpt/accuracy_line.yaml) - 训练损失曲线
```
parse_multi_runtime → filter_accuracy_line → plot_line
```
- 解析多个运行日志 → 过滤准确率数据 → 绘制线图

#### [`loss_batch_seq.yaml`](zerockpt/loss_batch_seq.yaml) - 批次序列损失对比
```
parse_multi_runtime → filter_runtime → plot_grouped_bar
```
- 解析运行时数据 → 过滤损失数据 → 绘制分组柱状图

#### [`loss_token_batch.yaml`](zerockpt/loss_token_batch.yaml) - Token批次损失对比
```
parse_multi_runtime → filter_runtime → plot_grouped_bar
```
- 解析运行时数据 → 过滤吞吐量数据 → 绘制分组柱状图

### 2. 系统资源监控流水线

#### `cpu_time.yaml` - CPU使用率监控
```
parse_cpu → plot_line
```
- 解析CPU监控数据 → 绘制时间序列线图

#### `gpu_use.yaml` - GPU使用率监控
```
parse_gpu → plot_line
```
- 解析GPU监控数据 → 绘制时间序列线图

#### `mem_use.yaml` - 内存使用率监控
```
parse_mem → plot_line
```
- 解析内存监控数据 → 绘制时间序列线图

#### `dual_axis_line.yaml` - CPU与GPU双轴对比
```
parse_cpu → ┐
            ├→ merge_tables → plot_dual_axis_line
parse_gpu → ┘
```
- 分别解析CPU和GPU数据 → 按时间合并 → 绘制双Y轴线图

### 3. 时间线分解分析流水线

#### `breakdown_grad.yaml` - Grad方案时间线分解
```
parse_multi_stalltime_with_timestamp → filter_single_save → rearrange_single_save → filter_by_condition → plot_timeline_hbar
```
- 解析带时间戳的停滞数据 → 过滤单次保存过程 → 重排为时间线格式 → 过滤背景数据 → 绘制时间线图

#### `breakdown_halfzero.yaml` - Half-zero方案时间线分解
```
parse_multi_stalltime_with_timestamp → filter_single_save → rearrange_single_save_halfzero → filter_by_condition → plot_timeline_hbar
```
- 解析带时间戳的停滞数据 → 过滤单次保存过程 → 重排为时间线格式(为Halfzero单独写了一个版本) → 过滤背景数据 → 绘制时间线图

### 4. 停滞时间分析流水线

#### `stalltime_group_bar.yaml` - 基础停滞时间对比
```
parse_multi_stalltime → plot_grouped_bar
```
- 解析多文件停滞时间 → 绘制分组柱状图

#### `stalltime_groupbar_eg2_corr.yaml` - 校正后停滞时间对比
```
parse_multi_stalltime_with_timestamp → adjust_datastates_stall_duration → filter_table → plot_grouped_bar
```
- 解析停滞数据 → 校正数据状态LLM停滞时间 → 过滤数据 → 绘制分组柱状图

#### `stalltime_statistics.yaml` - 停滞时间统计
```
parse_multi_stalltime_with_timestamp → filter_table → groupby_table
```
- 解析停滞数据 → 过滤Full Stall记录 → 按文件名分组聚合

### 5. 吞吐量分析流水线

#### `throughput_std_batch_seq.yaml` - 标准吞吐量对比
```
parse_multi_runtime → filter_runtime → plot_grouped_bar
```
- 解析运行时数据 → 过滤标准吞吐量数据 → 绘制分组柱状图

#### `throughput_groupbar_eg3.yaml` - 三模型吞吐量对比
```
parse_multi_stalltime_with_timestamp → filter_table → filter_by_condition → plot_grouped_bar
```
- 解析停滞数据 → 表格过滤 → 条件过滤 → 绘制分组柱状图

#### `multi_card.yaml` - 多卡实验吞吐量
```
parse_multi_stalltime_with_timestamp → filter_table → filter_by_condition → plot_grouped_bar
```
- 解析停滞数据 → 过滤多卡数据 → 条件过滤 → 绘制分组柱状图

### 6. 崩溃实验分析流水线

#### `crash_expr_bar.yaml` - 崩溃实验柱状图
```
parse_multi_stalltime_with_timestamp → filter_by_condition → update_universal → plot_bar
```
- 解析停滞数据 → 条件过滤 → 更新数据格式 → 绘制柱状图

#### `crash_expr_groupbar.yaml` - 崩溃实验分组图
```
parse_multi_stalltime_with_timestamp → filter_by_condition → plot_grouped_bar
```
- 解析停滞数据 → 条件过滤 → 绘制分组柱状图

### 7. 特殊分析流水线

#### `draw_grad_overhead.yaml` - 梯度开销曲线
```
generate_gockpt_overhead → plot_loss_curve
```
- 生成GoCkpt开销数据 → 绘制损失曲线

#### `thread_test.yaml` - 线程性能测试
```
parse_full_stall_to_callback → groupby_table → type_cast → plot_line_categorical
```
- 解析Full Stall到Callback间隔 → 按线程数分组 → 类型转换 → 绘制分类线图

## 🔧 常用组件说明

### 解析组件
- `parse_multi_stalltime_with_timestamp`: 解析带时间戳的多文件停滞时间数据
- `parse_multi_runtime`: 解析多文件运行时性能数据（**已废弃**）
- `parse_cpu/gpu/mem`: 解析系统资源监控数据

### 过滤组件
- `filter_by_condition`: 通用条件过滤器（**和filter_table类似只不过是直接传递pandas查询语句**）
- `filter_table`: 表格数据过滤器
- `filter_single_save`: 单次保存过程数据提取
- `filter_runtime`: 运行时数据过滤器

### 处理组件
- `rearrange_single_save`: 数据重排为时间线格式
- `adjust_datastates_stall_duration`: 校正Datastates_LLM stall_time 不准的问题
- `update_universal`: 通用数据更新器
- `merge_tables`: 表格合并器

### 绘图组件
- `plot_grouped_bar`: 分组柱状图（最常用）
- `plot_line`: 线图绘制
- `plot_timeline_hbar`: 时间线水平柱状图
- `plot_dual_axis_line`: 双Y轴线图
- `plot_line_categorical`: 分类线图
