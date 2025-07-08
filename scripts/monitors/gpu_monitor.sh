#!/bin/bash

# 检查nvidia-smi是否存在
if ! command -v nvidia-smi &> /dev/null; then
    echo "错误：nvidia-smi 未找到，请安装NVIDIA驱动后再运行此脚本"
    exit 1
fi

# 初始化参数变量
metric=""
target_pid=""

# 递归获取所有子进程PID函数
get_child_pids() {
    local parent_pid=$1
    echo "$parent_pid"
    for child_pid in $(pgrep -P "$parent_pid"); do
        get_child_pids "$child_pid"
    done
}

# 参数解析
while [[ $# -gt 0 ]]; do
    case "$1" in
        --metric)   metric="$2"; shift 2 ;;
        --pid)      target_pid="$2"; shift 2 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

# 获取GPU数量
gpu_count=$(nvidia-smi --query-gpu=count --format=csv,noheader | head -n 1)
if [[ -z "$gpu_count" ]] || ! [[ "$gpu_count" =~ ^[0-9]+$ ]]; then
    echo "错误：无法获取GPU数量"
    exit 1
fi

# 固定状态和时间戳
timestamp=$(date +"%T")
status="OK"

# 指标收集逻辑
case "$metric" in
    gpu_util)
        # 遍历每个GPU输出利用率
        for ((gpu_id=0; gpu_id<gpu_count; gpu_id++)); do
            value=$(nvidia-smi --id=$gpu_id --query-gpu=utilization.gpu --format=csv,noheader,nounits | awk '{print $1}')
            formatted_value=$(printf "%.2f" "$value")
            echo "[$timestamp][$status] GPU$gpu_id $metric usage: ${formatted_value}%"
        done
        ;;
        
    gpu_mem)
        if [[ -n "$target_pid" ]]; then
            # 进程模式：获取PID及所有子进程
            pids=($(get_child_pids "$target_pid" | sort -u))
            
            # 遍历每个GPU
            for ((gpu_id=0; gpu_id<gpu_count; gpu_id++)); do
                total_mem=$(nvidia-smi --id=$gpu_id --query-gpu=memory.total --format=csv,noheader,nounits | awk '{print $1}')
                
                # 获取该GPU上目标进程的显存使用
                gpu_proc_info=()
                while IFS=, read -r pid mem_usage; do
                    if [[ " ${pids[*]} " =~ " $pid " ]]; then
                        gpu_proc_info+=("PID $pid: ${mem_usage}MB")
                    fi
                done < <(nvidia-smi --id=$gpu_id --query-compute-apps=pid,used_memory --format=csv,noheader,nounits 2>/dev/null)

                # 计算进程组在该GPU上的总显存使用
                sum_mem=0
                if (( ${#gpu_proc_info[@]} > 0 )); then
                    # 从gpu_proc_info中提取内存数值并求和
                    for proc_entry in "${gpu_proc_info[@]}"; do
                        mem_value=$(echo "$proc_entry" | awk '{print $NF}' | sed 's/MB//')
                        sum_mem=$(echo "$sum_mem + $mem_value" | bc)
                    done
                fi
                
                # 计算显存占比
                if [[ -z "$total_mem" ]] || [[ "$total_mem" -eq 0 ]]; then
                    percentage=0
                else
                    percentage=$(echo "scale=2; 100 * $sum_mem / $total_mem" | bc)
                fi
                
                # 输出GPU信息
                echo "[$timestamp][$status] GPU$gpu_id $metric usage: ${percentage}% (Total: ${total_mem}MB, Used: ${sum_mem}MB)"
                
                # 输出每个进程的信息
                for proc_info in "${gpu_proc_info[@]}"; do
                    echo "    └─ $proc_info"
                done
                
                # 如果没有相关进程
                if (( ${#gpu_proc_info[@]} == 0 )); then
                    echo "    └─ No processes found"
                fi
            done
        else
            # 整机模式：直接输出每个GPU的信息
            for ((gpu_id=0; gpu_id<gpu_count; gpu_id++)); do
                used_mem=$(nvidia-smi --id=$gpu_id --query-gpu=memory.used --format=csv,noheader,nounits | awk '{print $1}')
                total_mem=$(nvidia-smi --id=$gpu_id --query-gpu=memory.total --format=csv,noheader,nounits | awk '{print $1}')
                
                if [[ -z "$total_mem" ]] || [[ "$total_mem" -eq 0 ]]; then
                    percentage=0
                else
                    percentage=$(echo "scale=2; 100 * $used_mem / $total_mem" | bc)
                fi
                
                echo "[$timestamp][$status] GPU$gpu_id $metric usage: ${percentage}% (Total: ${total_mem}MB, Used: ${used_mem}MB)"
            done
        fi
        ;;
        
    *) echo "未知指标: $metric"; exit 2 ;;
esac


# 输出---------------------------------------------------------
echo "----------------------------------------------------------"


# 使用示例:
# 监控整机GPU利用率: ./gpu_monitor.sh --metric gpu_util
# 监控进程显存使用:   ./gpu_monitor.sh --metric gpu_mem --pid 1234