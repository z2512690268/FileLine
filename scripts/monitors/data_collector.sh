#!/bin/bash
# 支持进程级资源监控的采集脚本（改进CPU瞬时利用率计算）
# 参数：--metric [cpu|mem] --target <进程名> (可选)

# 初始化变量
metric=""
target=""
pids=""

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case "$1" in
        --metric)   metric="$2";   shift 2 ;;
        --target)   target="$2";   shift 2 ;;
        *)          echo "未知参数: $1"; exit 1 ;;
    esac
done

# 获取CPU核心数
cores=$(nproc)

# 资源收集逻辑
case $metric in
    cpu)
        if [[ -n "$target" ]]; then
            # 获取目标进程及其子进程的PID列表
            pids=$(pstree -T -p "$target" 2>/dev/null | grep -oP '\d+(?=\))' | sort -u | tr '\n' ',' | sed 's/,$//')
            # pids=($target)
            # echo $pids
            if [[ -z "$pids" ]]; then
                value=0
            else
                # 改进：通过采样计算瞬时CPU利用率
                pid_array=(${pids//,/ })
                start_time=$(date +%s.%N)  # 精确开始时间
                declare -A cpu_time1
                
                # 第一次采样：记录各进程的CPU时间和状态
                for pid in "${pid_array[@]}"; do
                    if [ -f "/proc/$pid/stat" ]; then
                        read -r -a stat_array < "/proc/$pid/stat"
                        utime=${stat_array[13]}
                        stime=${stat_array[14]}
                        cpu_time1[$pid]=$((utime + stime))
                    else
                        cpu_time1[$pid]=""
                    fi
                done
                
                sleep 0.3  # 采样间隔(0.3秒平衡精度和延迟)
                
                # 第二次采样
                end_time=$(date +%s.%N)  # 精确结束时间
                total_cpu_ticks=0
                valid_count=0
                
                for pid in "${pid_array[@]}"; do
                    if [ -f "/proc/$pid/stat" ]; then
                        read -r -a stat_array < "/proc/$pid/stat"
                        utime=${stat_array[13]}
                        stime=${stat_array[14]}
                        cpu_time2=$((utime + stime))
                        
                        # 只计算两次采样均存在的进程
                        if [ -n "${cpu_time1[$pid]}" ]; then
                            tick_diff=$(( cpu_time2 - ${cpu_time1[$pid]} ))
                            total_cpu_ticks=$(( total_cpu_ticks + tick_diff ))
                            valid_count=$((valid_count + 1))
                        fi
                    fi
                done
                
                if [ $valid_count -eq 0 ]; then
                    value=0
                else
                    # 计算实际时间间隔（秒）
                    interval=$(awk "BEGIN { print $end_time - $start_time }")
                    # 获取系统时钟频率
                    clk_tck=$(getconf CLK_TCK)
                    [ -z "$clk_tck" ] && clk_tck=100  # 默认值
                    
                    # 计算总CPU时间(秒) = tick数 / 时钟频率
                    total_cpu_sec=$(awk "BEGIN { print $total_cpu_ticks / $clk_tck }")
                    # CPU利用率% = (总CPU时间 / 时间间隔) * 100
                    value=$(awk "BEGIN { printf \"%.1f\", ($total_cpu_sec / $interval) * 100 }")
                fi
            fi
        else
            # 系统级CPU监控（保持不变）
            value=$(top -bn1 | awk -v cores=$cores '
                /%Cpu\(s\):/ {
                    idle = $8
                    use = 100 - idle
                    total_use = use * cores
                    printf "%.1f", total_use
                    exit
                }'
            )
        fi
        ;;
        
    mem)
        # 内存监控部分保持不变
        # 获取系统总内存（字节）
        full_mem=$(free -b | awk '/Mem/{print $2}')
        
        if [[ -n "$target" ]]; then
            # 获取目标进程及其子进程的RSS内存
            pids=$(pstree -T -p "$target" 2>/dev/null | grep -oP '\d+(?=\))' | sort -u | tr '\n' ',' | sed 's/,$//')
            
            if [[ -z "$pids" ]]; then
                used_mem=0
                value=0
            else
                # 计算目标进程的总内存使用量
                used_mem=$(ps -p "$pids" -o rss= | awk '{s+=$1 * 1024} END {print s}')
                value=$(awk -v u="$used_mem" -v t="$full_mem" 'BEGIN {printf "%.2f", u/t * 100}')
            fi
        else
            # 系统级内存监控
            used_mem=$(free -b | awk '/Mem/{print $3}')
            value=$(awk -v u="$used_mem" -v t="$full_mem" 'BEGIN {printf "%.2f", u/t * 100}')
        fi
        ;;
        
    *)
        echo "未知metric: $metric"
        exit 2
        ;;
esac

# 格式化输出（保持不变）
timestamp=$(date +"%Y-%m-%d %H:%M:%S")

if [[ -n "$target" ]]; then
    # 进程级输出
    if [[ -z "$pids" ]]; then
        echo "[$timestamp] WARNING: 进程 '$target' 未找到!"
    else
        if [[ "$metric" == "mem" ]]; then
            # 内存输出增加实际使用量
            echo "[$timestamp] $metric usage for '$target': ${value}% (${used_mem}/${full_mem})"
        else
            echo "[$timestamp] $metric usage for '$target': ${value}%"
        fi
    fi
else
    # 系统级输出
    if [[ "$metric" == "mem" ]]; then
        echo "[$timestamp] $metric usage: ${value}% (${used_mem}/${full_mem})"
    else
        echo "[$timestamp] $metric usage: ${value}%"
    fi
fi