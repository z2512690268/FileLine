#!/bin/bash

# 测试命令 ./monitors.sh start --name cpu_alert --interval 1 --samples 10 --collector ./template_collector.sh --collector-args "--metric mem --warning 90 --critical 95" --output alerts.log
# 支持参数解析的collector脚本示例
while [[ $# -gt 0 ]]; do
    case "$1" in
        --metric)   metric="$2";   shift 2 ;;
        --warning)  warn="$2";     shift 2 ;;
        --critical) crit="$2";    shift 2 ;;
        *)          echo "未知参数: $1"; exit 1 ;;
    esac
done

# 实际数据收集逻辑
case $metric in
    cpu)
        value=$(top -bn1 | grep "Cpu(s)" | sed -E 's/.*,[[:space:]]*([0-9.]+)%* id.*/\1/' | awk '{print 100 - $1}')
        ;;
    mem)
        value=$(free | grep Mem | awk '{print $3/$2 * 100.0}')
        ;;
    *)
        echo "未知metric: $metric"
        exit 2
        ;;
esac

# 输出带状态信息
timestamp=$(date +"%T")
if (( $(echo "$value >= $crit" | bc -l) )); then
    status="CRITICAL"
elif (( $(echo "$value >= $warn" | bc -l) )); then
    status="WARNING"
else
    status="OK"
fi

echo "[$timestamp][$status] $metric usage: ${value}%"