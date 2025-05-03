#!/bin/bash

# 实验配置区（显式指定参数顺序）
# ================================================
declare -A PARAM_POOL=(
    ["script"]="run.sh"
    ["lr"]="0.1 0.01"
    ["batch"]="64 128"
    ["optim"]="adam sgd"
)
declare -a PARAM_ORDER=("script" "lr" "batch" "optim")  # 显式指定顺序
# 信号处理函数
# ================================================
handle_interrupt() {
    echo -e "\n[!] 捕获中断信号 (Ctrl+C)"
    
    # 优先转发信号给子进程
    if [ -n "$child_pid" ]; then
        echo "[*] 正在停止实验进程 (PID: $child_pid)..."
        kill -SIGINT "$child_pid" 2>/dev/null
        wait "$child_pid" 2>/dev/null
    fi
    
    # 关闭所有监控
    echo "[*] 正在停止监控进程..."
    ${MONITOR_DIR}/monitors.sh stop --all
    
    exit 1
}

# 注册信号处理
trap handle_interrupt SIGINT SIGTERM
# 钩子函数实现区
# ================================================
MONITOR_DIR="../monitors"
pre_param_group_hook() {
    echo "[PRE] 进入参数层级 $1: $2"

    if [[ $1 == 0 ]]
    then
        # echo "script参数组：$2"
        ${MONITOR_DIR}/monitors.sh clear --output alerts.log
        ${MONITOR_DIR}/monitors.sh start --name cpu_alert --interval 2 --samples 3 --collector ${MONITOR_DIR}/data_collector.sh --collector-args "--metric mem --warning 90 --critical 95" --output alerts.log
        ${MONITOR_DIR}/monitors.sh wait --name cpu_alert
        ${MONITOR_DIR}/monitors.sh write --output alerts.log
        ${MONITOR_DIR}/monitors.sh start --name cpu_alert --interval 2 --collector ${MONITOR_DIR}/data_collector.sh --collector-args "--metric mem --warning 90 --critical 95" --output alerts.log
    fi
}

post_param_group_hook() {
    echo "[POST] 离开参数层级 $1: $2"

    if [[ $1 == 0 ]]
    then
        # echo "script参数组：$2"
        ${MONITOR_DIR}/monitors.sh stop --name cpu_alert
    fi
}

# 执行实验逻辑
# ================================================
execute_experiment() {
    local args_str=$1
    
    # 解析参数字符串到关联数组
    declare -A args_dict
    for param_pair in ${args_str}; do
        IFS='=' read -r key value <<< "${param_pair}"
        args_dict[$key]="$value"
    done

    # 提取具体参数值
    local script=${args_dict[script]}
    local lr=${args_dict[lr]}
    local batch=${args_dict[batch]}
    local optim=${args_dict[optim]}

    # 实际调用示例（例如调用Python训练脚本）
    echo "[执行] 正在启动实验..."
    echo python train.py \
        --script "${script}" \
        --lr "${lr}" \
        --batch-size "${batch}" \
        --optimizer "${optim}"
    
    sleep 10
    child_pid=$!
    if [ -n "$child_pid" ]; then
        wait "$child_pid"  # 等待进程完成
    fi
    child_pid=""       # 清空PID记录
    # 添加其他逻辑（如日志记录、状态检查等）
    echo "[完成] 参数组合执行完毕"
}

source recursive_params.sh

main() {
    echo "=== 开始参数遍历（顺序：${PARAM_ORDER[@]}）==="
    start_experiments
    echo "=== 遍历结束 ==="
}
main