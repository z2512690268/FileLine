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

# 钩子函数实现区
# ================================================
pre_param_group_hook() {
    echo "[PRE] 进入参数层级 $1: $2"
}

post_param_group_hook() {
    echo "[POST] 离开参数层级 $1: $2"
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