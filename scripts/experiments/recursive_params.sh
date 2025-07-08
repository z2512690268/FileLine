source config.sh
# 生成参数池和顺序数组的函数
# ================================================
generate_params() {
    declare -gA PARAM_POOL  # 全局关联数组
    declare -ga PARAM_ORDER # 全局顺序数组

    local entry key value
    for entry in "${MASTER_PARAMS[@]}"; do
        # 跳过注释行和空行
        [[ "$entry" =~ ^# || -z "$entry" ]] && continue

        # 提取并清理 key 和 value
        IFS='=' read -r key value <<< "$entry"
        key=$(echo "$key" | xargs)    # 去除前后空格
        value=$(echo "$value" | xargs)

        PARAM_POOL["$key"]="$value"
        PARAM_ORDER+=("$key")
    done
}

# 核心递归逻辑
# ================================================
generate_combinations() {
    local -n arr=$1       # 接收顺序数组的引用
    local depth=$2
    local current_args=$3
    
    # 达到最大递归深度时执行实验
    if (( depth >= ${#arr[@]} )); then
        execute_experiment "$current_args"
        return
    fi
    
    local param=${arr[depth]}                   # 严格按PARAM_ORDER顺序取参数
    local values=(${PARAM_POOL[$param]})        # 从参数池获取候选值
    if [[ ${#values[@]} -eq 0 ]]; then
        # 跳过当前层，传递空参数标记，并进入下一层
        pre_param_group_hook $depth $param          # 进入层级钩子
        generate_combinations "$1" $((depth+1)) "${current_args} ${param}="
        post_param_group_hook $depth $param         # 离开层级钩子
    else
        pre_param_group_hook $depth $param          # 进入层级钩子
        for val in "${values[@]}"; do
            generate_combinations "$1" $((depth+1)) "${current_args} ${param}=${val}"
        done
        post_param_group_hook $depth $param         # 离开层级钩子
    fi
}

# 信号处理函数
# ================================================
EXPR_DIR=${PROJECT_ROOT}/scripts/experiments
MONITOR_DIR="${EXPR_DIR}/../monitors"
handle_interrupt() {
    echo -e "\n[!] 捕获中断信号 (Ctrl+C)"
    
    pre_interrupt_dir=$(pwd)
    cd $EXPR_DIR
    # 优先转发信号给子进程
    if [ -n "$child_pid" ]; then
        echo "[*] 正在停止实验进程 (PID: $child_pid)..."
        kill -SIGINT "$child_pid" 2>/dev/null
        wait "$child_pid" 2>/dev/null
    fi
    
    # 关闭所有监控
    echo "[*] 正在停止监控进程..."
    ${MONITOR_DIR}/monitors.sh stop --all
    
    cd $pre_interrupt_dir
    exit 1
}

# 注册信号处理
trap handle_interrupt SIGINT SIGTERM


# 主控制器
# ================================================
start_experiments() {
    generate_params
    generate_combinations "PARAM_ORDER" 0 ""
}