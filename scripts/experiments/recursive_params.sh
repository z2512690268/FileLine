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
    
    pre_param_group_hook $depth $param          # 进入层级钩子
    for val in "${values[@]}"; do
        generate_combinations "$1" $((depth+1)) "${current_args} ${param}=${val}"
    done
    post_param_group_hook $depth $param         # 离开层级钩子
}

# 主控制器
# ================================================
start_experiments() {
    generate_combinations "PARAM_ORDER" 0 ""
}