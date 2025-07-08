#!/bin/bash
source config.sh

# 实验配置区（显式指定参数顺序）
# ================================================
MASTER_PARAMS=(
    "model_name             =   LLM-Research/Llama-3.2-1B"
    # "ckpt_engine            =   deepspeed"
    # "ckpt_engine            =   datastates_llm"
    # "ckpt_engine            =   dlrover_flash"
    "ckpt_engine            =   grad"
    "tensor_parallel_size   =   0"
    "pipeline_parallel_size =   0"
    "num_iterations         =   300"
    "checkpoint_every       =   100"
    "batch_size             =   1"
    "max_seq_length         =   512"
    "dataload_same_length   =   True"
    "checkpoint_dir         =   ${CKPT_PATH}"
    # "checkpoint_dir         =   None"
    "load_checkpoint_dir    =   None"
    # "load_checkpoint_dir    =   ${CKPT_PATH}/deepspeed.2025.5.20.11.16.4.addjtvxg"
    # "load_checkpoint_dir    =   ${CKPT_PATH}/cpp_pretrain.2025.5.26.22.18.10.addjtvxg"
    "ckpt_tag               =   None"
    # "ckpt_tag               =   global_step50"
    "cpu_monitor            =   True"
    "mem_monitor            =   True"
    "gpumon_monitor         =   True"
)


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
    echo "[开始] 参数组合执行：$args_str"
    
    # 解析参数字符串到关联数组
    declare -A args_dict
    for param_pair in ${args_str}; do
        IFS='=' read -r key value <<< "${param_pair}"
        args_dict[$key]="$value"
    done

    # 提取具体参数值
    workdir=${PROJECT_ROOT}/test/pytest
    model_name=${args_dict["model_name"]}
    tensor_parallel_size=${args_dict["tensor_parallel_size"]}
    pipeline_parallel_size=${args_dict["pipeline_parallel_size"]}
    num_iterations=${args_dict["num_iterations"]}
    ckpt_engine=${args_dict["ckpt_engine"]}
    checkpoint_every=${args_dict["checkpoint_every"]}
    batch_size=${args_dict["batch_size"]}
    max_seq_length=${args_dict["max_seq_length"]}
    dataload_same_length=${args_dict["dataload_same_length"]}
    checkpoint_dir=${args_dict["checkpoint_dir"]}
    load_checkpoint_dir=${args_dict["load_checkpoint_dir"]}
    ckpt_tag=${args_dict["ckpt_tag"]}
    ckpt_dir_timestamp=$(date +%Y.%m.%d.%H.%M.%S)

    # 实际调用示例（例如调用Python训练脚本）
    output_dir=${PROJECT_ROOT}/scripts/data/test
    mkdir -p $output_dir
    #根据model_name，简写字符串，三种模型分别为Qwen, Llama, Opt   
    if [[ $model_name == *"Qwen"* ]]; then
        model_name_short="Qwen"
    elif [[ $model_name == *"Llama"* ]]; then
        model_name_short="Llama"
    elif [[ $model_name == *"opt"* ]]; then
        model_name_short="Opt"
    fi
    output_file=$output_dir/$model_name_short-$ckpt_engine-$num_iterations-$checkpoint_every-$batch_size-$max_seq_length-$dataload_same_length.log
    ${MONITOR_DIR}/monitors.sh clear --output $output_file
    
    original_dir=$(pwd)
    cd $workdir
    echo ./run_ds.sh --num_iterations $num_iterations --ckpt_engine $ckpt_engine --checkpoint_every $checkpoint_every --batch_size $batch_size --max_seq_length $max_seq_length --dataload_same_length $dataload_same_length --checkpoint_dir $checkpoint_dir --model_name $model_name --load_checkpoint_dir $load_checkpoint_dir --ckpt_tag $ckpt_tag --modelscope_cache_dir ${MODELSCOPE_PATH} --tensor_parallel_size $tensor_parallel_size --pipeline_parallel_size $pipeline_parallel_size --ckpt_dir_timestamp $ckpt_dir_timestamp
    echo $output_file
    # CUDA_VISIBLE_DEVICES=2,3,4,5
    ./run_ds.sh --num_iterations $num_iterations --ckpt_engine $ckpt_engine --checkpoint_every $checkpoint_every --batch_size $batch_size --max_seq_length $max_seq_length --dataload_same_length $dataload_same_length --checkpoint_dir $checkpoint_dir --model_name $model_name --load_checkpoint_dir $load_checkpoint_dir --ckpt_tag $ckpt_tag --modelscope_cache_dir ${MODELSCOPE_PATH} --tensor_parallel_size $tensor_parallel_size --pipeline_parallel_size $pipeline_parallel_size --ckpt_dir_timestamp $ckpt_dir_timestamp > $output_file 2>& 1 &
    child_pid=$!


    cpu_monitor_enabled=${args_dict["cpu_monitor"]}
    mem_monitor_enabled=${args_dict["mem_monitor"]}
    gpumon_monitor_enabled=${args_dict["gpumon_monitor"]}
    # start_gpu_mem_monitor $output_file
    if [[ "$cpu_monitor_enabled" == "True" ]]; then
        start_cpu_monitor $output_file $child_pid
    fi
    if [[ "$mem_monitor_enabled" == "True" ]]; then
        start_mem_monitor $output_file $child_pid
    fi
    if [[ "$gpumon_monitor_enabled" == "True" ]]; then
        start_gpumon_monitor $output_file
    fi
    # ---------------------------------等待逻辑---------------------------------
    if [ -n "$child_pid" ]; then
        wait "$child_pid"  # 等待进程完成
    fi
    # ---------------------------------等待逻辑结束---------------------------------
    if [[ "$gpumon_monitor_enabled" == "True" ]]; then
        stop_gpumon_monitor
    fi
    if [[ "$cpu_monitor_enabled" == "True" ]]; then
        stop_cpu_monitor
    fi
    if [[ "$mem_monitor_enabled" == "True" ]]; then
        stop_mem_monitor
    fi

    # stop_gpu_mem_monitor
    child_pid=""       # 清空PID记录
    # 添加其他逻辑（如日志记录、状态检查等）
    echo "[完成] 参数组合执行完毕"

    cd $original_dir
}

source recursive_params.sh
source monitors_switch.sh

main() {
    pip install ../.. -v
    echo "=== 开始参数遍历（顺序：${PARAM_ORDER[@]}）==="
    start_experiments
    echo "=== 遍历结束 ==="
}
main