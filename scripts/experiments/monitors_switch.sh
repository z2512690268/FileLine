# DeepSpeed GPU内存利用率监控脚本
# 获取DeepSpeed主进程PID
get_deepspeed_pid() {
    ps aux | grep '[d]eepspeed' | grep -v grep | awk '{print $2}' | sort -u | head -1
}
start_ds_gpu_mem_monitor() {
    output_file=$1

    monitor_output_file=${output_file}.gpu_mem
    ${MONITOR_DIR}/monitors.sh clear --output $monitor_output_file
    ds_pid=$(get_deepspeed_pid)
    if [[ -z "$ds_pid" ]]; then
        echo "错误：未找到正在运行的DeepSpeed进程"
        return 1
    fi
    echo "找到DeepSpeed主进程 PID: $ds_pid"
    ${MONITOR_DIR}/monitors.sh start --name "gpu_mem" --interval 5 --collector ${MONITOR_DIR}/gpu_monitor.sh --output $monitor_output_file --collector-args "--metric gpu_mem --pid $ds_pid"
}
stop_ds_gpu_mem_monitor() {
    ${MONITOR_DIR}/monitors.sh stop --name "gpu_mem"
}

# GPU nvidia-smi dmon持续监控
start_gpumon_monitor() {
    output_file=$1

    monitor_output_file=${output_file}.gpumon
    ${MONITOR_DIR}/monitors.sh clear --output $monitor_output_file
    ${MONITOR_DIR}/monitors.sh start --name "gpumon" --collector "nvidia-smi dmon -s ut | awk '{ print strftime(\"%Y-%m-%d %H:%M:%S\"), \$0; fflush() }'" --output $monitor_output_file --continuous
}
stop_gpumon_monitor() {
    ${MONITOR_DIR}/monitors.sh stop --name "gpumon"
}

# CPU利用率监控
start_cpu_monitor() {
    output_file=$1
    target_pid=$2

    monitor_output_file=${output_file}.cpu
    ${MONITOR_DIR}/monitors.sh clear --output $monitor_output_file
    ${MONITOR_DIR}/monitors.sh start --name "cpu" --interval 0.5 --collector ${MONITOR_DIR}/data_collector.sh --output $monitor_output_file --collector-args "--metric cpu --target $target_pid"
}
stop_cpu_monitor() {
    ${MONITOR_DIR}/monitors.sh stop --name "cpu"
}

# 内存监控
start_mem_monitor() {
    output_file=$1
    target_pid=$2

    monitor_output_file=${output_file}.mem
    ${MONITOR_DIR}/monitors.sh clear --output $monitor_output_file
    ${MONITOR_DIR}/monitors.sh start --name "mem" --interval 0.5 --collector ${MONITOR_DIR}/data_collector.sh --output $monitor_output_file --collector-args "--metric mem"
}
stop_mem_monitor() {
    ${MONITOR_DIR}/monitors.sh stop --name "mem"
}