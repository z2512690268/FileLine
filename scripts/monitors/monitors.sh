#!/bin/bash
MONITOR_DIR="/tmp/monitor_async.d"

usage() {
    echo "Multi-Monitor Manager (支持Collector参数传递)"
    echo "Usage:"
    echo "  $0 start  --name <ID> --interval <sec> [--samples <N>] --collector <script> --output <file> [--collector-args <\"quoted args\">] [--continuous]"
    echo "  $0 stop   [--name <ID> | --all]"
    echo "  $0 list"
    echo "  $0 clear  --output <file>         清空指定文件"
    echo "  $0 write --output <file> [--text \"内容\"]  向文件添加分割线"
    echo "  $0 wait   --name <ID>           等待指定监控任务完成（如果带有samples参数）" 
    echo ""
    echo "示例:"
    echo "  # 传递CPU监控参数"
    echo "  $0 start --name cpu --interval 5 --collector stats.sh --output cpu.log --collector-args \"--metric cpu --warning 80\""
    echo ""
    echo "  # 持续运行nvidia-smi dmon"
    echo "  $0 start --name gpumon --collector \"nvidia-smi dmon\" --output gpu.log --continuous"
    echo ""
    echo "  # 带空格参数需用引号包裹"
    echo "  $0 start --name temp --interval 10 --collector sensors.sh --output temp.log --collector-args \"--type 'CPU Temp' --unit celsius\""
    echo ""
    echo "  # 停止指定监控"
    echo "  $0 stop --name cpu"
    echo ""
    echo "  # 停止所有监控"
    echo "  $0 stop --all"
    echo ""
    echo "  # 列出所有监控"
    echo "  $0 list"
    echo ""
    echo "  # 清空指定文件"
    echo "  $0 clear --output cpu.log"
    echo ""
    echo "  # 向文件添加分割线"
    echo "  $0 write --output cpu.log --text \"------------------\""
}

clear_file() {
    local output
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --output) output="$2"; shift 2 ;;
            *) echo "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    if [[ -z "$output" ]]; then
        echo "必须指定--output参数"
        exit 1
    fi

    if > "$output"; then
        echo "已清空文件: $output"
    else
        echo "清空文件失败: $output"
        exit 1
    fi
}

add_writer() {
    local output text="--------------------------------------------------"
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --output) output="$2"; shift 2 ;;
            --text) text="$2"; shift 2 ;;
            *) echo "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    if [[ -z "$output" ]]; then
        echo "必须指定--output参数"
        exit 1
    fi

    echo "$text" >> "$output"
    if [[ $? -eq 0 ]]; then
        echo "已添加分割线到文件: $output"
    else
        echo "添加分割线失败: $output"
        exit 1
    fi
}

ensure_dir() {
    mkdir -p "$MONITOR_DIR"
}

gen_monitor_files() {
    local name=$1
    echo "$MONITOR_DIR/${name}.pid $MONITOR_DIR/${name}.conf $MONITOR_DIR/${name}.child_pid"
}

start_monitor() {
    declare -A args=(
        [name]="" 
        [interval]=""
        [samples]=""
        [collector]=""
        [output]=""
        [collector_args]=""
        [continuous]=""
    )
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --name)     args[name]="$2";     shift 2 ;;
            --interval) args[interval]="$2"; shift 2 ;;
            --samples)  args[samples]="$2";  shift 2 ;;
            --collector)args[collector]="$2";shift 2 ;;
            --output)   args[output]="$2";   shift 2 ;;
            --collector-args) args[collector_args]="$2"; shift 2 ;;
            --continuous) args[continuous]="true"; shift ;;
            *) echo "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    # 参数验证
    [[ -z "${args[name]}" || -z "${args[collector]}" || -z "${args[output]}" ]] && {
        echo "缺少必要参数"; usage; exit 1
    }
    
    # 持续模式无需interval/samples验证
    if [[ -z "${args[continuous]}" ]]; then
        [[ -z "${args[interval]}" ]] && { echo "瞬时监控需要--interval参数"; exit 1; }
        [[ "${args[interval]}" =~  ^[0-9]+(\.[0-9]+)?$ ]] || { echo "间隔时间必须为整数或浮点数"; exit 1; }
        [[ -n "${args[samples]}" && ! "${args[samples]}" =~ ^[0-9]+$ ]] && {
            echo "采样次数必须为整数"; exit 1
        }
    fi

    local pid_file conf_file child_pid_file
    gen_monitor_files "${args[name]}"
    read pid_file conf_file child_pid_file < <(gen_monitor_files "${args[name]}")

    # 防止重复启动
    if [[ -f "$pid_file" ]]; then
        if kill -0 $(cat "$pid_file") 2>/dev/null; then
            echo "监控任务 ${args[name]} 已在运行中"
            exit 2
        else
            rm -f "$pid_file" "$conf_file" "$child_pid_file"
        fi
    fi

    # 保存配置
    declare -p args > "$conf_file"
    
    # 启动监控进程
    (
        # 设置清理trap
        cleanup() {
            [[ -f "$child_pid_file" ]] && kill $(cat "$child_pid_file") 2>/dev/null
            rm -f "$pid_file" "$conf_file" "$child_pid_file"
        }
        trap 'cleanup' EXIT
        
        source "$conf_file"
        
        if [[ -n "${args[continuous]}" ]]; then
            # 持续运行模式 - 创建独立进程组
            (
                trap 'exit 0' INT TERM
                trap 'kill $(jobs -p) 2>/dev/null' EXIT
                eval "${args[collector]} ${args[collector_args]}"
            ) >> "${args[output]}" 2>&1 &
            
            child_pid=$!
            echo $child_pid > "$child_pid_file"
            
            # 监控主进程等待子进程退出
            wait $child_pid
        else
            # 瞬时采样模式
            count=0
            while :; do
                eval "${args[collector]} ${args[collector_args]}" >> "${args[output]}" 2>&1
                ((count++))
                [[ -n "${args[samples]}" && $count -ge "${args[samples]}" ]] && break
                sleep "${args[interval]}"
            done
        fi
    ) & 
    
    # 保存监控主进程PID
    monitor_pid=$!
    echo $monitor_pid > "$pid_file"

    echo "监控已启动 [ID: ${args[name]}] PID: $monitor_pid"
}

stop_monitor() {
    local stop_all=0 target_name

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --name) target_name="$2"; shift 2 ;;
            --all)  stop_all=1;       shift ;;
            *)      echo "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    if ((stop_all)); then
        echo "正在停止所有监控任务..."
        # 首先停止所有子进程
        for child_pid_file in "$MONITOR_DIR"/*.child_pid; do
            [[ -f "$child_pid_file" ]] || continue
            child_pid=$(cat "$child_pid_file")
            
            # 杀死整个进程组
            pgid=$(ps -o pgid= $child_pid 2>/dev/null | grep -o '[0-9]\+')
            if [[ -n "$pgid" ]]; then
                kill -- -$pgid 2>/dev/null
            fi
            
            # 确保子进程终止
            kill -9 $child_pid 2>/dev/null
            rm -f "$child_pid_file"
        done
        
        # 然后停止所有主进程
        for pid_file in "$MONITOR_DIR"/*.pid; do
            [[ -f "$pid_file" ]] || continue
            pid=$(cat "$pid_file")
            kill -9 $pid 2>/dev/null
            rm -f "$pid_file"
        done
        
        # 最后清理配置文件
        rm -f "$MONITOR_DIR"/*.conf
        echo "所有监控任务已停止"
        return
    fi

    # 停止指定监控
    [[ -z "$target_name" ]] && { echo "需要指定 --name 或 --all"; usage; exit 1; }
    
    local pid_file conf_file child_pid_file
    read pid_file conf_file child_pid_file < <(gen_monitor_files "$target_name")
    
    if [[ ! -f "$pid_file" ]]; then
        echo "监控任务不存在: $target_name"
        return 1
    fi

    # 首先处理持续监控子进程
    if [[ -f "$child_pid_file" ]]; then
        child_pid=$(cat "$child_pid_file")
        
        # 尝试通过进程组终止
        pgid=$(ps -o pgid= $child_pid 2>/dev/null | grep -o '[0-9]\+')
        if [[ -n "$pgid" ]]; then
            kill -- -$pgid 2>/dev/null
            echo "已通过进程组终止子进程: $child_pid (PGID: $pgid)"
        fi
        
        # 确保子进程终止
        if kill -0 $child_pid 2>/dev/null; then
            kill -9 $child_pid
            echo "已强制终止子进程: $child_pid"
        fi
        
        rm -f "$child_pid_file"
    fi

    # 然后停止主监控进程
    pid=$(cat "$pid_file")
    if kill -0 $pid 2>/dev/null; then
        kill -9 $pid
        echo "已终止监控主进程: $pid"
    fi
    
    # 清理所有相关文件
    rm -f "$pid_file" "$conf_file"
    echo "监控任务已停止: $target_name"
}

wait_for_monitor() {
    local name
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --name) name="$2"; shift 2 ;;
            *) echo "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    if [[ -z "$name" ]]; then
        echo "必须指定--name参数"
        usage
        exit 1
    fi

    local conf_file="$MONITOR_DIR/${name}.conf"
    local pid_file="$MONITOR_DIR/${name}.pid"

    if [[ ! -f "$conf_file" ]]; then
        echo "监控任务 $name 不存在"
        exit 1
    fi

    # 读取配置
    unset args
    source "$conf_file" || exit 1

    if [[ -n "${args[continuous]}" ]]; then
        echo "监控任务 $name 为持续运行模式，请使用stop命令停止"
        return 0
    fi

    if [[ -z "${args[samples]}" ]]; then
        echo "监控任务 $name 没有设置samples参数，直接返回"
        return 0
    fi

    if [[ ! -f "$pid_file" ]]; then
        echo "监控任务 $name 的PID文件不存在，可能已经完成"
        return 0
    fi

    local pid=$(cat "$pid_file" 2>/dev/null)
    if [[ -z "$pid" ]]; then
        echo "无法读取监控任务 $name 的PID"
        return 1
    fi

    if kill -0 "$pid" 2>/dev/null; then
        echo "等待监控任务 $name 完成，samples=${args[samples]}..."
        while kill -0 "$pid" 2>/dev/null; do
            sleep 1
        done
        echo "监控任务 $name 已完成"
    else
        echo "监控任务 $name 的进程未在运行"
        rm -f "$pid_file" "$conf_file"
    fi

    return 0
}

list_monitors() {
    printf "%-12s %-8s %-12s %-6s %-12s %s\n" "NAME" "PID" "INTERVAL" "SAMPLES" "MODE" "OUTPUT"
    for conf in "$MONITOR_DIR"/*.conf; do
        [[ -f "$conf" ]] || continue
        source "$conf"
        local pid_file="$MONITOR_DIR/${args[name]}.pid"
        local pid=$(cat "$pid_file" 2>/dev/null)
        
        if [[ -n "$pid" && -d "/proc/$pid" ]]; then
            printf "%-12s %-8s %-12s %-6s %-12s %s\n" \
                   "${args[name]}" \
                   "$pid" \
                   "${args[interval]:-N/A}" \
                   "${args[samples]:-∞}" \
                   "${args[continuous]:+Continuous}" \
                   "${args[output]}"
        else
            # 清理无效记录
            rm -f "$conf" "$pid_file" "$MONITOR_DIR/${args[name]}.child_pid"
        fi
    done
}

ensure_dir
case $1 in
    start) shift; start_monitor "$@" ;;
    stop)  shift; stop_monitor "$@" ;;
    list)  list_monitors ;;
    clear) shift; clear_file "$@" ;;
    write) shift; add_writer "$@" ;;
    wait)  shift; wait_for_monitor "$@" ;;
    *)     usage ;;
esac