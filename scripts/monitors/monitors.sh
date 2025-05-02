#!/bin/bash
MONITOR_DIR="/tmp/monitor_async.d"

usage() {
    echo "Multi-Monitor Manager (支持Collector参数传递)"
    echo "Usage:"
    echo "  $0 start  --name <ID> --interval <sec> [--samples <N>] --collector <script> --output <file> [--collector-args <\"quoted args\">]"
    echo "  $0 stop   [--name <ID> | --all]"
    echo "  $0 list"
    echo "  $0 clear  --output <file>         清空指定文件"
    echo "  $0 write --output <file> [--text \"内容\"]  向文件添加分割线"
    echo ""
    echo "示例:"
    echo "  # 传递CPU监控参数"
    echo "  $0 start --name cpu --interval 5 --collector stats.sh --output cpu.log --collector-args \"--metric cpu --warning 80\""
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
    echo "$MONITOR_DIR/${name}.pid $MONITOR_DIR/${name}.conf"
}

start_monitor() {
    declare -A args=(
        [name]="" 
        [interval]=""
        [samples]=""
        [collector]=""
        [output]=""
        [collector_args]=""  # 新增参数存储
    )
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --name)     args[name]="$2";     shift 2 ;;
            --interval) args[interval]="$2"; shift 2 ;;
            --samples)  args[samples]="$2";  shift 2 ;;
            --collector)args[collector]="$2";shift 2 ;;
            --output)   args[output]="$2";   shift 2 ;;
            --collector-args) args[collector_args]="$2"; shift 2 ;;  # 新增参数处理
            *) echo "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    # 参数验证
    [[ -z "${args[name]}" || -z "${args[collector]}" || -z "${args[output]}" ]] && {
        echo "缺少必要参数"; usage; exit 1
    }
    [[ "${args[interval]}" =~  ^[0-9]+(\.[0-9]+)?$ ]] || { echo "间隔时间必须为整数或浮点数"; exit 1; }
    [[ -n "${args[samples]}" && ! "${args[samples]}" =~ ^[0-9]+$ ]] && {
        echo "采样次数必须为整数"; exit 1
    }
    [[ -x "${args[collector]}" ]] || { echo "收集器脚本不可执行"; exit 1; }

    local pid_file conf_file
    gen_monitor_files "${args[name]}"
    read pid_file conf_file < <(gen_monitor_files "${args[name]}")
    echo "监控配置: $conf_file"
    echo "PID文件: $pid_file"
    # 防止重复启动
    if [[ -f "$pid_file" ]]; then
        if kill -0 $(cat "$pid_file") 2>/dev/null; then
            echo "监控任务 ${args[name]} 已在运行中"
            exit 2
        else
            rm -f "$pid_file" "$conf_file"
        fi
    fi

    # 保存配置
    declare -p args > "$conf_file"
    echo "配置已保存: $conf_file"
    # 启动监控进程
    (
        trap "rm -f '$pid_file' '$conf_file'" EXIT
        source "$conf_file"
        
        count=0
        while :; do
            eval "${args[collector]} ${args[collector_args]}" >> "${args[output]}"         

            ((count++))
            [[ -n "${args[samples]}" && $count -ge "${args[samples]}" ]] && break
            sleep "${args[interval]}"
        done
    ) & echo $! > "$pid_file"

    echo "监控已启动 [ID: ${args[name]}] PID: $(cat "$pid_file")"
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
        # 停止所有监控
        for conf in "$MONITOR_DIR"/*.conf; do
            [[ -f "$conf" ]] || continue
            source "$conf"
            local name=${args[name]}
            local pid_file="$MONITOR_DIR/${name}.pid"
            
            if [[ -f "$pid_file" ]]; then
                kill $(cat "$pid_file") 2>/dev/null && \
                echo "已停止监控任务: $name" || \
                echo "停止失败: $name"
            fi
        done
        rm -f "$MONITOR_DIR"/*.pid "$MONITOR_DIR"/*.conf
        return
    fi

    # 停止指定监控
    [[ -z "$target_name" ]] && { echo "需要指定 --name 或 --all"; usage; exit 1; }
    
    local pid_file conf_file
    read pid_file conf_file < <(gen_monitor_files "$target_name")
    
    if [[ -f "$pid_file" ]]; then
        if kill -0 $(cat "$pid_file") 2>/dev/null; then
            kill $(cat "$pid_file")
            echo "已停止监控任务: $target_name"
        else
            echo "进程不存在: $target_name"
        fi
        rm -f "$pid_file" "$conf_file"
    else
        echo "监控任务不存在: $target_name"
    fi
}

list_monitors() {
    printf "%-10s %-8s %-12s %-6s %s\n" "NAME" "PID" "INTERVAL" "SAMPLES" "OUTPUT"
    for conf in "$MONITOR_DIR"/*.conf; do
        [[ -f "$conf" ]] || continue
        source "$conf"
        local pid_file="$MONITOR_DIR/${args[name]}.pid"
        local pid=$(cat "$pid_file" 2>/dev/null)
        
        if [[ -n "$pid" && -d "/proc/$pid" ]]; then
            printf "%-10s %-8s %-12s %-6s %s\n" \
                   "${args[name]}" \
                   "$pid" \
                   "${args[interval]}s" \
                   "${args[samples]:-∞}" \
                   "${args[output]}"
        else
            # 清理无效记录
            rm -f "$conf" "$pid_file"
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
    *)     usage ;;
esac