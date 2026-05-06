"""FileLine - 可视化流水线构建器

支持多步数据处理流水线：选择数据 → 添加处理步骤 → 执行 → 查看中间结果与血缘追溯。
"""

import sys
import os
import json
import html as html_mod
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import processes  # noqa: 注册所有 processor

# ==================== JS→Python 桥接服务器 ====================
# 轻量级 HTTP 服务器, JS 通过 fetch POST 发送数据,
# Python 端在每次片段 rerun 时消费队列数据。
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

_bridge_queue = []
_bridge_lock = threading.Lock()
_bridge_port = None


def _start_bridge_server():
    """Start a localhost HTTP server for JS→Python bridge communication."""
    class _BridgeHandler(BaseHTTPRequestHandler):
        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
            try:
                data = json.loads(body)
                with _bridge_lock:
                    _bridge_queue.append(data)
            except json.JSONDecodeError:
                pass
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass  # suppress logs

    server = HTTPServer(("0.0.0.0", 0), _BridgeHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return port


def _consume_bridge_data(ds_idx):
    """Read and remove bridge data for a specific data source index."""
    global _bridge_queue
    with _bridge_lock:
        result = [item for item in _bridge_queue if item.get("dsIdx") == ds_idx]
        _bridge_queue = [item for item in _bridge_queue if item.get("dsIdx") != ds_idx]
    return result


_bridge_port = _start_bridge_server()
import yaml

# ==================== 流水线管理（持久化存储）====================
MANAGED_PIPELINES_DIR = Path(__file__).parent.parent / "managed_pipelines"
MANAGED_PIPELINES_DIR.mkdir(exist_ok=True)
FILELINE_PIPELINES_DIR = Path(__file__).parent.parent / "FileLine-Pipelines"

from core.processing import ProcessorRegistry
from app_utils import (
    get_data_entries,
    get_entry_dataframe,
    PipelineStepConfig,
    PipelineExecutionResult,
    DataSourceConfig,
    resolve_data_sources,
    render_processor_params_form,
    execute_pipeline,
    build_provenance_tree,
    render_provenance_ui,
    render_file_by_extension,
)

st.set_page_config(page_title="流水线构建器", page_icon="📈", layout="wide")

# ==================== Session State 初始化 ====================
if "pipeline_input_id" not in st.session_state:
    st.session_state.pipeline_input_id = None
if "pipeline_steps" not in st.session_state:
    st.session_state.pipeline_steps = []
if "pipeline_results" not in st.session_state:
    st.session_state.pipeline_results = {}
if "pipeline_status" not in st.session_state:
    st.session_state.pipeline_status = "idle"
if "pipeline_error" not in st.session_state:
    st.session_state.pipeline_error = None
if "pipeline_column_cache" not in st.session_state:
    st.session_state.pipeline_column_cache = {}
if "pipeline_expand_steps" not in st.session_state:
    st.session_state.pipeline_expand_steps = True
if "pipeline_data_sources" not in st.session_state:
    st.session_state.pipeline_data_sources = []
if "pipeline_data_context" not in st.session_state:
    st.session_state.pipeline_data_context = {}
if "pipeline_data_source_counter" not in st.session_state:
    st.session_state.pipeline_data_source_counter = 0
if "pm_name" not in st.session_state:
    st.session_state.pm_name = ""
if "pm_description" not in st.session_state:
    st.session_state.pm_description = ""
if "pm_global_vars" not in st.session_state:
    st.session_state.pm_global_vars = {}
if "pm_last_saved_name" not in st.session_state:
    st.session_state.pm_last_saved_name = ""
if "pm_show_import_ui" not in st.session_state:
    st.session_state.pm_show_import_ui = False
if "pm_show_global_import_ui" not in st.session_state:
    st.session_state.pm_show_global_import_ui = False
if "pipeline_last_params" not in st.session_state:
    st.session_state.pipeline_last_params = None


# ==================== 辅助操作函数 ====================

def _processor_emoji(proc_name: str) -> str:
    import re
    info = ProcessorRegistry._processors.get(proc_name)
    if not info:
        return "\U0001f6e0⃝"
    module = getattr(info["func"], "__module__", "") or ""
    if "plot_operations" in module:
        return "\U0001f4c8"
    elif "table_operations" in module:
        return "\U0001f4ca"
    elif "csv_operations" in module:
        return "\U0001f4cb"
    elif "text_operations" in module:
        return "\U0001f4dd"
    elif "zerockpt_operations" in module:
        return "⚡"
    return "\U0001f6e0⃝"


def add_step(processor_name: str = ""):
    steps = st.session_state.pipeline_steps
    idx = len(steps) + 1
    var_name = f"step_{idx}"
    steps.append(PipelineStepConfig(processor_name=processor_name or "", output_var=var_name))
    st.session_state.pipeline_steps = list(steps)


def remove_step(step_id: str):
    steps = [s for s in st.session_state.pipeline_steps if s.step_id != step_id]
    for i, s in enumerate(steps, 1):
        s.output_var = f"step_{i}"
    st.session_state.pipeline_steps = steps


def move_step(step_id: str, direction: int):
    steps = st.session_state.pipeline_steps
    idx = next(i for i, s in enumerate(steps) if s.step_id == step_id)
    new_idx = idx + direction
    if new_idx < 0 or new_idx >= len(steps):
        return
    steps[idx], steps[new_idx] = steps[new_idx], steps[idx]
    for i, s in enumerate(steps, 1):
        s.output_var = f"step_{i}"
    st.session_state.pipeline_steps = list(steps)


def clear_results():
    st.session_state.pipeline_results = {}
    st.session_state.pipeline_status = "idle"
    st.session_state.pipeline_error = None
    st.session_state.pipeline_column_cache = {}
    st.session_state.pipeline_last_params = None


def add_data_source():
    st.session_state.pipeline_data_source_counter += 1
    n = st.session_state.pipeline_data_source_counter
    st.session_state.pipeline_data_sources.append(DataSourceConfig(name=f"source_{n}"))


def remove_data_source(idx: int):
    sources = st.session_state.pipeline_data_sources
    if 0 <= idx < len(sources):
        name = sources[idx].name
        sources.pop(idx)
        st.session_state.pipeline_data_context.pop(name, None)


# ==================== 流水线管理（持久化）====================
def _pm_list_pipelines():
    """列出所有已保存的流水线名称"""
    if not MANAGED_PIPELINES_DIR.exists():
        return []
    return sorted(
        d.name for d in MANAGED_PIPELINES_DIR.iterdir()
        if d.is_dir() and (d / "pipeline.yaml").exists()
    )


def _pm_list_fileline_yamls():
    """列出 FileLine-Pipelines/ 下所有 .yaml 文件（递归），返回相对路径"""
    if not FILELINE_PIPELINES_DIR.exists():
        return []
    yamls = []
    for root, _dirs, files in os.walk(FILELINE_PIPELINES_DIR):
        for f in files:
            if f.endswith((".yaml", ".yml")):
                rel = os.path.relpath(os.path.join(root, f), FILELINE_PIPELINES_DIR)
                yamls.append(rel)
    return sorted(yamls)


def _parse_global_file(path):
    """解析 .global 文件 KEY = VALUE 行为 dict，自动去除值的引号"""
    result = {}
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                val = v.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                    val = val[1:-1]
                result[k.strip()] = val
    return result


def _load_companion_global(yaml_path):
    """级联查找 .global 文件：同basename → 同目录同名 → 根目录 plot.global"""
    p = Path(yaml_path)
    # 1) 同 basename
    gf = p.with_suffix(".global")
    if gf.exists():
        return _parse_global_file(gf)
    # 2) 目录级
    parent = p.parent
    gf = parent / f"{parent.name}.global"
    if gf.exists():
        return _parse_global_file(gf)
    # 3) 根目录 plot.global 兜底
    gf = FILELINE_PIPELINES_DIR / "plot.global"
    if gf.exists():
        return _parse_global_file(gf)
    return {}


def _pm_load_pipeline(name):
    """从 managed_pipelines/<name>/ 加载流水线到 session_state"""
    base = MANAGED_PIPELINES_DIR / name
    if not (base / "pipeline.yaml").exists():
        st.error(f"流水线 '{name}' 不存在")
        return

    with open(base / "pipeline.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    st.session_state.pm_name = name
    st.session_state.pm_last_saved_name = name
    st.session_state.pm_description = cfg.get("description", "")

    # 加载全局变量
    global_vars = {}
    gf = base / "globals.global"
    if gf.exists():
        with open(gf, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, _, v = line.partition("=")
                    global_vars[k.strip()] = v.strip()
    st.session_state.pm_global_vars = global_vars

    # 加载数据源
    il = cfg.get("initial_load", {})
    includes = il.get("include", [])
    new_sources = []
    for idx, inc in enumerate(includes):
        ds = DataSourceConfig(
            name=f"source_{idx + 1}",
            include_patterns=[inc["path"]] if isinstance(inc, dict) and "path" in inc else [],
            tags=inc.get("tags", []) if isinstance(inc, dict) else [],
        )
        new_sources.append(ds)
    st.session_state.pipeline_data_sources = new_sources
    st.session_state.pipeline_data_source_counter = len(new_sources)
    # 自动解析数据源
    try:
        ctx = resolve_data_sources(new_sources)
        st.session_state.pipeline_data_context = ctx
    except Exception:
        import traceback
        print(f"resolve_data_sources failed: {traceback.format_exc()}")
        st.session_state.pipeline_data_context = {}

    # 加载步骤
    steps_data = cfg.get("steps", [])
    new_steps = []
    for i, sd in enumerate(steps_data):
        params = sd.get("params", {})
        inp = sd.get("inputs", "initial")
        input_sources = [inp] if isinstance(inp, str) else list(inp) if inp else []
        step = PipelineStepConfig(
            processor_name=sd.get("processor", ""),
            input_sources=input_sources,
            output_var=sd.get("output", f"step_{i + 1}"),
            params=params,
        )
        new_steps.append(step)
    st.session_state.pipeline_steps = new_steps
    clear_results()


def _pm_load_fileline_yaml(rel_path):
    """从 FileLine-Pipelines 加载 YAML 到编辑器"""
    yaml_path = FILELINE_PIPELINES_DIR / rel_path
    if not yaml_path.exists():
        st.error(f"YAML 文件不存在: {yaml_path}")
        return

    with open(yaml_path, "r") as f:
        cfg = yaml.safe_load(f)

    name = cfg.get("name") or rel_path
    st.session_state.pm_name = name
    st.session_state.pm_last_saved_name = ""
    st.session_state.pm_description = cfg.get("description", "")

    # 级联查找 .global
    st.session_state.pm_global_vars = _load_companion_global(yaml_path)

    # 加载数据源
    il = cfg.get("initial_load", {})
    includes = il.get("include", [])
    new_sources = []
    for idx, inc in enumerate(includes):
        ds = DataSourceConfig(
            name=f"source_{idx + 1}",
            include_patterns=[inc["path"]] if isinstance(inc, dict) and "path" in inc else [],
            tags=inc.get("tags", []) if isinstance(inc, dict) else [],
        )
        new_sources.append(ds)
    st.session_state.pipeline_data_sources = new_sources
    st.session_state.pipeline_data_source_counter = len(new_sources)
    # 自动解析数据源
    try:
        ctx = resolve_data_sources(new_sources)
        st.session_state.pipeline_data_context = ctx
    except Exception:
        import traceback
        print(f"resolve_data_sources failed: {traceback.format_exc()}")
        st.session_state.pipeline_data_context = {}

    # 加载步骤
    steps_data = cfg.get("steps", [])
    new_steps = []
    for i, sd in enumerate(steps_data):
        params = sd.get("params", {})
        inp = sd.get("inputs", "initial")
        input_sources = [inp] if isinstance(inp, str) else list(inp) if inp else []
        step = PipelineStepConfig(
            processor_name=sd.get("processor", ""),
            input_sources=input_sources,
            output_var=sd.get("output", f"step_{i + 1}"),
            params=params,
        )
        new_steps.append(step)
    st.session_state.pipeline_steps = new_steps
    clear_results()


def _pm_import_from_path(yaml_path_str, global_path_str=None):
    """从任意文件系统路径导入 YAML 到编辑器"""
    yaml_path = Path(yaml_path_str).expanduser().resolve()
    if not yaml_path.exists():
        st.error(f"YAML 文件不存在: {yaml_path}")
        return False
    if yaml_path.suffix.lower() not in (".yaml", ".yml"):
        st.error("文件必须是 .yaml 或 .yml 格式")
        return False

    with open(yaml_path, "r") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        st.error("无效的 YAML 格式")
        return False

    name = cfg.get("name") or yaml_path.stem
    st.session_state.pm_name = name
    st.session_state.pm_last_saved_name = ""
    st.session_state.pm_description = cfg.get("description", "")

    # 全局变量：显式指定 > 级联查找 > 空
    if global_path_str:
        gf = Path(global_path_str).expanduser().resolve()
        global_vars = _parse_global_file(gf) if gf.exists() else {}
    else:
        global_vars = _load_companion_global(yaml_path)
    st.session_state.pm_global_vars = global_vars

    # 加载数据源
    il = cfg.get("initial_load", {})
    includes = il.get("include", [])
    new_sources = []
    for idx, inc in enumerate(includes):
        ds = DataSourceConfig(
            name=f"source_{idx + 1}",
            include_patterns=[inc["path"]] if isinstance(inc, dict) and "path" in inc else [],
            tags=inc.get("tags", []) if isinstance(inc, dict) else [],
        )
        new_sources.append(ds)
    st.session_state.pipeline_data_sources = new_sources
    st.session_state.pipeline_data_source_counter = len(new_sources)
    # 自动解析数据源
    try:
        ctx = resolve_data_sources(new_sources)
        st.session_state.pipeline_data_context = ctx
    except Exception:
        import traceback
        print(f"resolve_data_sources failed: {traceback.format_exc()}")
        st.session_state.pipeline_data_context = {}

    # 加载步骤
    steps_data = cfg.get("steps", [])
    new_steps = []
    for i, sd in enumerate(steps_data):
        params = sd.get("params", {})
        inp = sd.get("inputs", "initial")
        input_sources = [inp] if isinstance(inp, str) else list(inp) if inp else []
        step = PipelineStepConfig(
            processor_name=sd.get("processor", ""),
            input_sources=input_sources,
            output_var=sd.get("output", f"step_{i + 1}"),
            params=params,
        )
        new_steps.append(step)
    st.session_state.pipeline_steps = new_steps
    clear_results()
    return True


def _pm_save_current_pipeline():
    """将当前 session_state 保存到 managed_pipelines/<name>/"""
    name = st.session_state.pm_name
    if not name:
        st.error("请先输入流水线名称")
        return False

    base = MANAGED_PIPELINES_DIR / name
    base.mkdir(parents=True, exist_ok=True)

    # 构建初始加载配置
    include_list = []
    for ds in st.session_state.pipeline_data_sources:
        for pat in ds.include_patterns:
            entry = {"path": pat}
            if ds.re_pattern:
                entry["re_pattern"] = ds.re_pattern
            if ds.tags:
                entry["tags"] = ds.tags
            include_list.append(entry)

    exclude_list = []
    for ds in st.session_state.pipeline_data_sources:
        for ep in ds.exclude_patterns:
            if ep not in exclude_list:
                exclude_list.append(ep)

    initial_load = {"include": include_list, "type": "raw"}
    if exclude_list:
        initial_load["exclude"] = exclude_list

    # 构建步骤列表
    steps_list = []
    final_outputs = []
    for step in st.session_state.pipeline_steps:
        if not step.processor_name:
            continue
        entry = {
            "processor": step.processor_name,
            "inputs": step.input_sources[0] if len(step.input_sources) == 1 else step.input_sources if step.input_sources else "initial",
            "output": step.output_var,
        }
        if step.params:
            entry["params"] = step.params
        steps_list.append(entry)
        final_outputs.append({"name": step.output_var})

    # 构建完整 YAML
    cfg = {
        "name": name,
        "description": st.session_state.pm_description,
        "initial_load": initial_load,
        "steps": steps_list,
        "final_output": final_outputs,
    }

    # 写入 pipeline.yaml
    with open(base / "pipeline.yaml", "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # 写入 globals.global
    if st.session_state.pm_global_vars:
        with open(base / "globals.global", "w") as f:
            f.write(f"# Global variables for {name}\n")
            for k, v in st.session_state.pm_global_vars.items():
                f.write(f"{k} = {v}\n")

    st.session_state.pm_last_saved_name = name
    return True


def _pm_delete_pipeline(name):
    """删除流水线及其所有文件"""
    import shutil
    base = MANAGED_PIPELINES_DIR / name
    if base.exists():
        shutil.rmtree(base)
    if st.session_state.pm_name == name:
        _pm_reset_session()


def _pm_reset_session():
    """重置编辑器为空白状态"""
    st.session_state.pm_name = ""
    st.session_state.pm_description = ""
    st.session_state.pm_global_vars = {}
    st.session_state.pm_last_saved_name = ""
    st.session_state.pipeline_data_sources = []
    st.session_state.pipeline_data_context = {}
    st.session_state.pipeline_data_source_counter = 0
    st.session_state.pipeline_steps = []
    clear_results()


def _pm_generate_yaml(download=False):
    """生成可在 FileLine-Pipelines 下使用的标准 YAML 字符串"""
    include_list = []
    for ds in st.session_state.pipeline_data_sources:
        for pat in ds.include_patterns:
            entry = {"path": pat}
            if ds.tags:
                entry["tags"] = ds.tags
            include_list.append(entry)

    exclude_list = []
    for ds in st.session_state.pipeline_data_sources:
        for ep in ds.exclude_patterns:
            if ep not in exclude_list:
                exclude_list.append(ep)

    initial_load = {"include": include_list, "type": "raw"}
    if exclude_list:
        initial_load["exclude"] = exclude_list

    steps_list = []
    final_outputs = []
    for step in st.session_state.pipeline_steps:
        if not step.processor_name:
            continue
        inputs_val = "initial"
        if step.input_sources:
            inputs_val = step.input_sources[0] if len(step.input_sources) == 1 else step.input_sources
        entry = {
            "processor": step.processor_name,
            "inputs": inputs_val,
            "output": step.output_var,
        }
        if step.params:
            entry["params"] = step.params
        steps_list.append(entry)

        # 应用全局变量替换（如果有）
        final_name = step.output_var
        for k, v in st.session_state.pm_global_vars.items():
            # 简单替换：如果 param 中包含 ${KEY} 则已处理
            pass
        final_outputs.append({"name": final_name})

    cfg = {
        "initial_load": initial_load,
        "steps": steps_list,
        "final_output": final_outputs,
    }

    return yaml.dump(cfg, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _pm_gen_global_file_content():
    """生成 .global 文件内容"""
    if not st.session_state.pm_global_vars:
        return ""
    lines = [f"# Global variables for {st.session_state.pm_name or 'pipeline'}"]
    for k, v in st.session_state.pm_global_vars.items():
        lines.append(f"{k} = {v}")
    return "\n".join(lines) + "\n"


def _summarize_pattern(selected_files):
    if not selected_files:
        return None
    exts = {Path(f).suffix.lower() for f in selected_files}
    dirs_set = {os.path.dirname(f) for f in selected_files}
    dirs_set.discard("")
    if len(dirs_set) == 1 and len(exts) == 1:
        ext = next(iter(exts))
        d = next(iter(dirs_set))
        return (os.path.relpath(d) + f"/*{ext}") if d else f"*{ext}"
    elif len(dirs_set) == 1:
        el = ",".join(sorted(exts))
        d = next(iter(dirs_set))
        return (os.path.relpath(d) + f"/*.{{{el}}}") if d else f"*.{{{el}}}"
    elif len(exts) == 1:
        return f"**/*{next(iter(exts))}"
    return None


DATA_EXTS = frozenset({".csv", ".json", ".parquet", ".xlsx", ".xls",
                        ".log", ".txt", ".yaml", ".yml", ".pdf"})
YAML_EXTS = frozenset({".yaml", ".yml"})



def _render_fb_html(ds_idx, cwd, dirs, files, selected, bridge_port, confirm_text="✅ 确认添加"):
    """Generate a native-like file browser in pure HTML/CSS/JS."""
    bk = f"fb_{ds_idx}"
    escaped_cwd = html_mod.escape(cwd)
    selected_list = list(selected)
    selected_json = json.dumps(selected_list)

    def _fmt_size(size):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    def _fmt_mtime(ts):
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

    # --- Breadcrumb path segments ---
    parts = cwd.rstrip("/").split("/")
    path_html = ""
    acc = ""
    for i, part in enumerate(parts):
        if not part:  # root "/"
            acc = "/"
            path_html += '<span class="fb-crumb fb-crumb-root" data-fb-nav="/">/</span>'
            continue
        acc = os.path.join(acc, part) if acc != "/" else "/" + part
        e = html_mod.escape(part)
        if i == len(parts) - 1:
            path_html += f'<span class="fb-crumb-sep">/</span><span class="fb-crumb fb-crumb-last">{e}</span>'
        else:
            path_html += f'<span class="fb-crumb-sep">/</span><a class="fb-crumb fb-crumb-link" data-fb-nav="{html_mod.escape(acc)}">{e}</a>'

    # --- Directory rows ---
    dir_rows = ""
    for d in dirs:
        ed = html_mod.escape(d)
        dir_rows += (
            f'<tr class="fb-dir-row" data-fb-nav="{ed}">'
            f'<td class="fb-cb-cell"></td>'
            f'<td class="fb-name-cell"><span class="fb-ico fb-ico-dir">\U0001f4c1</span> {ed}</td>'
            f'<td class="fb-size-cell">—</td>'
            f'<td class="fb-date-cell">—</td>'
            f'</tr>\n'
        )

    # --- File rows ---
    file_rows = ""
    for f in files:
        fn = f["name"]
        ef = html_mod.escape(fn)
        ck = "checked" if fn in selected else ""
        file_rows += (
            f'<tr class="fb-file-row">'
            f'<td class="fb-cb-cell"><input type="checkbox" class="fb-file-cb" data-fb-file="{ef}" {ck}></td>'
            f'<td class="fb-name-cell"><span class="fb-ico fb-ico-file">\U0001f4c4</span> {ef}</td>'
            f'<td class="fb-size-cell">{_fmt_size(f["size"])}</td>'
            f'<td class="fb-date-cell">{_fmt_mtime(f["mtime"])}</td>'
            f'</tr>\n'
        )

    return f"""<div id="{bk}" class="fb-root" data-fb-idx="{ds_idx}">
<style>
.fb-root[data-fb-idx="{ds_idx}"] * {{ box-sizing: border-box; }}
.fb-root[data-fb-idx="{ds_idx}"] {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 14px;
  border: 1px solid color-mix(in srgb, var(--text-color) 20%, transparent);
  border-radius: 6px;
  overflow: hidden;
  background: var(--background-color);
  color: var(--text-color);
  margin-bottom: 8px;
}}
/* toolbar */
.fb-toolbar {{
  display: flex; align-items: center; gap: 6px; padding: 6px 10px;
  background: var(--secondary-background-color);
  border-bottom: 1px solid color-mix(in srgb, var(--text-color) 15%, transparent);
}}
.fb-toolbar button {{
  background: none; border: 1px solid color-mix(in srgb, var(--text-color) 25%, transparent);
  border-radius: 4px; color: var(--text-color); cursor: pointer; padding: 2px 8px; font-size: 16px; line-height: 1.4;
}}
.fb-toolbar button:hover {{ background: color-mix(in srgb, var(--text-color) 10%, transparent); }}
.fb-path-bar {{ flex: 1; font-size: 13px; padding: 3px 8px; background: var(--background-color); border-radius: 4px;
  border: 1px solid color-mix(in srgb, var(--text-color) 15%, transparent); overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }}
.fb-crumb {{ color: var(--text-color); }}
.fb-crumb-link {{ color: var(--primary-color); text-decoration: none; cursor: pointer; }}
.fb-crumb-link:hover {{ text-decoration: underline; }}
.fb-crumb-last {{ color: var(--text-color); font-weight: 600; }}
.fb-crumb-sep {{ color: color-mix(in srgb, var(--text-color) 40%, transparent); margin: 0 1px; }}
/* header row (sticky) */
.fb-header {{
  display: flex; font-size: 12px; font-weight: 600; text-transform: uppercase;
  color: color-mix(in srgb, var(--text-color) 55%, transparent);
  background: var(--secondary-background-color);
  border-bottom: 1px solid color-mix(in srgb, var(--text-color) 12%, transparent);
  padding: 4px 10px;
}}
.fb-h-cb {{ width: 36px; flex-shrink: 0; }}
.fb-h-name {{ flex: 1; }}
.fb-h-size {{ width: 90px; flex-shrink: 0; text-align: right; }}
.fb-h-date {{ width: 150px; flex-shrink: 0; text-align: right; }}
/* body */
.fb-body {{ max-height: 420px; overflow-y: auto; }}
.fb-body table {{ width: 100%; border-collapse: collapse; }}
.fb-body td {{ padding: 3px 10px; border-bottom: 1px solid color-mix(in srgb, var(--text-color) 8%, transparent);
  white-space: nowrap; vertical-align: middle; }}
.fb-body tr:hover td {{ background: color-mix(in srgb, var(--primary-color) 8%, transparent); }}
.fb-cb-cell {{ width: 36px; text-align: center; }}
.fb-name-cell {{ cursor: default; }}
.fb-dir-row .fb-name-cell {{ cursor: pointer; }}
.fb-size-cell {{ width: 90px; text-align: right; color: color-mix(in srgb, var(--text-color) 60%, transparent); }}
.fb-date-cell {{ width: 150px; text-align: right; color: color-mix(in srgb, var(--text-color) 60%, transparent); }}
.fb-ico {{ margin-right: 4px; }}
.fb-ico-dir {{ }}
.fb-ico-file {{ opacity: 0.7; }}
/* footer */
.fb-footer {{
  display: flex; justify-content: space-between; align-items: center; padding: 6px 10px;
  background: var(--secondary-background-color);
  border-top: 1px solid color-mix(in srgb, var(--text-color) 15%, transparent);
}}
.fb-count {{ font-size: 13px; color: color-mix(in srgb, var(--text-color) 60%, transparent); }}
.fb-confirm {{
  background: var(--primary-color); color: white; border: none; border-radius: 4px;
  padding: 6px 18px; cursor: pointer; font-size: 14px; font-weight: 500;
}}
.fb-confirm:hover {{ filter: brightness(1.1); }}
.fb-confirm:disabled {{ opacity: 0.4; cursor: default; filter: none; }}
/* checkbox style */
.fb-file-cb, .fb-select-all {{ cursor: pointer; accent-color: var(--primary-color); }}
/* empty state */
.fb-empty {{ padding: 40px 20px; text-align: center; color: color-mix(in srgb, var(--text-color) 45%, transparent); }}
</style>
<div class="fb-toolbar">
  <button class="fb-up-btn" data-fb-nav=".." title="⬆ 上级目录">⬆</button>
  <div class="fb-path-bar">{path_html}</div>
</div>
<div class="fb-header">
  <span class="fb-h-cb"><input type="checkbox" class="fb-select-all" title="全选/取消"></span>
  <span class="fb-h-name">名称</span>
  <span class="fb-h-size">大小</span>
  <span class="fb-h-date">修改时间</span>
</div>
<div class="fb-body">
  <table>
    <tbody>
      {dir_rows if dir_rows else ''}
      {file_rows if file_rows else ''}
    </tbody>
  </table>
  {'' if dir_rows or file_rows else '<div class="fb-empty">此目录中无数据文件</div>'}
</div>
<div class="fb-footer">
  <span class="fb-count">已选择 {len(selected)} 个文件</span>
  <button class="fb-confirm" {'disabled' if not selected else ''}>{confirm_text}</button>
</div>
<script>
(function(){{
  var DS = {json.dumps(ds_idx)};
  var KEY = 'fb_' + DS;
  var root = document.getElementById({json.dumps(bk)});
  if (!root) return;
  // state
  window.__FB = window.__FB || {{}};
  if (!window.__FB[KEY]) {{
    window.__FB[KEY] = {{ selected: new Set({selected_json}), dsIdx: DS }};
  }}
  var state = window.__FB[KEY];
  var BRIDGE_PORT = {json.dumps(bridge_port)};
  // helper: send action to Python via bridge server
  function _send(action) {{
    action.dsIdx = DS;
    fetch(window.location.protocol + '//' + window.location.hostname + ':' + BRIDGE_PORT + '/', {{
      method: 'POST',
      body: JSON.stringify(action)
    }}).catch(function(e) {{ console.warn('bridge fetch failed', e); }});
    setTimeout(function() {{
      var btns = document.querySelectorAll('button');
      for (var i = 0; i < btns.length; i++) {{
        if (btns[i].textContent.indexOf('▶▶' + DS) >= 0) {{
          btns[i].click();
          break;
        }}
      }}
    }}, 50);
  }}
  // hide trigger button
  var btns = document.querySelectorAll('button');
  for (var i = 0; i < btns.length; i++) {{
    if (btns[i].textContent.indexOf('▶▶' + DS) >= 0) {{
      var container2 = btns[i].closest('[data-testid="stButton"]');
      if (container2) {{ container2.style.cssText = 'position:absolute!important;left:-9999px!important;top:0;width:1px;height:1px;overflow:hidden;opacity:.01;pointer-events:none!important;'; }}
    }}
  }}
  // update selection count
  function _updateCount() {{
    var n = state.selected.size;
    var el = root.querySelector('.fb-count');
    if (el) el.textContent = '已选择 ' + n + ' 个文件';
    var btn = root.querySelector('.fb-confirm');
    if (btn) btn.disabled = n === 0;
  }}
  // click delegation
  root.addEventListener('click', function(e) {{
    // directory navigation
    var navEl = e.target.closest('[data-fb-nav]');
    if (navEl) {{
      e.preventDefault();
      state.selected.clear();
      _send({{action: 'navigate', dir: navEl.dataset.fbNav}});
      return;
    }}
    // confirm button
    if (e.target.closest('.fb-confirm')) {{
      _send({{action: 'confirm', files: Array.from(state.selected)}});
      return;
    }}
    // up button
    if (e.target.closest('.fb-up-btn')) {{
      state.selected.clear();
      _send({{action: 'navigate', dir: '..'}});
      return;
    }}
  }});
  // checkbox change delegation
  root.addEventListener('change', function(e) {{
    var cb = e.target.closest('.fb-file-cb');
    if (cb) {{
      var fname = cb.dataset.fbFile;
      if (cb.checked) state.selected.add(fname); else state.selected.delete(fname);
      _updateCount();
      return;
    }}
    // select all
    var sa = e.target.closest('.fb-select-all');
    if (sa) {{
      var checked = sa.checked;
      var cbs = root.querySelectorAll('.fb-file-cb');
      for (var j = 0; j < cbs.length; j++) {{
        cbs[j].checked = checked;
        if (checked) state.selected.add(cbs[j].dataset.fbFile); else state.selected.delete(cbs[j].dataset.fbFile);
      }}
      _updateCount();
    }}
  }});
  // initial count
  _updateCount();
}})();
</script>
</div>"""


# ==================== 文件浏览器 Fragment ====================

@st.fragment
def _file_browser_fragment(ds, ds_idx):
    """原生风格文件浏览器 — 使用 st.html(unsafe_allow_javascript=True) 渲染，
    JS 通过 HTTP bridge server + 隐藏 button 实现 JS→Python 双向通信。"""
    bk = f"fb_{ds_idx}"
    cwd_k = f"{bk}_cwd"
    sel_k = f"{bk}_sel"

    if cwd_k not in st.session_state:
        st.session_state[cwd_k] = os.path.abspath(".")
    if sel_k not in st.session_state:
        st.session_state[sel_k] = []

    cwd = st.session_state[cwd_k]

    # ---- 桥接组件: 隐藏的 button 触 rerun, JS 通过 bridge server 发送数据 ----
    trigger_key = f"{bk}_trigger"
    st.button(f" ▶▶{ds_idx}", key=trigger_key)

    # ---- 处理来自 JS 的桥接数据 (via bridge server queue) ----
    for data in _consume_bridge_data(ds_idx):
        action = data.get("action", "")

        if action == "navigate":
            target_dir = data.get("dir", "")
            if target_dir == "..":
                new_cwd = os.path.dirname(cwd)
            elif os.path.isabs(target_dir):
                new_cwd = target_dir
            else:
                new_cwd = os.path.normpath(os.path.join(cwd, target_dir))
            if os.path.isdir(new_cwd):
                abs_new = os.path.abspath(new_cwd)
                st.session_state[cwd_k] = abs_new
                st.session_state[sel_k] = []
            st.rerun()

        elif action == "confirm":
            selected_files = data.get("files", [])
            full_paths = [os.path.join(cwd, f) for f in selected_files]
            full_paths = [fp for fp in full_paths if os.path.isfile(fp)]
            if full_paths:
                ds.include_patterns = [os.path.relpath(fp) for fp in full_paths]
                with st.spinner("正在解析文件..."):
                    ctx = resolve_data_sources([ds])
                    st.session_state.pipeline_data_context.update(ctx)
                st.session_state[sel_k] = list(selected_files)
            st.rerun()

    # ---- 列出目录内容 ----
    try:
        entries = sorted(os.listdir(cwd))
    except PermissionError:
        entries = []
    except OSError:
        entries = []

    dirs = []
    data_files = []
    for name in entries:
        full = os.path.join(cwd, name)
        try:
            if os.path.isdir(full):
                dirs.append(name)
            elif os.path.isfile(full) and os.path.splitext(name)[1].lower() in DATA_EXTS:
                st_info = os.stat(full)
                data_files.append({
                    "name": name,
                    "size": st_info.st_size,
                    "mtime": st_info.st_mtime,
                })
        except OSError:
            pass

    # ---- 渲染原生风格文件浏览器 ----
    selected = st.session_state.get(sel_k, [])
    fb_html = _render_fb_html(ds_idx, cwd, dirs, data_files, selected, _bridge_port)
    st.html(fb_html, unsafe_allow_javascript=True)


# ==================== YAML 导入文件浏览器 Fragment ====================

@st.fragment
def _yaml_import_fragment():
    """文件浏览器专用 fragment — 选择 YAML 文件并导入流水线。"""
    ds_idx = "yaml_import"
    bk = f"fb_{ds_idx}"
    cwd_k = f"{bk}_cwd"
    sel_k = f"{bk}_sel"

    if cwd_k not in st.session_state:
        st.session_state[cwd_k] = os.path.abspath(".")
    if sel_k not in st.session_state:
        st.session_state[sel_k] = []

    cwd = st.session_state[cwd_k]

    # 桥接组件: 隐藏的 button 触发 rerun
    trigger_key = f"{bk}_trigger"
    st.button(f" ▶▶{ds_idx}", key=trigger_key)

    # 处理来自 JS 的桥接数据
    for data in _consume_bridge_data(ds_idx):
        action = data.get("action", "")

        if action == "navigate":
            target_dir = data.get("dir", "")
            if target_dir == "..":
                new_cwd = os.path.dirname(cwd)
            elif os.path.isabs(target_dir):
                new_cwd = target_dir
            else:
                new_cwd = os.path.normpath(os.path.join(cwd, target_dir))
            if os.path.isdir(new_cwd):
                st.session_state[cwd_k] = os.path.abspath(new_cwd)
                st.session_state[sel_k] = []
            st.rerun()

        elif action == "confirm":
            selected_files = data.get("files", [])
            full_paths = [os.path.join(cwd, f) for f in selected_files]
            full_paths = [fp for fp in full_paths if os.path.isfile(fp)]
            if full_paths:
                yaml_path = full_paths[0]
                st.session_state[sel_k] = list(selected_files)
                if _pm_import_from_path(yaml_path):
                    st.success(f"已导入: {st.session_state.pm_name}")
                    st.session_state.pm_show_import_ui = False
                    st.rerun()
            st.rerun()

    # 列出目录内容
    try:
        entries = sorted(os.listdir(cwd))
    except (PermissionError, OSError):
        entries = []

    dirs = []
    yaml_files = []
    for name in entries:
        full = os.path.join(cwd, name)
        try:
            if os.path.isdir(full):
                dirs.append(name)
            elif os.path.isfile(full) and os.path.splitext(name)[1].lower() in YAML_EXTS:
                st_info = os.stat(full)
                yaml_files.append({
                    "name": name,
                    "size": st_info.st_size,
                    "mtime": st_info.st_mtime,
                })
        except OSError:
            pass

    # 渲染文件浏览器
    selected = st.session_state.get(sel_k, [])
    fb_html = _render_fb_html(ds_idx, cwd, dirs, yaml_files, selected, _bridge_port,
                              confirm_text="📥 导入此 YAML")
    st.html(fb_html, unsafe_allow_javascript=True)


@st.fragment
def _global_import_fragment():
    """文件浏览器 — 选择 .global 文件并加载全局变量。"""
    ds_idx = "global_import"
    bk = f"fb_{ds_idx}"
    cwd_k = f"{bk}_cwd"
    sel_k = f"{bk}_sel"

    if cwd_k not in st.session_state:
        st.session_state[cwd_k] = os.path.abspath(".")
    if sel_k not in st.session_state:
        st.session_state[sel_k] = []

    cwd = st.session_state[cwd_k]

    trigger_key = f"{bk}_trigger"
    st.button(f" ▶▶{ds_idx}", key=trigger_key)

    for data in _consume_bridge_data(ds_idx):
        action = data.get("action", "")
        if action == "navigate":
            target_dir = data.get("dir", "")
            if target_dir == "..":
                new_cwd = os.path.dirname(cwd)
            elif os.path.isabs(target_dir):
                new_cwd = target_dir
            else:
                new_cwd = os.path.normpath(os.path.join(cwd, target_dir))
            if os.path.isdir(new_cwd):
                st.session_state[cwd_k] = os.path.abspath(new_cwd)
                st.session_state[sel_k] = []
            st.rerun()
        elif action == "confirm":
            selected_files = data.get("files", [])
            full_paths = [os.path.join(cwd, f) for f in selected_files]
            full_paths = [fp for fp in full_paths if os.path.isfile(fp)]
            if full_paths:
                global_path = full_paths[0]
                st.session_state[sel_k] = list(selected_files)
                try:
                    st.session_state.pm_global_vars = _parse_global_file(Path(global_path))
                    st.success(f"已加载: {os.path.basename(global_path)}")
                    st.session_state.pm_show_global_import_ui = False
                    st.rerun()
                except Exception as e:
                    st.error(f"加载失败: {e}")
            st.rerun()

    try:
        entries = sorted(os.listdir(cwd))
    except (PermissionError, OSError):
        entries = []

    dirs = []
    global_files = []
    for name in entries:
        full = os.path.join(cwd, name)
        try:
            if os.path.isdir(full):
                dirs.append(name)
            elif os.path.isfile(full) and os.path.splitext(name)[1].lower() == ".global":
                st_info = os.stat(full)
                global_files.append({
                    "name": name,
                    "size": st_info.st_size,
                    "mtime": st_info.st_mtime,
                })
        except OSError:
            pass

    selected = st.session_state.get(sel_k, [])
    fb_html = _render_fb_html(ds_idx, cwd, dirs, global_files, selected, _bridge_port,
                              confirm_text="📥 加载此 .global")
    st.html(fb_html, unsafe_allow_javascript=True)


# ==================== 页面标题 ====================
st.title("\U0001f4c8 可视化流水线构建器")
st.caption("构建多步数据处理流水线，支持分支/合并，追溯每步中间结果")

# ==================== 流水线管理工具栏 ====================
pm_pipelines = _pm_list_pipelines()
pm_fileline_yamls = _pm_list_fileline_yamls()
pm_fileline_options = [f"📋 {y}" for y in pm_fileline_yamls]
pm_options = ["(新建空白流水线)"] + pm_pipelines + pm_fileline_options
pm_is_loaded = bool(st.session_state.pm_name)

# 判断当前是否匹配某一个已保存的流水线
current_pm_name = st.session_state.pm_name
all_keys = ["(新建空白流水线)"] + pm_pipelines + pm_fileline_options
if current_pm_name in all_keys:
    pm_sel_index = all_keys.index(current_pm_name)
elif current_pm_name in pm_fileline_yamls:
    display = f"📋 {current_pm_name}"
    pm_sel_index = all_keys.index(display) if display in all_keys else 0
else:
    pm_sel_index = 0

tool_cols = st.columns([2.5, 1, 1, 1, 1, 1, 1], vertical_alignment="bottom")
with tool_cols[0]:
    selected_pm = st.selectbox(
        "流水线", options=pm_options, index=pm_sel_index, key="pm_selector",
        label_visibility="collapsed",
    )
with tool_cols[1]:
    if st.button("➕ 新建", use_container_width=True, help="新建空白流水线"):
        _pm_reset_session()
        st.rerun()
with tool_cols[2]:
    load_disabled = selected_pm == "(新建空白流水线)"
    if st.button("📂 加载", use_container_width=True, help="加载选中流水线",
                 disabled=load_disabled):
        if selected_pm.startswith("📋 "):
            _pm_load_fileline_yaml(selected_pm[2:])
        else:
            _pm_load_pipeline(selected_pm)
        st.rerun()
with tool_cols[3]:
    if st.button("💾 保存", use_container_width=True, type="primary", help="保存流水线",
                 disabled=not st.session_state.pm_name):
        if _pm_save_current_pipeline():
            st.success(f"已保存: {st.session_state.pm_name}")
            st.rerun()
with tool_cols[4]:
    if st.button("📥 导入 YAML", use_container_width=True, help="从文件系统导入 YAML 配置"):
        st.session_state.pm_show_import_ui = not st.session_state.pm_show_import_ui
        st.rerun()
with tool_cols[5]:
    if st.button("📤 YAML", use_container_width=True, help="导出为 YAML 配置",
                 disabled=not st.session_state.pipeline_steps):
        yaml_str = _pm_generate_yaml()
        st.download_button(
            label="📥 下载 pipeline.yaml",
            data=yaml_str,
            file_name=f"{st.session_state.pm_name or 'pipeline'}.yaml",
            mime="text/yaml",
        )
        # 如果有全局变量，也提供下载
        if st.session_state.pm_global_vars:
            globals_str = _pm_gen_global_file_content()
            st.download_button(
                label="📥 下载 globals.global",
                data=globals_str,
                file_name=f"{st.session_state.pm_name or 'pipeline'}.global",
                mime="text/plain",
            )
with tool_cols[6]:
    if st.button("🗑 删除", use_container_width=True, type="primary", help="删除流水线",
                 disabled=selected_pm == "(新建空白流水线)" or selected_pm.startswith("📋 ")):
        import shutil
        base = MANAGED_PIPELINES_DIR / selected_pm
        if base.exists():
            shutil.rmtree(base)
        if st.session_state.pm_name == selected_pm:
            _pm_reset_session()
        st.rerun()

# ==================== 导入 YAML UI ====================
if st.session_state.pm_show_import_ui:
    with st.container(border=True):
        st.markdown("### 📥 导入 YAML 配置")
        st.caption("在文件浏览器中选择要导入的 .yaml / .yml 文件。全局变量将自动从同名 .global 文件级联加载。")

        _yaml_import_fragment()

        if st.button("取消", use_container_width=True):
            st.session_state.pm_show_import_ui = False
            st.rerun()

# 流水线名称和描述（新建或已加载时显示）
if pm_is_loaded or selected_pm == "(新建空白流水线)":
    name_col, desc_col = st.columns([1, 3])
    with name_col:
        st.session_state.pm_name = st.text_input(
            "流水线名称", value=st.session_state.pm_name,
            placeholder="my_pipeline", key="pm_name_input",
        )
    with desc_col:
        st.session_state.pm_description = st.text_input(
            "描述（可选）", value=st.session_state.pm_description,
            placeholder="流水线功能描述", key="pm_desc_input",
        )

    # 全局变量编辑器
    gv = st.session_state.pm_global_vars
    gv_count = len(gv)
    gv_label = "🌐 全局变量"
    if gv_count > 0:
        gv_label += f" ({gv_count} 个已定义)"
    with st.expander(gv_label, expanded=gv_count > 0 or pm_is_loaded):
        st.caption("定义全局变量，可在处理器参数中使用 ${VAR} 引用")
        if gv:
            gv_keys = list(gv.keys())
            for k in gv_keys:
                gcols = st.columns([1.5, 3, 0.5])
                with gcols[0]:
                    new_k = st.text_input("变量名", value=k, key=f"gvk_{k}",
                                          placeholder="KEY").strip().upper()
                with gcols[1]:
                    new_v = st.text_input("值", value=gv.get(k, ""),
                                          key=f"gvv_{k}", placeholder="value")
                with gcols[2]:
                    if st.button("✕", key=f"gvdel_{k}"):
                        del st.session_state.pm_global_vars[k]
                        st.rerun()
                if new_k and new_k != k:
                    # 变量重命名
                    st.session_state.pm_global_vars[new_k] = st.session_state.pm_global_vars.pop(k, "")
                elif new_v != gv.get(k, ""):
                    st.session_state.pm_global_vars[k] = new_v

        if st.button("➕ 添加变量", key="add_gv", use_container_width=True):
            n = 0
            for existing_key in gv:
                if existing_key.startswith("VAR"):
                    try:
                        n = max(n, int(existing_key[3:]))
                    except ValueError:
                        pass
            st.session_state.pm_global_vars[f"VAR{n+1}"] = ""
            st.rerun()

        if st.button("📥 加载 .global 文件", key="load_global", use_container_width=True):
            st.session_state.pm_show_global_import_ui = not st.session_state.pm_show_global_import_ui
            st.rerun()

        if st.session_state.pm_show_global_import_ui:
            _global_import_fragment()

st.divider()

# ==================== Section 1: 选择数据文件 ====================
st.header("Step 1: 选择数据文件")

for idx, ds in enumerate(st.session_state.pipeline_data_sources):
    resolved_ids = st.session_state.pipeline_data_context.get(ds.name, [])
    n_files = len(resolved_ids)
    is_resolved = ds.name in st.session_state.pipeline_data_context

    with st.container(border=True):
        row = st.columns([3, 1.2, 0.6, 0.7] if is_resolved else [3, 1.2, 0.7])
        with row[0]:
            label = f"**{ds.name}**"
            label += f"  — {n_files} 个文件 ✅" if is_resolved else "  — 未选择文件"
            st.markdown(label)
        with row[1]:
            if is_resolved:
                pat = _summarize_pattern(ds.include_patterns) if ds.include_patterns else None
                if pat:
                    st.caption(f"`{pat}`")
                elif n_files == 1:
                    st.caption("1 个文件")
                else:
                    st.caption(f"{n_files} 个文件")
        if is_resolved:
            with row[2]:
                if st.button("\U0001f504", key=f"reres_{idx}", help="重新选择"):
                    st.session_state.pipeline_data_context.pop(ds.name, None)
                    st.rerun()
        with row[-1]:
            st.button("✕ 删除", on_click=remove_data_source, args=(idx,),
                      key=f"ds_remove_{idx}", type="primary")

        if not is_resolved:
            _file_browser_fragment(ds, idx)

if st.button("➕ 添加数据源", use_container_width=True):
    add_data_source()
    st.rerun()

if not st.session_state.pipeline_data_sources:
    st.info("点击「添加数据源」开始选择数据文件")

st.divider()

# ==================== Section 2: 构建流水线步骤 ====================
st.header("Step 2: 构建流水线步骤")

all_processors = sorted(ProcessorRegistry._processors.keys())

if st.session_state.pipeline_steps:
    st.caption(f"当前共 {len(st.session_state.pipeline_steps)} 个步骤")

    for step_idx, step in enumerate(st.session_state.pipeline_steps):
        container = st.container(border=True)
        with container:
            cols = st.columns([3, 2, 2, 1])
            with cols[0]:
                proc_idx = 0
                if step.processor_name in all_processors:
                    proc_idx = all_processors.index(step.processor_name)
                new_processor = st.selectbox(
                    f"步骤 {step_idx + 1} 处理器",
                    options=all_processors,
                    index=proc_idx,
                    key=f"proc_{step.step_id}",
                    format_func=lambda n: f"{_processor_emoji(n)} {n}",
                )
                if new_processor != step.processor_name:
                    step.processor_name = new_processor
                    step.params = {}
            with cols[1]:
                named_sources = [ds.name for ds in st.session_state.pipeline_data_sources]
                prev_vars = list(named_sources)
                for ps in st.session_state.pipeline_steps[:step_idx]:
                    if ps.output_var not in prev_vars:
                        prev_vars.append(ps.output_var)

                is_multi = False
                if step.processor_name:
                    try:
                        proc_info = ProcessorRegistry.get_processor(step.processor_name)
                        is_multi = proc_info["input_type"] == "multi"
                    except KeyError:
                        pass

                if is_multi:
                    valid_sources = [s for s in step.input_sources if s in prev_vars]
                    if not valid_sources and prev_vars:
                        valid_sources = [prev_vars[0]]
                    new_sources = st.multiselect(
                        "输入源 (可多选)",
                        options=prev_vars,
                        default=valid_sources,
                        key=f"input_{step.step_id}",
                    )
                    step.input_sources = list(new_sources)
                else:
                    input_options = ["(无输入)"] + prev_vars
                    current = step.input_sources[0] if step.input_sources else "(无输入)"
                    inp_idx = input_options.index(current) if current in input_options else 0
                    selected = st.selectbox(
                        "输入源",
                        options=input_options,
                        index=inp_idx,
                        key=f"input_{step.step_id}",
                    )
                    if selected == "(无输入)":
                        step.input_sources = []
                    else:
                        step.input_sources = [selected]
            with cols[2]:
                step.output_var = st.text_input(
                    "输出变量名",
                    value=step.output_var,
                    key=f"var_{step.step_id}",
                )
            with cols[3]:
                st.write("操作")
                if step_idx > 0:
                    if st.button("▲", key=f"up_{step.step_id}", help="上移"):
                        move_step(step.step_id, -1)
                        st.rerun()
                if step_idx < len(st.session_state.pipeline_steps) - 1:
                    if st.button("▼", key=f"dn_{step.step_id}", help="下移"):
                        move_step(step.step_id, 1)
                        st.rerun()
                if st.button("✕", key=f"rm_{step.step_id}", help="删除", type="primary"):
                    remove_step(step.step_id)
                    st.rerun()

            if step.processor_name:
                with st.expander("⚙️ 参数配置", expanded=False):
                    step.params = render_processor_params_form(
                        step.processor_name,
                        step.step_id,
                        col_cache=st.session_state.get("pipeline_column_cache"),
                        current_values=step.params,
                    )
else:
    st.info("尚未添加任何步骤。点击下方按钮添加。")

add_col1, add_col2 = st.columns([1, 4])
with add_col1:
    if st.button("➕ 添加步骤", use_container_width=True):
        first_proc = all_processors[0] if all_processors else ""
        add_step(processor_name=first_proc)
        st.rerun()
with add_col2:
    if st.session_state.pipeline_steps:
        if st.button("\U0001f5d1️ 清空所有步骤", use_container_width=True):
            st.session_state.pipeline_steps = []
            clear_results()
            st.rerun()

st.divider()

# ==================== Section 3: 执行 ====================
st.header("Step 3: 执行流水线")

if st.session_state.pipeline_status == "error":
    st.error(f"上次执行失败: {st.session_state.pipeline_error}")

if st.session_state.pipeline_last_params:
    with st.expander("\U0001f50d 调试: 上次执行的参数", expanded=False):
        for _s in st.session_state.pipeline_last_params:
            st.markdown(f"**{_s['step']}** → `{_s['processor']}`")
            st.json(_s["params"])

context_keys = set(st.session_state.pipeline_data_context.keys())
all_sources_resolved = all(
    ds.name in context_keys for ds in st.session_state.pipeline_data_sources
)

exec_disabled = (
    not all_sources_resolved
    or len(st.session_state.pipeline_steps) == 0
    or st.session_state.pipeline_status == "running"
    or any(not s.processor_name for s in st.session_state.pipeline_steps)
)

if not exec_disabled:
    exec_col1, exec_col2 = st.columns([1, 3])
    with exec_col1:
        if st.button("\U0001f680 执行流水线", type="primary", use_container_width=True):
            st.session_state.pipeline_status = "running"
            st.session_state.pipeline_error = None
            st.rerun()
    with exec_col2:
        if st.session_state.pipeline_results:
            if st.button("\U0001f504 清除结果", use_container_width=True):
                clear_results()
                st.rerun()
else:
    reasons = []
    if not all_sources_resolved:
        reasons.append("存在未选择文件的数据源")
    if len(st.session_state.pipeline_steps) == 0:
        reasons.append("未添加流水线步骤")
    if any(not s.processor_name for s in st.session_state.pipeline_steps):
        reasons.append("存在未选择处理器的步骤")
    if reasons:
        st.warning(" - ".join(reasons))

# ==================== 执行逻辑 ====================
if st.session_state.pipeline_status == "running":
    progress_bar = st.progress(0, text="准备执行...")
    status_text = st.empty()

    def progress_cb(current, total, msg):
        progress_bar.progress(current / total, text=msg)
        status_text.text(msg)

    try:
        import copy, re

        def _substitute_globals(v, global_vars):
            """递归替换值中的 ${KEY} 为全局变量值"""
            if isinstance(v, str):
                return re.sub(
                    r'\$\{(\w+)\}',
                    lambda m: str(global_vars.get(m.group(1), m.group(0))),
                    v,
                )
            elif isinstance(v, dict):
                return {k: _substitute_globals(val, global_vars) for k, val in v.items()}
            elif isinstance(v, list):
                return [_substitute_globals(item, global_vars) for item in v]
            return v

        substituted_steps = []
        for s in st.session_state.pipeline_steps:
            s_copy = copy.deepcopy(s)
            s_copy.params = _substitute_globals(s_copy.params, st.session_state.pm_global_vars)
            substituted_steps.append(s_copy)

        st.session_state.pipeline_last_params = [
            {"step": _s.output_var, "processor": _s.processor_name, "params": _s.params}
            for _s in substituted_steps
        ]

        results = execute_pipeline(
            st.session_state.pipeline_data_context,
            substituted_steps,
            progress_callback=progress_cb,
            partial_results=st.session_state.pipeline_results,
        )
        st.session_state.pipeline_results = {r.output_var: r for r in results}
        st.session_state.pipeline_status = "done"

        for r in results:
            try:
                df_out = get_entry_dataframe(r.entry_id)
                if df_out is not None:
                    st.session_state.pipeline_column_cache[r.output_var] = list(df_out.columns)
            except Exception:
                pass

        st.rerun()
    except Exception as e:
        import traceback
        st.session_state.pipeline_status = "error"
        st.session_state.pipeline_error = traceback.format_exc()
        st.rerun()

# ==================== Section 4: 结果展示 ====================
if st.session_state.pipeline_results:
    st.divider()
    st.header("Step 4: 结果")

    results = st.session_state.pipeline_results
    steps = st.session_state.pipeline_steps
    results_list = [results[s.output_var] for s in steps if s.output_var in results]

    final_result = results_list[-1] if results_list else None

    if final_result and st.session_state.pipeline_status == "done":
        st.subheader("\U0001f3af 最终结果")
        render_file_by_extension(final_result.entry_path, final_result.entry_id)
        st.divider()

    re_cols = st.columns([1, 1, 4])
    with re_cols[0]:
        if st.button("\U0001f504 重新执行", type="primary", use_container_width=True):
            st.session_state.pipeline_status = "running"
            st.session_state.pipeline_error = None
            st.rerun()
    with re_cols[1]:
        if st.button("清除", use_container_width=True):
            clear_results()
            st.rerun()

    show_all = st.checkbox("展开所有步骤详情",
                           value=st.session_state.get("pipeline_expand_steps", True))
    st.session_state.pipeline_expand_steps = show_all

    for i, r in enumerate(results_list):
        step = steps[i] if i < len(steps) else None
        label = f"\U0001f4cc 步骤 {i+1}: {r.processor_name} → ID {r.entry_id} ({r.entry_type})"
        with st.expander(label, expanded=show_all):
            col_info, col_data = st.columns([1, 1])
            with col_info:
                st.markdown(f"**Processor:** `{r.processor_name}`")
                st.markdown(f"**输入源:** `{', '.join(step.input_sources) if step else '?'}`")
                st.markdown(f"**输出变量:** `{r.output_var}`")
                st.markdown(f"**Entry ID:** {r.entry_id}")
                st.markdown(f"**文件:** `{Path(r.entry_path).name}`")
                if r.parent_ids:
                    st.markdown(f"**父记录 IDs:** {r.parent_ids}")
                if step:
                    with st.expander("⚙️ 参数", expanded=False):
                        st.json(step.params)

            with col_data:
                r_path = Path(r.entry_path)
                if r_path.exists():
                    ext = r_path.suffix.lower()
                    preview_label = (
                        "\U0001f441️ 预览图表" if ext in (".png", ".jpg", ".jpeg", ".svg", ".pdf") else
                        "\U0001f441️ 预览数据" if ext in (".csv", ".parquet", ".xlsx", ".xls") else
                        "\U0001f441️ 预览文本" if ext == ".txt" else
                        "\U0001f441️ 预览文件"
                    )
                    if st.button(f"{preview_label} ID {r.entry_id}", key=f"prev_{r.output_var}"):
                        render_file_by_extension(r.entry_path, r.entry_id)
                else:
                    st.info("文件已不存在")

    if final_result:
        st.divider()
        st.subheader("\U0001f517 处理链追溯 / Provenance")
        tree = build_provenance_tree(final_result.entry_id)
        if tree:
            render_provenance_ui(tree)
