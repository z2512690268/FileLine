"""FileLine 可视化平台 - 共享工具函数"""

import os
import sys
import uuid
import inspect
import json
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import streamlit as st

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy.orm import selectinload
from core.base import get_session, experiment_manager
from core.models import DataEntry, Tag
from core.processing import ProcessorRegistry, DataProcessor
from core.storage import FileStorage

# ==================== Pipeline 构建相关数据结构 ====================

@dataclass
class PipelineStepConfig:
    """UI 中配置的一个流水线步骤"""
    step_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    processor_name: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    output_var: str = ""
    input_sources: List[str] = field(default_factory=lambda: ["initial"])


@dataclass
class PipelineExecutionResult:
    """单步执行结果"""
    output_var: str
    processor_name: str
    entry_id: int
    entry_path: str
    entry_type: str
    parent_ids: List[int] = field(default_factory=list)


@dataclass
class ProvenanceNode:
    """血缘关系树节点"""
    entry_id: int
    entry_type: str
    entry_path: str
    description: str
    tags: List[str] = field(default_factory=list)
    timestamp: Optional[datetime] = None
    children: List["ProvenanceNode"] = field(default_factory=list)


@dataclass
class EntryInfo:
    """安全的数据条目信息（脱离 ORM session 使用）"""
    id: int
    type: str
    path: str
    original_path: Optional[str]
    timestamp: datetime
    description: str
    tags: List[str]
    parent_ids: List[int] = field(default_factory=list)


@dataclass
class DataSourceConfig:
    """UI 中配置的一个命名数据源（路径模式匹配）"""
    name: str = ""
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    re_pattern: Optional[str] = None
    tags: List[str] = field(default_factory=list)


def _entry_to_info(entry: DataEntry) -> EntryInfo:
    """将 ORM DataEntry 转换为安全的 EntryInfo（需在 session 内调用）"""
    return EntryInfo(
        id=entry.id,
        type=entry.type,
        path=entry.path,
        original_path=entry.original_path,
        timestamp=entry.timestamp,
        description=entry.description,
        tags=[t.name for t in entry.tags],
        parent_ids=[p.id for p in entry.parents],
    )


def _get_processor_module_category(proc_name: str) -> str:
    """从 processor 函数定义所在模块目录推断其分类"""
    import re
    info = ProcessorRegistry._processors.get(proc_name)
    if not info:
        return "other"
    module = getattr(info["func"], "__module__", "") or ""
    m = re.search(r"processes\.(\w+)_operations", module)
    if m:
        return m.group(1)
    return "other"


def get_processor_list() -> Dict[str, Dict]:
    """获取所有注册的处理器信息，按模块目录类型分组"""
    processors = ProcessorRegistry._processors
    categorized: Dict[str, Dict] = {}
    for name in processors:
        cat = _get_processor_module_category(name)
        if cat not in categorized:
            categorized[cat] = {}
        categorized[cat][name] = processors[name]
    return categorized


def get_data_entries(
    entry_type: Optional[str] = None,
    tags: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[EntryInfo]:
    """获取数据条目，支持过滤，返回安全的 EntryInfo 列表"""
    with get_session() as session:
        query = session.query(DataEntry).options(
            selectinload(DataEntry.tags),
            selectinload(DataEntry.parents),
        )
        if entry_type:
            query = query.filter(DataEntry.type == entry_type)
        if tags:
            from sqlalchemy import or_
            tag_conditions = [Tag.name == t for t in tags]
            subq = (
                session.query(DataEntry.id)
                .join(DataEntry.tags)
                .filter(or_(*tag_conditions))
                .subquery()
            )
            query = query.filter(DataEntry.id.in_(subq))
        entries = (
            query.order_by(DataEntry.timestamp.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return [_entry_to_info(e) for e in entries]


def get_entry_dataframe(entry_id: int) -> Optional[pd.DataFrame]:
    """获取数据条目对应的 DataFrame"""
    with get_session() as session:
        entry = session.get(DataEntry, entry_id)
        if not entry:
            return None
        path = Path(entry.path)
        if not path.exists():
            return None
        ext = path.suffix.lower()
        try:
            if ext == ".csv":
                return pd.read_csv(path)
            elif ext == ".parquet":
                return pd.read_parquet(path)
            elif ext in (".xlsx", ".xls"):
                return pd.read_excel(path)
            elif ext == ".json":
                return pd.read_json(path)
        except Exception:
            return None
    return None


def run_processor(
    processor_name: str,
    input_ids: List[int],
    params: Dict[str, Any],
) -> Optional[Dict]:
    """运行处理器并返回结果信息"""
    try:
        storage = FileStorage()
        with get_session() as session:
            processor = DataProcessor(storage, session)
            result = processor.run(processor_name, input_ids, **params)
            session.commit()
            # 扁平化（支持 list/dict 多输出）
            if isinstance(result, dict):
                all_entries = [e for g in result.values() for e in g]
            elif isinstance(result, list):
                all_entries = result
            else:
                all_entries = [result]
            entry = all_entries[0]
            return {
                "id": entry.id,
                "path": str(entry.path),
                "type": entry.type,
                "timestamp": entry.timestamp,
            }
    except Exception as e:
        st.error(f"处理失败: {e}")
        return None


def get_available_tags() -> List[str]:
    """获取所有可用的标签"""
    with get_session() as session:
        tags = session.query(Tag).all()
        return [t.name for t in tags]


def format_file_size(path_str: str) -> str:
    """格式化文件大小"""
    try:
        size = os.path.getsize(path_str)
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"
    except (OSError, FileNotFoundError):
        return "?"


@st.cache_data(ttl=5)
def get_all_tags_cached() -> List[str]:
    """缓存的标签列表"""
    return get_available_tags()


@st.cache_data(ttl=5)
def get_stats_cached() -> Dict:
    """缓存的统计数据"""
    with get_session() as session:
        total = session.query(DataEntry).count()
        raw = session.query(DataEntry).filter(DataEntry.type == "raw").count()
        processed = (
            session.query(DataEntry).filter(DataEntry.type == "processed").count()
        )
        # DataProcessor.run() 始终设 type='processed'，故通过文件扩展名而非 type 字段检测
        from sqlalchemy import or_
        plot_extensions = ("%.pdf", "%.png", "%.jpg", "%.jpeg", "%.svg")
        plot = (
            session.query(DataEntry)
            .filter(DataEntry.type == "processed")
            .filter(or_(*(DataEntry.path.like(pat) for pat in plot_extensions)))
            .count()
        )
        return {"total": total, "raw": raw, "processed": processed, "plot": plot}


def render_file_by_extension(file_path: str, entry_id: int):
    """根据文件扩展名自动选择预览方式（图片/PDF/表格/文本/其他）"""
    if not hasattr(render_file_by_extension, "_dl_counter"):
        render_file_by_extension._dl_counter = 0
    render_file_by_extension._dl_counter += 1
    _dl_key = f"dl_{entry_id}_{render_file_by_extension._dl_counter}"
    path = Path(file_path)
    if not path.exists():
        st.error("文件已不存在")
        return

    ext = path.suffix.lower()
    file_name = path.name

    if ext in (".png", ".jpg", ".jpeg"):
        st.image(str(path), width="stretch")

    elif ext == ".svg":
        with open(path) as f:
            st.image(f.read(), width="stretch")

    elif ext == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(str(path)) as pdf:
                if pdf.pages:
                    img = pdf.pages[0].to_image(resolution=200)
                    st.image(img.original, width="stretch")
            with open(path, "rb") as f:
                st.download_button(
                    label="📥 下载 PDF", data=f, file_name=file_name,
                    mime="application/pdf", use_container_width=True,
                    key=_dl_key,
                )
        except ImportError:
            st.info("PDF 预览需安装 pdfplumber: `pip install pdfplumber`")
            with open(path, "rb") as f:
                st.download_button(
                    label="📥 下载 PDF", data=f, file_name=file_name, mime="application/pdf",
                    key=_dl_key,
                )

    elif ext in (".csv", ".parquet", ".xlsx", ".xls"):
        df = get_entry_dataframe(entry_id)
        if df is not None:
            st.dataframe(df.head(100), width="stretch")
            st.caption(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")
        else:
            st.info("无法以表格形式预览此文件")

    elif ext in (".txt", ".log"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            st.text(content[:5000])
            if len(content) > 5000:
                st.caption(f"... 显示前 5000 字符（共 {len(content)} 字符）")
        except Exception:
            st.info("无法预览此文本文件")

    else:
        st.info(f"文件格式 ({ext}) 暂不支持预览")

    st.caption(f"Entry ID: {entry_id} | 文件: `{file_path}` | 大小: {format_file_size(str(path))}")


# ==================== Pipeline Builder 工具函数 ====================

def get_processor_signature_info(processor_name: str) -> List[Dict]:
    """解析 processor 函数签名，返回参数元数据列表

    返回每项: {name, type, has_default, default, is_optional}
    """
    import processes.plot_operations  # noqa: ensure registered
    proc_info = ProcessorRegistry.get_processor(processor_name)
    func = proc_info["func"]
    sig = inspect.signature(func)
    params = []
    for p_name, p in sig.parameters.items():
        # 跳过框架管理参数
        if p_name in ("input_path", "input_paths", "output_path", "kwargs", "args"):
            continue

        type_hint = p.annotation if p.annotation is not inspect.Parameter.empty else None
        has_default = p.default is not inspect.Parameter.empty
        default_val = p.default if has_default else None
        is_optional = False

        # 判断 Optional[X] -> get_origin 判断 Union[..., None]
        origin = getattr(type_hint, "__origin__", None) if type_hint else None
        if origin is Union:
            args = type_hint.__args__
            if type(None) in args:
                is_optional = True
                non_none = [a for a in args if a is not type(None)]
                type_hint = non_none[0] if non_none else str

        params.append({
            "name": p_name,
            "type_hint": type_hint,
            "has_default": has_default,
            "default": default_val,
            "is_optional": is_optional,
        })
    return params


def render_processor_params_form(
    processor_name: str,
    step_id: str,
    col_cache: Optional[Dict[str, List[str]]] = None,
    current_values: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """根据 processor 签名自动渲染 Streamlit 参数表单

    返回用户填写的参数字典。
    col_cache: {"var_name": ["col1", "col2"]} 用于列名提示。
    current_values: 已有的参数值（如从 YAML 导入），用于预填表单。
    """
    param_infos = get_processor_signature_info(processor_name)
    values = {}
    for p in param_infos:
        key = f"{step_id}_{p['name']}"
        hint = p["type_hint"]
        default = current_values.get(p["name"], p["default"]) if current_values else p["default"]

        # Optional 类型: checkbox 开关 + 条件输入
        if p["is_optional"]:
            use_key = f"{key}_use"
            use = st.checkbox(f"启用 {p['name']}", value=default is not None, key=use_key)
            if not use:
                continue
            # checkbox 打开后，用非 None 默认值
            if default is None:
                if hint is str or hint is None:
                    default = ""
                elif hint is int:
                    default = 0
                elif hint is float:
                    default = 0.0
                elif hint is bool:
                    default = False
                elif hint is list or (hasattr(hint, "__origin__") and hint.__origin__ is list):
                    default = []
                elif hint is dict or (hasattr(hint, "__origin__") and hint.__origin__ is dict):
                    default = {}
                elif hint is tuple and hasattr(hint, "__args__") and len(hint.__args__) == 2:
                    default = (10.0, 6.0)
                else:
                    default = ""

        # 根据类型渲染不同 widget
        if hint is str or hint is None:
            v = st.text_input(p["name"], value=str(default) if default is not None else "", key=key)
            values[p["name"]] = v
        elif hint is int or hint is float:
            step_val = 1 if hint is int else 0.1
            fmt = "%d" if hint is int else "%.4f"
            # 确保 value 类型与 step 一致（YAML 可能把 float 值解析为 int）
            coerced = hint(default) if default is not None else hint(0)
            v = st.number_input(p["name"], value=coerced, step=step_val, format=fmt, key=key)
            values[p["name"]] = int(v) if hint is int else v
        elif hint is bool:
            v = st.checkbox(p["name"], value=bool(default), key=key)
            values[p["name"]] = v
        elif hint is tuple or (hasattr(hint, "__origin__") and hint.__origin__ is tuple):
            # Tuple[X, Y] 或 plain tuple -> 两个输入框
            if default is None:
                # default=None 表示使用函数默认值（如 xlim 自动范围），跳过避免覆盖
                pass
            else:
                t0, t1 = float, float
                if hasattr(hint, "__args__") and len(hint.__args__) == 2:
                    t0, t1 = hint.__args__[0], hint.__args__[1]
                if isinstance(default, (list, tuple)) and len(default) >= 2:
                    d0, d1 = default[0], default[1]
                else:
                    d0, d1 = 10.0, 6.0
                cols = st.columns(2)
                with cols[0]:
                    v0 = st.number_input(f"{p['name']}[0]", value=float(d0), step=0.5, key=f"{key}_0")
                with cols[1]:
                    v1 = st.number_input(f"{p['name']}[1]", value=float(d1), step=0.5, key=f"{key}_1")
                values[p["name"]] = (v0, v1)
        elif hint is list or (hasattr(hint, "__origin__") and hint.__origin__ is list):
            # List[str] / List[int] -> 逗号分隔文本
            item_type = str
            if hasattr(hint, "__args__") and hint.__args__:
                item_type = hint.__args__[0]
            default_str = ""
            if isinstance(default, (list, tuple)):
                default_str = ", ".join(str(d) for d in default)
            v = st.text_input(f"{p['name']} (逗号分隔)", value=default_str, key=key)
            if v.strip():
                items = [item.strip() for item in v.split(",") if item.strip()]
                if item_type is int:
                    items = [int(x) for x in items if x.lstrip("-").isdigit()]
                values[p["name"]] = items
            else:
                values[p["name"]] = []
        elif hint is dict or (hasattr(hint, "__origin__") and hint.__origin__ is dict):
            # Dict[str, str] -> JSON 文本
            default_json = ""
            if isinstance(default, dict):
                default_json = json.dumps(default, ensure_ascii=False)
            elif isinstance(default, str):
                default_json = default
            v = st.text_area(f"{p['name']} (JSON)", value=default_json, key=key)
            if v.strip():
                try:
                    values[p["name"]] = json.loads(v)
                except json.JSONDecodeError:
                    values[p["name"]] = v
            else:
                values[p["name"]] = {}
        elif hasattr(hint, "__origin__") and hint.__origin__ is Union:
            # Union[X, Y] — 根据包含的类型选择合适控件
            _union_args = [a for a in hint.__args__ if a is not type(None)]
            if any(a is dict or (hasattr(a, "__origin__") and a.__origin__ is dict) for a in _union_args):
                default_json = json.dumps(default, ensure_ascii=False) if isinstance(default, (list, dict)) else ""
                v = st.text_area(f"{p['name']} (JSON)", value=default_json, key=key)
                if v.strip():
                    try:
                        values[p["name"]] = json.loads(v)
                    except json.JSONDecodeError:
                        values[p["name"]] = v
            else:
                v = st.text_input(p["name"], value=str(default) if default is not None else "", key=key)
                values[p["name"]] = v
        else:
            # fallback: text_input — skip empty values to avoid overriding function defaults
            v = st.text_input(p["name"], value=str(default) if default is not None else "", key=key)
            if v:
                values[p["name"]] = v

    return values


def resolve_data_sources(
    data_sources: List["DataSourceConfig"],
) -> Dict[str, List[int]]:
    """将命名数据源解析为 {source_name: [entry_id, ...]}

    对每个 DataSourceConfig，用 PipelineRunner._load_initial_files() 执行
    glob 匹配、mtime 缓存检测、文件复制和 DataEntry 创建/重用。
    """
    from core.pipeline import PipelineRunner, InitialLoadConfig, IncludeSpec
    from core.storage import FileStorage
    from core.base import get_session

    storage = FileStorage()
    context: Dict[str, List[int]] = {}

    with get_session() as session:
        for ds in data_sources:
            if not ds.name or not ds.include_patterns:
                continue
            include_specs = []
            for p in ds.include_patterns:
                spec = IncludeSpec(path=p)
                if ds.re_pattern:
                    spec.re_pattern = ds.re_pattern
                if ds.tags:
                    spec.tags = ds.tags
                include_specs.append(spec)
            load_config = InitialLoadConfig(
                include_patterns=include_specs,
                exclude_patterns=ds.exclude_patterns if ds.exclude_patterns else None,
                data_type="raw",
                tags=ds.tags if ds.tags else None,
            )
            runner = PipelineRunner(storage, session)
            try:
                ids = runner._load_initial_files(load_config)
                context[ds.name] = ids
            except FileNotFoundError:
                context[ds.name] = []

    return context


def execute_pipeline(
    data_context: Dict[str, List[int]],
    steps: List["PipelineStepConfig"],
    progress_callback: Optional[Callable] = None,
    partial_results: Optional[Dict[str, "PipelineExecutionResult"]] = None,
) -> List["PipelineExecutionResult"]:
    """执行多步流水线，返回每步执行结果

    data_context: 由 resolve_data_sources() 返回的 {source_name: [entry_id, ...]}
    partial_results: 如果传入，每步完成后会同步更新该 dict，方便中间步骤异常时保留已有结果
    """
    storage = FileStorage()
    total = len(steps)
    context: Dict[str, List[int]] = dict(data_context)
    results = []

    with get_session() as session:
        processor = DataProcessor(storage, session)
        for i, step in enumerate(steps):
            if progress_callback:
                progress_callback(i + 1, total, f"执行: {step.processor_name} ({step.output_var})")

            input_ids = []
            for src in step.input_sources:
                input_ids.extend(context.get(src, []))

            proc_info = ProcessorRegistry.get_processor(step.processor_name)

            if proc_info["input_type"] == "none":
                run_input = None
            elif not input_ids and step.input_sources:
                srcs_str = ", ".join(step.input_sources)
                raise ValueError(f"步骤 '{step.output_var}': 所有输入源 '{srcs_str}' 均为空")
            elif proc_info["input_type"] == "multi":
                run_input = input_ids
            else:
                run_input = input_ids[0]

            result = processor.run(step.processor_name, run_input, **step.params)

            if isinstance(result, list):
                entries = result
                context[step.output_var] = [e.id for e in entries]
            else:
                entries = [result]
                context[step.output_var] = [result.id]

            session.commit()

            for entry in entries:
                step_result = PipelineExecutionResult(
                    output_var=step.output_var,
                    processor_name=step.processor_name,
                    entry_id=entry.id,
                    entry_path=str(entry.path),
                    entry_type=entry.type,
                    parent_ids=[p.id for p in entry.parents],
                )
                results.append(step_result)

            if partial_results is not None:
                partial_results[step.output_var] = entries[0]

    if progress_callback:
        progress_callback(total, total, "完成")
    return results


# ==================== 血缘追溯工具函数 ====================

def build_provenance_tree(
    entry_id: int,
    max_depth: int = 15,
    visited: Optional[set] = None,
) -> Optional["ProvenanceNode"]:
    """递归构建血缘关系树（沿 DataEntry.parents 向上遍历）"""
    if max_depth <= 0:
        return None
    if visited is None:
        visited = set()
    if entry_id in visited:
        return None
    visited.add(entry_id)

    with get_session() as session:
        entry = (
            session.query(DataEntry)
            .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
            .filter(DataEntry.id == entry_id)
            .first()
        )
        if entry is None:
            return None

        node = ProvenanceNode(
            entry_id=entry.id,
            entry_type=entry.type,
            entry_path=str(entry.path),
            description=entry.description,
            tags=[t.name for t in entry.tags],
            timestamp=entry.timestamp,
        )
        for parent in entry.parents:
            child = build_provenance_tree(parent.id, max_depth - 1, visited)
            if child:
                node.children.append(child)
    return node


def _extract_processor_name(description: str) -> str:
    """从 DataEntry.description 中提取 processor 名称"""
    import re
    m = re.search(r"Processed by (\S+)", description)
    return m.group(1) if m else "unknown"


def render_provenance_ui(node: Optional["ProvenanceNode"], depth: int = 0):
    """渲染血缘树（Streamlit expander 嵌套）"""
    if node is None:
        return

    proc_name = _extract_processor_name(node.description) if node.description else "raw"
    label = f"[{node.entry_type.upper()}] ID {node.entry_id} — {proc_name}"

    if node.children:
        with st.expander(label, expanded=depth < 2):
            st.caption(f"路径: `{node.entry_path}`")
            if node.description:
                st.caption(f"描述: {node.description[:120]}")
            if node.tags:
                st.caption(f"标签: {', '.join(node.tags[:8])}")

            # 预览按钮（数据表格或 PDF/图片）
            show_key = f"prov_show_{node.entry_id}_{depth}"
            ext = (Path(node.entry_path).suffix.lower() if node.entry_path else "")
            btn_label = "👁️ 预览" if ext in (".pdf", ".png", ".jpg", ".jpeg", ".svg") else f"👁️ 预览 ID {node.entry_id} 数据"
            if st.button(btn_label, key=show_key):
                if ext in (".pdf", ".png", ".jpg", ".jpeg", ".svg"):
                    render_file_by_extension(node.entry_path, node.entry_id)
                else:
                    df = get_entry_dataframe(node.entry_id)
                    if df is not None:
                        st.dataframe(df.head(50), width="stretch")
                        st.caption(f"Shape: {df.shape}")
                    else:
                        st.info("无法预览此文件")

            for child in node.children:
                render_provenance_ui(child, depth + 1)
    else:
        st.markdown(f"- {label}")
        show_key = f"prov_show_{node.entry_id}_{depth}"
        ext = (Path(node.entry_path).suffix.lower() if node.entry_path else "")
        btn_label = "👁️ 预览" if ext in (".pdf", ".png", ".jpg", ".jpeg", ".svg") else "👁️ 数据"
        if st.button(btn_label, key=show_key):
            if ext in (".pdf", ".png", ".jpg", ".jpeg", ".svg"):
                render_file_by_extension(node.entry_path, node.entry_id)
            else:
                df = get_entry_dataframe(node.entry_id)
                if df is not None:
                    st.dataframe(df.head(20), width="stretch")
