"""FastAPI backend for the React FileLine workspace."""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
import shutil
import tempfile
import inspect
import threading
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "fileline_matplotlib"))

import pandas as pd
import yaml
from fastapi import FastAPI, File as FastAPIFile, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.orm import selectinload

import processes  # noqa: F401 - register built-in processors
from core.base import experiment_manager, get_session, init_db
from core.global_sets import (
    declared_global_variables,
    duplicate_global_set,
    global_set_affected,
    legacy_global_candidates,
    list_global_sets,
    load_global_values,
    parse_global_set_text,
    pipeline_global_set,
    pipeline_required_variables,
    read_global_set,
    resolve_text,
    save_global_set,
    used_variables,
)
from core.models import DataEntry, ExportMeta, PipelineVersion, StepCache
from core.processing import ProcessorRegistry, load_processors_from_dir
from core.storage import FileStorage

PROJECT_ROOT = Path(__file__).resolve().parent
PIPELINES_ROOT = PROJECT_ROOT / "FileLine-Pipelines"
WEB_DIST = PROJECT_ROOT / "web" / "dist"
EXPERIMENT_CONTEXT_LOCK = threading.RLock()

app = FastAPI(title="FileLine API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PipelineSaveRequest(BaseModel):
    yaml_text: str


class PipelineRenameRequest(BaseModel):
    name: str
    experiment: str | None = None


class PipelineCloneRequest(BaseModel):
    source_path: str
    name: str
    experiment: str | None = None
    data_entry_id: int | None = None
    source_spec: dict[str, Any] | None = None


class AutoChartRequest(BaseModel):
    entry_id: int
    name: str
    goal: str = "compare"
    chart_type: str | None = None
    title: str | None = None
    x_col: str | None = None
    y_col: str | None = None
    group_col: str | None = None


class ProcessorDraftRequest(BaseModel):
    filename: str = "custom_processor.py"
    code: str


class GlobalSetSaveRequest(BaseModel):
    text: str
    description: str = ""
    scope: str = "experiment"


class GlobalSetDuplicateRequest(BaseModel):
    target: str


class GlobalSetRegenerateRequest(BaseModel):
    source_mode: str = "version"
    force_fresh: bool = False
    limit: int | None = None


class PipelineResolveRequest(BaseModel):
    global_set: str | None = None
    overrides: dict[str, str] | None = None


def _parse_run_feedback(stdout: str, stderr: str) -> dict[str, Any]:
    combined_lines = [line.strip() for line in f"{stdout}\n{stderr}".splitlines() if line.strip()]
    failed_stage = ""
    failed_processor = ""
    for line in combined_lines:
        if line.startswith("Pipeline Step:"):
            failed_stage = line
            try:
                failed_processor = line.split("Pipeline Step:", 1)[1].split(",", 1)[0].strip()
            except Exception:
                failed_processor = ""

    error_type = ""
    message = ""
    for line in reversed(combined_lines):
        if ":" in line and any(token in line for token in ("Error", "Exception", "ValueError", "FileNotFoundError", "RuntimeError", "KeyError")):
            error_type = line.split(":", 1)[0].strip()
            message = line.split(":", 1)[1].strip()
            break
    if not message and combined_lines:
        message = combined_lines[-1]

    summary = message or "Pipeline execution failed."
    hint = ""
    lowered = summary.lower()
    if "缺少必要列" in summary or "missing" in lowered and "col" in lowered:
        hint = "The selected data is missing one or more columns required by this processor."
    elif "未找到匹配文件" in summary or "no such file" in lowered:
        hint = "Check the source include pattern and whether the current experiment has matching raw data."
    elif "未注册的处理器" in summary or "keyerror" in lowered:
        hint = "This pipeline refers to a processor that is not currently registered for the experiment."
    elif "cache" in lowered:
        hint = "Try a fresh run to bypass cached outputs."

    return {
        "summary": summary,
        "errorType": error_type or "ExecutionError",
        "failedStage": failed_stage,
        "failedProcessor": failed_processor,
        "hint": hint,
    }


@contextmanager
def experiment_context(name: str):
    with EXPERIMENT_CONTEXT_LOCK:
        previous = experiment_manager.current_experiment
        try:
            experiment_manager.set_current(name, persist=False)
            init_db()
            yield
        finally:
            experiment_manager.current_experiment = previous


def _safe_rel(path: Path, root: Path) -> str:
    return str(path.resolve().relative_to(root.resolve()))


def _pipeline_path_from_rel(pipeline_path: str) -> Path:
    path = (PIPELINES_ROOT / pipeline_path).resolve()
    if (
        not path.exists()
        or path.suffix.lower() not in {".yaml", ".yml"}
        or PIPELINES_ROOT.resolve() not in path.parents
    ):
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return path


def _slugify_name(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip()).strip("._")
    return slug or "studio_pipeline"


def _experiment_pipeline_dir(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip())
    if safe in {"", ".", ".."}:
        return "experiment"
    return safe


def _entry_payload(entry: DataEntry) -> dict[str, Any]:
    path = Path(entry.path) if entry.path else None
    return {
        "id": entry.id,
        "type": entry.type,
        "path": str(entry.path or ""),
        "originalPath": entry.original_path or "",
        "fileName": path.name if path else "",
        "extension": path.suffix.lower() if path else "",
        "size": path.stat().st_size if path and path.exists() else None,
        "timestamp": entry.timestamp.isoformat() if entry.timestamp else "",
        "description": entry.description or "",
        "tags": [tag.name for tag in entry.tags],
        "parentIds": [parent.id for parent in entry.parents],
    }


def _source_payload(entry: DataEntry) -> dict[str, Any]:
    original = entry.original_path or entry.path or ""
    source_kind = "cached"
    if re.match(r"^[^@\s]+@[^:\s]+:\d+:", original):
        source_kind = "remote"
    elif re.match(r"^[^@\s]+@[^:\s]+:", original):
        source_kind = "remote"
    elif original and Path(original).is_absolute():
        source_kind = "local"
    return {
        "entry": _entry_payload(entry),
        "kind": source_kind,
        "path": original,
        "directory": str(Path(original).parent) if original and source_kind != "remote" else "",
        "pattern": original,
        "label": Path(original).name if original else Path(entry.path or "").name,
    }


PROCESSOR_TEMPLATES: list[dict[str, str]] = [
    {
        "id": "single_table_to_csv",
        "name": "Single input table",
        "description": "Read one table-like input and write one CSV output.",
        "filename": "custom_table_processor.py",
        "code": """from pathlib import Path
import pandas as pd

from core.processing import InputPath, ProcessorRegistry


@ProcessorRegistry.register(input_type=\"single\", output_ext=\".csv\")
def custom_table_processor(input_path: InputPath, output_path: Path, value_col: str = \"value\"):
    \"\"\"Create a cleaned CSV from one input file.\"\"\"
    if input_path.path.suffix == \".parquet\":
        df = pd.read_parquet(input_path.path)
    else:
        df = pd.read_csv(input_path.path)

    if value_col not in df.columns:
        raise ValueError(f\"Missing required column: {value_col}\")

    df[[value_col]].to_csv(output_path, index=False)
    return [\"custom\", \"table\"]
""",
    },
    {
        "id": "single_plot_pdf",
        "name": "Single input figure",
        "description": "Read one table and write one PDF figure.",
        "filename": "custom_plot_processor.py",
        "code": """from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from core.processing import InputPath, ProcessorRegistry


@ProcessorRegistry.register(input_type=\"single\", output_ext=\".pdf\")
def custom_plot_processor(input_path: InputPath, output_path: Path, x_col: str, y_col: str, title: str = \"Custom figure\"):
    \"\"\"Create a PDF figure from one table.\"\"\"
    df = pd.read_parquet(input_path.path) if input_path.path.suffix == \".parquet\" else pd.read_csv(input_path.path)
    for column in (x_col, y_col):
        if column not in df.columns:
            raise ValueError(f\"Missing required column: {column}\")

    fig, ax = plt.subplots(figsize=(6, 4), dpi=300)
    ax.plot(df[x_col], df[y_col])
    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
    return [\"custom\", \"figure\"]
""",
    },
    {
        "id": "multi_output_files",
        "name": "Multiple outputs",
        "description": "Create several named files from one input.",
        "filename": "custom_multi_processor.py",
        "code": """from pathlib import Path
import pandas as pd

from core.processing import InputPath, ProcessorRegistry


@ProcessorRegistry.register(input_type=\"single\", output_type=\"multi\")
def custom_multi_processor(input_path: InputPath, output_dir: Path, group_col: str):
    \"\"\"Split one table into multiple CSV files.\"\"\"
    df = pd.read_parquet(input_path.path) if input_path.path.suffix == \".parquet\" else pd.read_csv(input_path.path)
    if group_col not in df.columns:
        raise ValueError(f\"Missing required column: {group_col}\")

    outputs = []
    for group_name, sub_df in df.groupby(group_col):
        safe = str(group_name).replace(\"/\", \"_\").replace(\" \", \"_\")
        filename = f\"{safe}.csv\"
        sub_df.to_csv(output_dir / filename, index=False)
        outputs.append((filename, [\"custom\", str(group_name)]))
    return outputs
""",
    },
]


def _literal_node_value(node: ast.AST, fallback: Any = None) -> Any:
    try:
        return ast.literal_eval(node)
    except Exception:
        return fallback


def _register_decorator_info(decorator: ast.AST) -> dict[str, Any] | None:
    if not isinstance(decorator, ast.Call):
        return None
    func = decorator.func
    is_register = False
    if isinstance(func, ast.Attribute) and func.attr == "register":
        is_register = True
    elif isinstance(func, ast.Name) and func.id == "register":
        is_register = True
    if not is_register:
        return None
    info: dict[str, Any] = {}
    if decorator.args:
        first = _literal_node_value(decorator.args[0])
        if isinstance(first, str):
            info["name"] = first
    for kw in decorator.keywords:
        if kw.arg:
            info[kw.arg] = _literal_node_value(kw.value)
    return info


def _validate_processor_code(code: str, filename: str = "custom_processor.py") -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    processors: list[dict[str, Any]] = []
    safe_filename = _slugify_name(Path(filename or "custom_processor.py").stem) + ".py"
    if not code.strip():
        errors.append("Processor code is empty.")
        return {"ok": False, "filename": safe_filename, "errors": errors, "warnings": warnings, "processors": processors}
    try:
        tree = ast.parse(code, filename=safe_filename)
    except SyntaxError as exc:
        location = f"line {exc.lineno}" if exc.lineno else "unknown line"
        errors.append(f"Python syntax error at {location}: {exc.msg}")
        return {"ok": False, "filename": safe_filename, "errors": errors, "warnings": warnings, "processors": processors}

    imports_core = False
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "core.processing":
            names = {alias.name for alias in node.names}
            if "ProcessorRegistry" in names:
                imports_core = True
        if isinstance(node, ast.Import):
            if any(alias.name == "core.processing" for alias in node.names):
                imports_core = True
    if not imports_core:
        warnings.append("Import ProcessorRegistry from core.processing so FileLine can register the function.")

    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        register_info = None
        for decorator in node.decorator_list:
            register_info = _register_decorator_info(decorator)
            if register_info is not None:
                break
        if register_info is None:
            continue

        input_type = str(register_info.get("input_type") or "single")
        output_type = str(register_info.get("output_type") or "single")
        output_ext = str(register_info.get("output_ext") or ".txt")
        registered_name = str(register_info.get("name") or node.name)
        args = [arg.arg for arg in node.args.args]
        param_names = [arg for arg in args if arg not in {"input_path", "input_paths", "output_path", "output_dir"}]
        proc_errors: list[str] = []
        proc_warnings: list[str] = []

        if input_type not in {"single", "multi", "none"}:
            proc_errors.append("input_type must be one of: single, multi, none.")
        if output_type not in {"single", "multi"}:
            proc_errors.append("output_type must be one of: single, multi.")
        if output_type == "single" and "output_path" not in args:
            proc_errors.append("single-output processors must accept output_path.")
        if output_type == "multi" and "output_dir" not in args:
            proc_errors.append("multi-output processors must accept output_dir.")
        if input_type == "single" and not any(arg in args for arg in ("input_path", "input_paths")):
            proc_errors.append("single-input processors should accept input_path as the first data argument.")
        if input_type == "multi" and "input_paths" not in args:
            proc_warnings.append("multi-input processors normally accept input_paths.")
        if input_type == "none" and any(arg in args for arg in ("input_path", "input_paths")):
            proc_warnings.append("input_type='none' processors receive no input_path/input_paths from FileLine.")
        if output_type == "single" and output_ext != ProcessorRegistry.SAME_OUTPUT_EXT and not output_ext.startswith("."):
            proc_errors.append("output_ext should start with '.', for example .csv or .pdf.")
        if ast.get_docstring(node) is None:
            proc_warnings.append("Add a one-line docstring; FileLine shows it as the processor description.")
        if not any(isinstance(child, ast.Return) for child in ast.walk(node)):
            proc_warnings.append("Return tags for generated entries, for example ['custom', 'figure'].")
        if registered_name in ProcessorRegistry._processors:
            proc_warnings.append(f"Processor name '{registered_name}' already exists and may override an existing processor in this experiment.")

        processors.append({
            "name": registered_name,
            "function": node.name,
            "inputType": input_type,
            "outputType": output_type,
            "outputExt": output_ext,
            "params": param_names,
            "errors": proc_errors,
            "warnings": proc_warnings,
        })
        errors.extend(f"{registered_name}: {message}" for message in proc_errors)
        warnings.extend(f"{registered_name}: {message}" for message in proc_warnings)

    if not processors:
        errors.append("No @ProcessorRegistry.register(...) function was found.")
    return {"ok": not errors, "filename": safe_filename, "errors": errors, "warnings": warnings, "processors": processors}


def _processor_description(name: str, info: dict[str, Any]) -> str:
    doc = inspect.getdoc(info.get("func"))
    if doc:
        return doc.splitlines()[0].strip()
    words = name.replace("_", " ")
    if name.startswith("parse"):
        return f"Parse source files into a structured table for later plotting: {words}."
    if name.startswith("plot"):
        return f"Create a figure from prepared table data: {words}."
    if "filter" in name:
        return f"Filter rows or columns before plotting: {words}."
    if "group" in name:
        return f"Group and aggregate table data: {words}."
    if "merge" in name or "concat" in name:
        return f"Combine multiple inputs into one dataset: {words}."
    if "timeline" in name:
        return f"Transform timeline-style data for visualization: {words}."
    return f"Process data as part of a FileLine pipeline: {words}."


def _read_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    if ext == ".tsv":
        return pd.read_csv(path, sep="\t")
    if ext == ".parquet":
        return pd.read_parquet(path)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if ext in {".json", ".jsonl"}:
        return pd.read_json(path, lines=ext == ".jsonl")
    raise ValueError(f"Unsupported table format: {ext}")


def _is_numeric(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series)


def _is_datetime_like(series: pd.Series, name: str) -> bool:
    lowered = name.lower()
    if any(token in lowered for token in ("time", "date", "timestamp")):
        return True
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    if series.dtype == object:
        sample = series.dropna().astype(str).head(20)
        if sample.empty:
            return False
        try:
            parsed = pd.to_datetime(sample, errors="coerce")
            return parsed.notna().mean() >= 0.7
        except Exception:
            return False
    return False


def _column_profile(df: pd.DataFrame) -> list[dict[str, Any]]:
    profiles = []
    sample_size = max(len(df), 1)
    for column in df.columns:
        series = df[column]
        unique_count = int(series.nunique(dropna=True))
        kind = "numeric" if _is_numeric(series) else "datetime" if _is_datetime_like(series, str(column)) else "category"
        if kind == "category" and unique_count > max(24, sample_size // 2):
            kind = "text"
        profiles.append(
            {
                "name": str(column),
                "kind": kind,
                "uniqueCount": unique_count,
                "sample": [str(value) for value in series.dropna().head(3).tolist()],
            }
        )
    return profiles


def _pick_metric(columns: list[str]) -> str | None:
    preferred = ("throughput", "accuracy", "reward", "loss", "score", "value", "duration", "latency", "time", "util", "memory")
    lowered = {column: column.lower() for column in columns}
    for token in preferred:
        for column in columns:
            if token in lowered[column]:
                return column
    return columns[0] if columns else None


def _pick_time(columns: list[str]) -> str | None:
    preferred = ("iteration", "step", "epoch", "time", "timestamp", "date", "x")
    lowered = {column: column.lower() for column in columns}
    for token in preferred:
        for column in columns:
            if token in lowered[column]:
                return column
    return columns[0] if columns else None


def _infer_chart_plan(entry: DataEntry, goal: str) -> dict[str, Any]:
    path = Path(entry.path)
    df = _read_table(path)
    df = df.dropna(axis=1, how="all")
    profiles = _column_profile(df)
    profile_by_name = {item["name"]: item for item in profiles}
    numeric_cols = [item["name"] for item in profiles if item["kind"] == "numeric"]
    categorical_cols = [item["name"] for item in profiles if item["kind"] == "category"]
    datetime_cols = [item["name"] for item in profiles if item["kind"] == "datetime"]
    timeline_starts = [name for name in df.columns if any(token in str(name).lower() for token in ("start", "begin"))]
    timeline_ends = [name for name in df.columns if any(token in str(name).lower() for token in ("end", "finish", "stop"))]
    base = {
      "supported": False,
      "reason": "FileLine could not infer a reliable chart from this source yet.",
      "chartType": "unsupported",
      "processor": None,
      "confidence": "low",
      "columns": profiles,
      "shape": [int(df.shape[0]), int(df.shape[1])],
      "sampleRows": df.head(8).where(pd.notnull(df), None).to_dict(orient="records"),
      "params": {},
      "title": Path(entry.original_path or entry.path or "chart").stem.replace("_", " ").title(),
    }

    if goal == "timeline":
        if timeline_starts and timeline_ends and len(categorical_cols) >= 1:
            category_col = categorical_cols[0]
            sub_category_col = categorical_cols[1] if len(categorical_cols) > 1 else categorical_cols[0]
            base.update({
                "supported": True,
                "chartType": "timeline",
                "processor": "plot_timeline_hbar",
                "confidence": "medium",
                "reason": "Detected start/end columns and category columns suitable for a timeline chart.",
                "params": {
                    "category_col": category_col,
                    "sub_category_col": sub_category_col,
                    "start_col": timeline_starts[0],
                    "end_col": timeline_ends[0],
                    "title": base["title"],
                    "xlabel": "Time",
                },
            })
            return base
        if categorical_cols and numeric_cols:
            metric = _pick_metric(numeric_cols)
            base.update({
                "supported": True,
                "chartType": "horizontal_bar",
                "processor": "plot_horizontal_bar",
                "confidence": "medium",
                "reason": "Timeline-specific columns were not found, so FileLine fell back to a breakdown-style horizontal bar chart.",
                "params": {
                    "y_col": categorical_cols[0],
                    "value_col": metric,
                    "title": base["title"],
                    "xlabel": metric.replace("_", " ").title(),
                    "ylabel": categorical_cols[0].replace("_", " ").title(),
                },
            })
            return base
        return base

    if goal == "trend":
        x_candidates = datetime_cols + [name for name in numeric_cols if profile_by_name[name]["uniqueCount"] > 3]
        x_col = _pick_time(x_candidates)
        y_candidates = [name for name in numeric_cols if name != x_col]
        y_col = _pick_metric(y_candidates)
        if x_col and y_col:
            tag_col = next((name for name in categorical_cols if 1 < profile_by_name[name]["uniqueCount"] <= 12), None)
            base.update({
                "supported": True,
                "chartType": "line",
                "processor": "plot_line",
                "confidence": "high" if tag_col else "medium",
                "reason": "Detected an ordered x-axis column and a numeric metric suitable for a line chart.",
                "params": {
                    "time_col": x_col,
                    "value_col": y_col,
                    "tag_col": tag_col,
                    "title": base["title"],
                    "xlabel": x_col.replace("_", " ").title(),
                    "ylabel": y_col.replace("_", " ").title(),
                },
            })
            return base

    if goal in {"compare", "breakdown"} and numeric_cols:
        value_col = _pick_metric(numeric_cols)
        compact_categories = [name for name in categorical_cols if 1 < profile_by_name[name]["uniqueCount"] <= 20]
        if len(compact_categories) >= 2:
            base.update({
                "supported": True,
                "chartType": "grouped_bar",
                "processor": "plot_grouped_bar",
                "confidence": "high",
                "reason": "Detected two grouping columns plus a numeric metric, which fits a grouped bar chart.",
                "params": {
                    "main_group_col": compact_categories[0],
                    "sub_group_col": compact_categories[1],
                    "value_col": value_col,
                    "title": base["title"],
                    "xlabel": compact_categories[0].replace("_", " ").title(),
                    "ylabel": value_col.replace("_", " ").title(),
                },
            })
            return base
        if compact_categories:
            chart_type = "horizontal_bar" if goal == "breakdown" else "bar"
            processor = "plot_horizontal_bar" if goal == "breakdown" else "plot_bar"
            params = {
                "title": base["title"],
                "ylabel": value_col.replace("_", " ").title(),
            }
            if chart_type == "horizontal_bar":
                params.update({"y_col": compact_categories[0], "value_col": value_col, "xlabel": value_col.replace("_", " ").title()})
            else:
                params.update({"x_col": compact_categories[0], "value_col": value_col, "xlabel": compact_categories[0].replace("_", " ").title()})
            base.update({
                "supported": True,
                "chartType": chart_type,
                "processor": processor,
                "confidence": "medium",
                "reason": "Detected one grouping column and one numeric metric suitable for a simple comparison chart.",
                "params": params,
            })
            return base

    if len(numeric_cols) >= 2:
        x_col = _pick_time(numeric_cols)
        y_col = _pick_metric([name for name in numeric_cols if name != x_col])
        if x_col and y_col:
            base.update({
                "supported": True,
                "chartType": "line",
                "processor": "plot_line",
                "confidence": "low",
                "reason": "Fell back to a numeric-vs-numeric line chart suggestion.",
                "params": {
                    "time_col": x_col,
                    "value_col": y_col,
                    "title": base["title"],
                    "xlabel": x_col.replace("_", " ").title(),
                    "ylabel": y_col.replace("_", " ").title(),
                },
            })
    return base


def _auto_chart_yaml(name: str, entry: DataEntry, plan: dict[str, Any]) -> str:
    export_name = f"{_slugify_name(name)}.pdf"
    source_path = entry.path or entry.original_path
    config = {
        "name": name,
        "description": f"Auto-generated from {Path(entry.original_path or entry.path or name).name}",
        "initial_load": {
            "include": [{"path": source_path, "source": "initial", "tags": ["studio_input"]}],
            "type": "raw",
            "global_tags": ["studio", "auto_chart"],
        },
        "steps": [
            {
                "processor": plan["processor"],
                "inputs": "initial",
                "output": _slugify_name(name),
                "params": {**plan["params"], "figsize": [6, 4], "grid": True, "dpi": 300},
            }
        ],
        "final_output": [{"name": _slugify_name(name), "export": export_name}],
    }
    if config["steps"][0]["params"].get("tag_col") is None:
        config["steps"][0]["params"].pop("tag_col", None)
    return yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _customized_chart_plan(plan: dict[str, Any], payload: AutoChartRequest) -> dict[str, Any]:
    chart_type = (payload.chart_type or plan.get("chartType") or "auto").strip()
    if chart_type == "auto":
        chart_type = str(plan.get("chartType") or "line")
    title = (payload.title or plan.get("title") or "").strip() or str(plan.get("title") or "Chart")
    x_col = (payload.x_col or "").strip()
    y_col = (payload.y_col or "").strip()
    group_col = (payload.group_col or "").strip()
    base_params = dict(plan.get("params") or {})

    if chart_type == "line" and x_col and y_col:
        params = {
            "time_col": x_col,
            "value_col": y_col,
            "title": title,
            "xlabel": x_col.replace("_", " ").title(),
            "ylabel": y_col.replace("_", " ").title(),
        }
        if group_col:
            params["tag_col"] = group_col
        return {**plan, "chartType": "line", "processor": "plot_line", "params": params, "title": title}

    if chart_type == "bar" and x_col and y_col:
        return {
            **plan,
            "chartType": "bar",
            "processor": "plot_bar",
            "params": {
                "x_col": x_col,
                "value_col": y_col,
                "title": title,
                "xlabel": x_col.replace("_", " ").title(),
                "ylabel": y_col.replace("_", " ").title(),
            },
            "title": title,
        }

    if chart_type == "grouped_bar" and x_col and y_col:
        params = {
            "main_group_col": x_col,
            "sub_group_col": group_col or x_col,
            "value_col": y_col,
            "title": title,
            "xlabel": x_col.replace("_", " ").title(),
            "ylabel": y_col.replace("_", " ").title(),
        }
        return {**plan, "chartType": "grouped_bar", "processor": "plot_grouped_bar", "params": params, "title": title}

    if chart_type == "horizontal_bar" and x_col and y_col:
        return {
            **plan,
            "chartType": "horizontal_bar",
            "processor": "plot_horizontal_bar",
            "params": {
                "y_col": x_col,
                "value_col": y_col,
                "title": title,
                "xlabel": y_col.replace("_", " ").title(),
                "ylabel": x_col.replace("_", " ").title(),
            },
            "title": title,
        }

    base_params["title"] = title
    if x_col:
        for key in ("x_col", "time_col", "main_group_col", "y_col", "category_col"):
            if key in base_params:
                base_params[key] = x_col
                break
    if y_col and "value_col" in base_params:
        base_params["value_col"] = y_col
    if group_col:
        for key in ("tag_col", "sub_group_col", "sub_category_col"):
            if key in base_params:
                base_params[key] = group_col
                break
    return {**plan, "params": base_params, "title": title}


def _experiment_stats(name: str) -> dict[str, Any]:
    with experiment_context(name), get_session() as session:
        def count_or_zero(fn):
            try:
                return fn()
            except Exception:
                return 0

        plot_patterns = ("%.pdf", "%.png", "%.jpg", "%.jpeg", "%.svg")
        total = count_or_zero(lambda: session.query(DataEntry).count())
        raw = count_or_zero(lambda: session.query(DataEntry).filter(DataEntry.type == "raw").count())
        processed = count_or_zero(lambda: session.query(DataEntry).filter(DataEntry.type == "processed").count())
        plots = count_or_zero(lambda: (
            session.query(DataEntry)
            .filter(DataEntry.type == "processed")
            .filter(or_(*(DataEntry.path.like(pattern) for pattern in plot_patterns)))
            .count()
        ))
        exports = count_or_zero(lambda: session.query(ExportMeta).count())
        cache_rows = count_or_zero(lambda: session.query(StepCache).count())
        versions = count_or_zero(lambda: session.query(PipelineVersion).count())
        return {
            "total": total,
            "raw": raw,
            "processed": processed,
            "plots": plots,
            "exports": exports,
            "cacheRows": cache_rows,
            "versions": versions,
        }


def _load_global_file(yaml_path: Path) -> dict[str, str]:
    global_path = _find_global_file(yaml_path)
    if not global_path:
        return {}
    values, _metadata = parse_global_set_text(global_path.read_text(encoding="utf-8"))
    return values


def _find_global_file(yaml_path: Path) -> Path | None:
    for candidate in legacy_global_candidates(yaml_path):
        if candidate.exists():
            return candidate
    return None


def _pipeline_summary(path: Path) -> dict[str, Any]:
    try:
        config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        config = {}
    steps = config.get("steps") or []
    includes = (config.get("initial_load") or {}).get("include") or []
    final_outputs = config.get("final_output") or []
    processors = [step.get("processor") for step in steps if isinstance(step, dict)]
    return {
        "path": _safe_rel(path, PIPELINES_ROOT),
        "name": config.get("name") or path.stem,
        "description": config.get("description") or "",
        "group": path.parent.name if path.parent != PIPELINES_ROOT else "root",
        "stepCount": len(steps),
        "sourceCount": len(includes),
        "exportCount": len(final_outputs),
        "processors": processors,
    }


def _pipeline_graph(config: dict[str, Any]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    var_to_node: dict[str, str] = {}

    includes = (config.get("initial_load") or {}).get("include") or []
    include_groups: dict[str, list[dict[str, Any]]] = {}
    for include in includes:
        if not isinstance(include, dict):
            continue
        source_name = str(include.get("source", "initial"))
        include_groups.setdefault(source_name, []).append(include)
    if not include_groups:
        include_groups["initial"] = []
    for source_name, source_includes in include_groups.items():
        source_id = f"source:{source_name}"
        nodes.append(
            {
                "id": source_id,
                "type": "source",
                "label": source_name,
                "subtitle": f"{len(source_includes)} include pattern(s)",
                "meta": {"includes": source_includes},
            }
        )
        var_to_node[source_name] = source_id

    for index, step in enumerate(config.get("steps") or [], start=1):
        if not isinstance(step, dict):
            continue
        node_id = f"step:{index}"
        processor = step.get("processor", "unknown")
        output = step.get("output") or f"step_{index}"
        outputs = step.get("outputs") or {}
        params = step.get("params") or {}
        nodes.append(
            {
                "id": node_id,
                "type": "processor",
                "label": processor,
                "subtitle": output if not outputs else f"{len(outputs)} named outputs",
                "meta": {
                    "index": index,
                    "inputs": step.get("inputs", "initial"),
                    "output": output,
                    "outputs": outputs,
                    "params": params,
                    "export": step.get("export"),
                },
            }
        )

        raw_inputs = step.get("inputs", "initial")
        input_vars = raw_inputs if isinstance(raw_inputs, list) else [raw_inputs]
        fallback_source = next(iter(var_to_node.values()))
        for input_var in input_vars:
            from_id = var_to_node.get(str(input_var), fallback_source)
            edges.append(
                {
                    "id": f"{from_id}->{node_id}:{input_var}",
                    "source": from_id,
                    "target": node_id,
                    "label": str(input_var),
                }
            )

        if isinstance(outputs, dict) and outputs:
            for group_name, var_name in outputs.items():
                var_to_node[str(var_name)] = node_id
        else:
            var_to_node[str(output)] = node_id

    for index, final in enumerate(config.get("final_output") or [], start=1):
        if not isinstance(final, dict):
            continue
        name = str(final.get("name", ""))
        export_name = final.get("export") or name
        node_id = f"export:{index}"
        upstream_node = var_to_node.get(name, "")
        upstream_index = None
        if isinstance(upstream_node, str) and upstream_node.startswith("step:"):
            try:
                upstream_index = int(upstream_node.split(":", 1)[1])
            except ValueError:
                upstream_index = None
        nodes.append(
            {
                "id": node_id,
                "type": "export",
                "label": export_name,
                "subtitle": name,
                "meta": {**final, "source": name, "upstreamIndex": upstream_index},
            }
        )
        from_id = var_to_node.get(name)
        if from_id:
            edges.append(
                {
                    "id": f"{from_id}->{node_id}:{name}",
                    "source": from_id,
                    "target": node_id,
                    "label": name,
                }
            )

    return {"nodes": nodes, "edges": edges}


def _pipeline_stage_results(config: dict[str, Any], session) -> dict[str, list[dict[str, Any]]]:
    stage_results: dict[str, list[dict[str, Any]]] = {}
    export_meta = {
        row.name: row
        for row in session.query(ExportMeta).order_by(ExportMeta.created_at.desc()).all()
    }
    output_to_export: dict[str, str] = {}
    for index, step in enumerate(config.get("steps") or [], start=1):
        if not isinstance(step, dict):
            continue
        processor = step.get("processor", "")
        if step.get("output") and step.get("export"):
            output_to_export[str(step["output"])] = str(step["export"])
        for _, output_name in (step.get("outputs") or {}).items():
            if step.get("export"):
                output_to_export[str(output_name)] = str(step["export"])
        raw_inputs = step.get("inputs", "initial")
        input_forms = {str(raw_inputs), repr(raw_inputs)}
        if isinstance(raw_inputs, list):
            input_forms.add(str(raw_inputs))
            input_forms.add(repr(raw_inputs))

        query = (
            session.query(DataEntry)
            .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
            .filter(DataEntry.description.like(f"%Pipeline Step: {processor}%"))
        )
        input_matches = [DataEntry.description.like(f"%Inputs: {form}%") for form in input_forms]
        if input_matches:
            query = query.filter(or_(*input_matches))
        entries = query.order_by(DataEntry.timestamp.desc()).limit(12).all()

        if not entries:
            entries = (
                session.query(DataEntry)
                .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
                .filter(DataEntry.description.like(f"%Processed by {processor}%"))
                .order_by(DataEntry.timestamp.desc())
                .limit(8)
                .all()
            )

        stage_results[f"step:{index}"] = [_entry_payload(entry) for entry in entries]

    raw_entries = (
        session.query(DataEntry)
        .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
        .filter(DataEntry.type == "raw")
        .order_by(DataEntry.timestamp.desc())
        .limit(12)
        .all()
    )
    for node in _pipeline_graph(config)["nodes"]:
        if node["type"] == "source":
            includes = (config.get("initial_load") or {}).get("include") or []
            include_paths = [str(inc.get("path", "")) for inc in includes if isinstance(inc, dict)]
            matched = []
            for entry in raw_entries:
                op = entry.original_path or entry.path or ""
                for pat in include_paths:
                    if Path(op).match(pat) or Path(op).match(f"**/{pat}"):
                        matched.append(entry)
                        break
            stage_results[node["id"]] = [_entry_payload(entry) for entry in matched]
        elif node["type"] == "export":
            meta = node.get("meta") or {}
            export_name = str(meta.get("export") or "")
            source_name = str(meta.get("source") or "")
            resolved_export = export_name or output_to_export.get(source_name, "")
            row = export_meta.get(resolved_export)
            if row:
                entry = session.get(DataEntry, row.data_entry_id)
                stage_results[node["id"]] = [_entry_payload(entry)] if entry else []
            else:
                stage_results[node["id"]] = []
    return stage_results


def _entry_lineage(entry: DataEntry, max_depth: int = 16) -> dict[str, Any]:
    nodes: dict[int, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    visited: set[int] = set()

    def visit(current: DataEntry, depth: int) -> None:
        if current.id in visited or depth > max_depth:
            return
        visited.add(current.id)
        payload = _entry_payload(current)
        payload["processor"] = _processor_from_description(current.description or "")
        nodes[current.id] = payload
        for parent in current.parents:
            edges.append(
                {
                    "id": f"{parent.id}->{current.id}",
                    "source": parent.id,
                    "target": current.id,
                }
            )
            visit(parent, depth + 1)

    visit(entry, 0)
    return {"nodes": list(nodes.values()), "edges": edges}


def _processor_from_description(description: str) -> str:
    prefix = "Processed by "
    if not description.startswith(prefix):
        return "raw"
    return description[len(prefix):].split(",", 1)[0]


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/experiments")
def list_experiments() -> list[dict[str, Any]]:
    experiments = experiment_manager.get_experiments()
    result = []
    for name, config in experiments.items():
        try:
            stats = _experiment_stats(name)
        except Exception:
            stats = {}
        result.append(
            {
                "name": name,
                "description": config.get("description", ""),
                "current": name == experiment_manager.current_experiment,
                "sourceMode": config.get("source_mode", ""),
                "dataRoot": config.get("data_root", ""),
                "database": config.get("database", ""),
                "createdAt": config.get("created_at", ""),
                "stats": stats,
            }
        )
    return sorted(result, key=lambda item: item["name"])


@app.get("/api/experiments/{name}/entries")
def list_entries(name: str, limit: int = 200) -> list[dict[str, Any]]:
    with experiment_context(name), get_session() as session:
        entries = (
            session.query(DataEntry)
            .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
            .order_by(DataEntry.timestamp.desc())
            .limit(limit)
            .all()
        )
        return [_entry_payload(entry) for entry in entries]


@app.get("/api/experiments/{name}/sources")
def list_sources(name: str, limit: int = 200) -> list[dict[str, Any]]:
    with experiment_context(name), get_session() as session:
        entries = (
            session.query(DataEntry)
            .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
            .filter(DataEntry.type == "raw")
            .order_by(DataEntry.timestamp.desc(), DataEntry.id.desc())
            .limit(limit * 3)
            .all()
        )
        seen: dict[str, DataEntry] = {}
        for entry in entries:
            key = entry.original_path or entry.path or str(entry.id)
            if key not in seen:
                seen[key] = entry
        return [_source_payload(entry) for entry in list(seen.values())[:limit]]


@app.post("/api/experiments/{name}/sources/upload")
async def upload_source(name: str, file: UploadFile = FastAPIFile(...)) -> dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    suffix = Path(file.filename).suffix
    with experiment_context(name), get_session() as session, tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / f"upload{suffix}"
        with tmp_path.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)
        storage = FileStorage()
        entry = storage.store_raw_data(tmp_path, session)
        entry.description = f"Uploaded from web: {file.filename}"
        entry.original_path = file.filename
        session.commit()
        session.refresh(entry)
        return _source_payload(entry)


@app.post("/api/experiments/{name}/processors/upload")
async def upload_processor(name: str, file: UploadFile = FastAPIFile(...)) -> dict[str, Any]:
    if not file.filename or not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="Upload a Python processor file")
    raw = await file.read()
    try:
        code = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Processor file must be UTF-8: {exc}") from exc
    validation = _validate_processor_code(code, file.filename)
    if not validation["ok"]:
        raise HTTPException(status_code=400, detail={"message": "Processor format is invalid", **validation})
    with experiment_context(name):
        proc_dir = experiment_manager.base_path / "processors"
        proc_dir.mkdir(parents=True, exist_ok=True)
        safe_name = validation["filename"]
        target = proc_dir / safe_name
        target.write_text(code, encoding="utf-8")
        load_processors_from_dir(proc_dir)
        return {"path": str(target), "filename": safe_name, "validation": validation}


@app.get("/api/processors/templates")
def processor_templates() -> list[dict[str, str]]:
    return PROCESSOR_TEMPLATES


@app.post("/api/experiments/{name}/processors/validate")
def validate_processor(name: str, payload: ProcessorDraftRequest) -> dict[str, Any]:
    with experiment_context(name):
        return _validate_processor_code(payload.code, payload.filename)


@app.post("/api/experiments/{name}/processors/save")
def save_processor(name: str, payload: ProcessorDraftRequest) -> dict[str, Any]:
    validation = _validate_processor_code(payload.code, payload.filename)
    if not validation["ok"]:
        raise HTTPException(status_code=400, detail={"message": "Processor format is invalid", **validation})
    with experiment_context(name):
        proc_dir = experiment_manager.base_path / "processors"
        proc_dir.mkdir(parents=True, exist_ok=True)
        safe_name = validation["filename"]
        target = proc_dir / safe_name
        target.write_text(payload.code, encoding="utf-8")
        load_processors_from_dir(proc_dir)
        return {"path": str(target), "filename": safe_name, "validation": validation}


@app.get("/api/experiments/{name}/exports")
def list_exports(name: str) -> list[dict[str, Any]]:
    with experiment_context(name), get_session() as session:
        rows = session.query(ExportMeta).order_by(ExportMeta.created_at.desc()).all()
        exports = []
        for row in rows:
            entry = session.get(DataEntry, row.data_entry_id)
            payload = _entry_payload(entry) if entry else {}
            exports.append(
                {
                    "name": row.name,
                    "entryId": row.data_entry_id,
                    "createdAt": row.created_at.isoformat() if row.created_at else "",
                    "entry": payload,
                }
            )
        return exports


@app.get("/api/experiments/{name}/versions")
def list_versions(name: str, pipeline_path: str | None = None) -> list[dict[str, Any]]:
    from core.pipeline_versions import PipelineVersionManager

    with experiment_context(name), get_session() as session:
        rows = session.query(PipelineVersion).order_by(PipelineVersion.id.desc()).all()
        result = []
        for row in rows:
            if pipeline_path and not PipelineVersionManager.config_matches(row.config_file, pipeline_path):
                continue
            try:
                entry_ids = json.loads(row.entry_ids or "[]")
            except json.JSONDecodeError:
                entry_ids = []
            result.append(
                {
                    "id": row.id,
                    "timestamp": row.timestamp.isoformat() if row.timestamp else "",
                    "configFile": row.config_file or "",
                    "pipelinePath": row.config_file or "",
                    "entryIds": entry_ids,
                    "exportId": row.export_id,
                    "exportName": row.export_name or "",
                    "cacheScope": row.cache_scope or "legacy",
                    "globalSet": getattr(row, "global_set", None) or "",
                    "globalValuesSnapshot": getattr(row, "global_values_snapshot", None) or "",
                    "hasConfigSnapshot": bool(getattr(row, "config_snapshot", None)),
                    "hasProcessorSnapshot": bool(getattr(row, "processor_snapshot", None)),
                    "status": row.status,
                }
            )
        return result


@app.get("/api/experiments/{name}/storage")
def experiment_storage(name: str, pipeline_path: str | None = None) -> dict[str, Any]:
    from core.pipeline_versions import PipelineVersionManager

    with experiment_context(name):
        return PipelineVersionManager().storage_report(pipeline_path or "")


@app.post("/api/experiments/{name}/versions/{version_id}/switch")
def switch_version(name: str, version_id: int, pipeline_path: str | None = None) -> dict[str, Any]:
    from core.pipeline_versions import PipelineVersionManager

    with experiment_context(name), get_session() as session:
        pvm = PipelineVersionManager()
        target = pvm.get_version(version_id)
        if not target:
            raise HTTPException(status_code=404, detail=f"Version #{version_id} not found")
        if pipeline_path and not PipelineVersionManager.config_matches(target.get("config_file", ""), pipeline_path):
            raise HTTPException(status_code=400, detail=f"Version #{version_id} does not belong to {pipeline_path}")

        export_id = target.get("export_id")
        export_name = target.get("export_name", "")
        if export_id:
            entry = session.get(DataEntry, export_id)
            if not entry or not Path(entry.path).exists():
                raise HTTPException(status_code=404, detail=f"Version #{version_id} export is missing")
            if export_name:
                export_path = experiment_manager.base_path / "exports" / export_name
                export_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(entry.path, export_path)

        restored_pipeline_path = ""
        config_snapshot = target.get("config_snapshot", "")
        if config_snapshot:
            restore_path = pipeline_path or target.get("config_file", "")
            try:
                pipeline_file = _pipeline_path_from_rel(restore_path)
            except HTTPException as exc:
                raise HTTPException(status_code=400, detail=f"Cannot restore pipeline YAML for version #{version_id}: {exc.detail}") from exc
            pipeline_file.write_text(config_snapshot, encoding="utf-8")
            restored_pipeline_path = _safe_rel(pipeline_file, PIPELINES_ROOT)

        restored_processors = 0
        processor_snapshot = target.get("processor_snapshot", "")
        if processor_snapshot:
            restored_processors = len(PipelineVersionManager.restore_processor_snapshot(processor_snapshot))

        switched = pvm.set_current(version_id)
        pvm._stamp_version(switched.get("config_file", "") if switched else "")
        return {
            "version": {
                "id": switched["id"],
                "timestamp": switched["timestamp"],
                "configFile": switched.get("config_file", ""),
                "pipelinePath": switched.get("config_file", ""),
                "restoredPipelinePath": restored_pipeline_path,
                "entryIds": switched.get("entry_ids", []),
                "exportId": switched.get("export_id"),
                "exportName": switched.get("export_name", ""),
                "cacheScope": switched.get("cache_scope", "legacy"),
                "hasConfigSnapshot": bool(switched.get("config_snapshot")),
                "hasProcessorSnapshot": bool(switched.get("processor_snapshot")),
                "restoredProcessors": restored_processors,
                "status": switched.get("status", ""),
            }
        }


@app.delete("/api/experiments/{name}/versions/{version_id}")
def delete_version(name: str, version_id: int, pipeline_path: str | None = None) -> dict[str, Any]:
    from core.pipeline_versions import PipelineVersionManager

    with experiment_context(name):
        pvm = PipelineVersionManager()
        target = pvm.get_version(version_id)
        if not target:
            raise HTTPException(status_code=404, detail=f"Version #{version_id} not found")
        if pipeline_path and not PipelineVersionManager.config_matches(target.get("config_file", ""), pipeline_path):
            raise HTTPException(status_code=400, detail=f"Version #{version_id} does not belong to {pipeline_path}")
        result = pvm.delete_version(version_id)
        if not result.get("deleted"):
            reason = result.get("reason", "unknown")
            if reason == "active_version":
                raise HTTPException(status_code=409, detail="Active versions cannot be deleted. Restore another version first.")
            raise HTTPException(status_code=400, detail=reason)
        return result


@app.get("/api/experiments/{name}/entries/{entry_id}/preview")
def entry_preview(name: str, entry_id: int) -> dict[str, Any]:
    with experiment_context(name), get_session() as session:
        entry = session.get(DataEntry, entry_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")
        path = Path(entry.path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        ext = path.suffix.lower()
        if ext in {".csv", ".parquet", ".xlsx", ".xls", ".json"}:
            if ext == ".csv":
                df = pd.read_csv(path)
            elif ext == ".parquet":
                df = pd.read_parquet(path)
            elif ext in {".xlsx", ".xls"}:
                df = pd.read_excel(path)
            else:
                df = pd.read_json(path)
            sample = df.head(80).where(pd.notnull(df), None)
            return {
                "kind": "table",
                "columns": [str(col) for col in sample.columns],
                "rows": sample.to_dict(orient="records"),
                "shape": [int(df.shape[0]), int(df.shape[1])],
            }
        if ext in {".txt", ".log", ".yaml", ".yml"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            return {"kind": "text", "text": text[:20000], "truncated": len(text) > 20000}
        return {"kind": "file", "extension": ext}


@app.get("/api/experiments/{name}/entries/{entry_id}/chart-plan")
def entry_chart_plan(name: str, entry_id: int, goal: str = "compare") -> dict[str, Any]:
    with experiment_context(name), get_session() as session:
        entry = session.get(DataEntry, entry_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")
        try:
            return _infer_chart_plan(entry, goal)
        except ValueError as exc:
            return {
                "supported": False,
                "chartType": "unsupported",
                "processor": None,
                "confidence": "low",
                "reason": str(exc),
                "columns": [],
                "shape": [0, 0],
                "sampleRows": [],
                "params": {},
                "title": Path(entry.original_path or entry.path or "chart").stem.replace("_", " ").title(),
            }


@app.post("/api/experiments/{name}/auto-chart")
def create_auto_chart(name: str, payload: AutoChartRequest) -> dict[str, Any]:
    with experiment_context(name), get_session() as session:
        entry = session.get(DataEntry, payload.entry_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")
        plan = _infer_chart_plan(entry, payload.goal)
        if not plan.get("supported") or not plan.get("processor"):
            raise HTTPException(status_code=400, detail=plan.get("reason") or "Could not infer a chart for this data")
        plan = _customized_chart_plan(plan, payload)
        studio_dir = PIPELINES_ROOT / _experiment_pipeline_dir(name) / "studio"
        studio_dir.mkdir(parents=True, exist_ok=True)
        slug = _slugify_name(payload.name)
        target = studio_dir / f"{slug}.yaml"
        counter = 2
        while target.exists():
            target = studio_dir / f"{slug}_{counter}.yaml"
            counter += 1
        target.write_text(_auto_chart_yaml(payload.name, entry, plan), encoding="utf-8")
    return get_pipeline(_safe_rel(target, PIPELINES_ROOT))


@app.get("/api/experiments/{name}/entries/{entry_id}/file")
def entry_file(name: str, entry_id: int):
    with experiment_context(name), get_session() as session:
        entry = session.get(DataEntry, entry_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")
        path = Path(entry.path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        if path.suffix.lower() == ".pdf":
            return FileResponse(
                path,
                media_type="application/pdf",
                filename=path.name,
                content_disposition_type="inline",
            )
        return FileResponse(path)


@app.get("/api/experiments/{name}/entries/{entry_id}/lineage")
def entry_lineage(name: str, entry_id: int) -> dict[str, Any]:
    with experiment_context(name), get_session() as session:
        entry = (
            session.query(DataEntry)
            .options(selectinload(DataEntry.tags), selectinload(DataEntry.parents))
            .filter(DataEntry.id == entry_id)
            .first()
        )
        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")
        return _entry_lineage(entry)


@app.get("/api/pipelines")
def list_pipelines() -> list[dict[str, Any]]:
    if not PIPELINES_ROOT.exists():
        return []
    paths = sorted(
        path for path in PIPELINES_ROOT.rglob("*.y*ml") if ".git" not in path.parts
    )
    return [_pipeline_summary(path) for path in paths]


@app.get("/api/globals")
def global_sets(experiment: str | None = None) -> list[dict[str, Any]]:
    context = experiment_context(experiment) if experiment else nullcontext()
    with context:
        return list_global_sets(experiment)


@app.get("/api/globals/{set_name}")
def global_set(set_name: str, experiment: str | None = None) -> dict[str, Any]:
    context = experiment_context(experiment) if experiment else nullcontext()
    with context:
        try:
            return read_global_set(set_name, experiment)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Global set not found") from exc


@app.get("/api/experiments/{name}/globals")
def experiment_global_sets(name: str) -> list[dict[str, Any]]:
    with experiment_context(name):
        return list_global_sets(name)


@app.get("/api/experiments/{name}/globals/{set_name}")
def experiment_global_set(name: str, set_name: str) -> dict[str, Any]:
    with experiment_context(name):
        try:
            return read_global_set(set_name, name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Global set not found") from exc


@app.put("/api/experiments/{name}/globals/{set_name}")
def put_global_set(name: str, set_name: str, payload: GlobalSetSaveRequest) -> dict[str, Any]:
    if payload.scope not in {"experiment", "shared"}:
        raise HTTPException(status_code=400, detail="scope must be experiment or shared")
    with experiment_context(name):
        try:
            return save_global_set(
                set_name,
                payload.text,
                experiment=name,
                description=payload.description,
                scope=payload.scope,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid global set: {exc}") from exc


@app.post("/api/experiments/{name}/globals/{set_name}/duplicate")
def duplicate_experiment_global_set(name: str, set_name: str, payload: GlobalSetDuplicateRequest) -> dict[str, Any]:
    if not payload.target.strip():
        raise HTTPException(status_code=400, detail="Target name cannot be empty")
    with experiment_context(name):
        try:
            return duplicate_global_set(set_name, payload.target, name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Global set not found") from exc


@app.get("/api/experiments/{name}/globals/{set_name}/affected")
def affected_by_global_set(name: str, set_name: str) -> list[dict[str, Any]]:
    with experiment_context(name):
        try:
            return global_set_affected(set_name, name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Global set not found") from exc


@app.post("/api/experiments/{name}/globals/{set_name}/regenerate")
def regenerate_affected_outputs(name: str, set_name: str, payload: GlobalSetRegenerateRequest) -> dict[str, Any]:
    if payload.source_mode not in {"version", "raw", "external", "auto"}:
        raise HTTPException(status_code=400, detail="source_mode must be version, raw, external, or auto")
    with experiment_context(name):
        try:
            affected = global_set_affected(set_name, name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Global set not found") from exc

    targets = affected[: payload.limit] if payload.limit else affected
    results: list[dict[str, Any]] = []
    for item in targets:
        pipeline_rel_path = item["path"]
        path = _pipeline_path_from_rel(pipeline_rel_path)
        command = [
            sys.executable,
            "main.py",
            "--experiment",
            name,
            "pipeline",
            "run",
            str(path),
            "--debug",
            "--global-set",
            set_name,
            "--source-mode",
            payload.source_mode,
        ]
        if payload.force_fresh:
            command.append("--fresh-scope")
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                run_env = {
                    **os.environ,
                    "PYTHONUNBUFFERED": "1",
                    "TMPDIR": tmpdir,
                    "FILELINE_PIPELINE_PATH": pipeline_rel_path,
                    "FILELINE_SOURCE_MODE": payload.source_mode,
                }
                result = subprocess.run(
                    command,
                    cwd=PROJECT_ROOT,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=900,
                    env=run_env,
                )
        except Exception as exc:
            results.append({
                "path": pipeline_rel_path,
                "ok": False,
                "returnCode": -1,
                "summary": str(exc),
                "stdout": "",
                "stderr": str(exc),
                "outputs": item.get("outputs", []),
            })
            continue
        feedback = _parse_run_feedback(result.stdout, result.stderr) if result.returncode != 0 else {}
        results.append({
            "path": pipeline_rel_path,
            "ok": result.returncode == 0,
            "returnCode": result.returncode,
            "summary": feedback.get("summary", "Regenerated") if result.returncode != 0 else "Regenerated",
            "stdout": result.stdout,
            "stderr": result.stderr,
            "outputs": item.get("outputs", []),
        })
    return {
        "ok": all(item["ok"] for item in results),
        "globalSet": set_name,
        "affectedPipelines": len(affected),
        "affectedOutputs": sum(item.get("outputCount", 0) for item in affected),
        "ranPipelines": len(results),
        "results": results,
    }


@app.get("/api/pipelines/{pipeline_path:path}")
def get_pipeline(pipeline_path: str) -> dict[str, Any]:
    path = _pipeline_path_from_rel(pipeline_path)
    raw_text = path.read_text(encoding="utf-8")
    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    declared = declared_global_variables(config)
    required = pipeline_required_variables(raw_text, config)
    return {
        **_pipeline_summary(path),
        "config": config,
        "yaml": raw_text,
        "globals": _load_global_file(path),
        "globalSet": pipeline_global_set(config),
        "declaredVariables": declared,
        "usedVariables": required,
        "graph": _pipeline_graph(config),
    }


@app.post("/api/pipelines/{pipeline_path:path}/resolve")
def resolve_pipeline(pipeline_path: str, payload: PipelineResolveRequest, experiment: str | None = None) -> dict[str, Any]:
    path = _pipeline_path_from_rel(pipeline_path)
    text = path.read_text(encoding="utf-8")
    values: dict[str, str] = {}
    info: dict[str, Any] = {"source": "", "set": "", "values": {}}
    exp_name = experiment or experiment_manager.current_experiment
    context = experiment_context(exp_name) if exp_name else nullcontext()
    with context:
        selected_global_set = payload.global_set or pipeline_global_set(yaml.safe_load(text) or {})
        if selected_global_set:
            try:
                values, info = load_global_values(
                    global_set=selected_global_set,
                    overrides=payload.overrides or {},
                    experiment=exp_name,
                )
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail="Global set not found") from exc
        elif payload.overrides:
            values, info = load_global_values(overrides=payload.overrides, experiment=exp_name)
    result = resolve_text(text, values)
    return {
        **result,
        "globalInfo": info,
    }


@app.post("/api/pipelines/{pipeline_path:path}/rename")
def rename_pipeline(pipeline_path: str, payload: PipelineRenameRequest) -> dict[str, Any]:
    path = _pipeline_path_from_rel(pipeline_path)
    old_rel = _safe_rel(path, PIPELINES_ROOT)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Pipeline name cannot be empty")

    try:
        config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {exc}") from exc
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Pipeline YAML must be a mapping")

    config["name"] = name
    yaml_text = yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)
    slug = _slugify_name(name)
    target = path.with_name(f"{slug}{path.suffix}")
    if target.resolve() != path.resolve():
        counter = 2
        while target.exists():
            target = path.with_name(f"{slug}_{counter}{path.suffix}")
            counter += 1
        path.write_text(yaml_text, encoding="utf-8")
        old_global = path.with_suffix(".global")
        path.rename(target)
        if old_global.exists():
            new_global = target.with_suffix(".global")
            if not new_global.exists():
                old_global.rename(new_global)
    else:
        path.write_text(yaml_text, encoding="utf-8")
        target = path
    new_rel = _safe_rel(target, PIPELINES_ROOT)
    if payload.experiment and new_rel != old_rel:
        with experiment_context(payload.experiment), get_session() as session:
            for version in session.query(PipelineVersion).all():
                if version.config_file == old_rel:
                    version.config_file = new_rel
            session.commit()
    return get_pipeline(_safe_rel(target, PIPELINES_ROOT))


@app.post("/api/pipelines/clone")
def clone_pipeline(payload: PipelineCloneRequest) -> dict[str, Any]:
    source = _pipeline_path_from_rel(payload.source_path)
    try:
        config = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid source YAML: {exc}") from exc
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Source pipeline YAML must be a mapping")

    if payload.source_spec:
        spec = dict(payload.source_spec)
        source_name = str(spec.get("source") or "initial")
        include: dict[str, Any] = {
            "path": str(spec.get("path") or spec.get("pattern") or "*"),
            "source": source_name,
            "tags": spec.get("tags") or ["studio_input"],
        }
        for key in ("remote", "regex", "sort_by", "sort_key", "limit"):
            if spec.get(key) not in (None, ""):
                include[key] = spec[key]
        initial_load = config.setdefault("initial_load", {})
        initial_load["include"] = [include]
        initial_load.setdefault("type", spec.get("type") or "raw")
        tags = list(initial_load.get("global_tags", []))
        if "studio" not in tags:
            tags.append("studio")
        initial_load["global_tags"] = tags
    elif payload.data_entry_id and payload.experiment:
        with experiment_context(payload.experiment), get_session() as session:
            entry = session.get(DataEntry, payload.data_entry_id)
            if not entry:
                raise HTTPException(status_code=404, detail="Data entry not found")
            data_path = entry.original_path or entry.path
        initial_load = config.setdefault("initial_load", {})
        initial_load["include"] = [{"path": data_path, "tags": ["studio_input"]}]
        initial_load.setdefault("type", "raw")
        tags = list(initial_load.get("global_tags", []))
        if "studio" not in tags:
            tags.append("studio")
        initial_load["global_tags"] = tags

    config["name"] = payload.name.strip() or source.stem
    description = config.get("description", "")
    config["description"] = description or "Created from FileLine Studio"

    studio_dir = PIPELINES_ROOT / _experiment_pipeline_dir(payload.experiment) / "studio" if payload.experiment else PIPELINES_ROOT / "studio"
    studio_dir.mkdir(parents=True, exist_ok=True)
    slug = _slugify_name(payload.name)
    target = studio_dir / f"{slug}.yaml"
    counter = 2
    while target.exists():
        target = studio_dir / f"{slug}_{counter}.yaml"
        counter += 1
    target.write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return get_pipeline(_safe_rel(target, PIPELINES_ROOT))


@app.get("/api/experiments/{name}/pipelines/{pipeline_path:path}/stage-results")
def pipeline_stage_results(name: str, pipeline_path: str) -> dict[str, list[dict[str, Any]]]:
    path = _pipeline_path_from_rel(pipeline_path)
    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    with experiment_context(name), get_session() as session:
        return _pipeline_stage_results(config, session)


@app.put("/api/pipelines/{pipeline_path:path}")
def save_pipeline(pipeline_path: str, payload: PipelineSaveRequest) -> dict[str, Any]:
    path = _pipeline_path_from_rel(pipeline_path)
    try:
        config = yaml.safe_load(payload.yaml_text) or {}
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {exc}") from exc
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Pipeline YAML must be a mapping")
    if "initial_load" not in config or "steps" not in config:
        raise HTTPException(status_code=400, detail="Pipeline YAML requires initial_load and steps")

    path.write_text(payload.yaml_text, encoding="utf-8")
    return get_pipeline(pipeline_path)


@app.post("/api/experiments/{name}/pipelines/{pipeline_path:path}/run")
def run_pipeline(
    name: str,
    pipeline_path: str,
    dry_run: bool = False,
    force_fresh: bool = False,
    source_mode: str | None = None,
    global_set: str | None = None,
) -> dict[str, Any]:
    if source_mode not in {None, "", "version", "raw", "external", "auto"}:
        raise HTTPException(status_code=400, detail="source_mode must be version, raw, external, or auto")
    path = _pipeline_path_from_rel(pipeline_path)
    command_path = path
    pipeline_rel_path = _safe_rel(path, PIPELINES_ROOT)
    temp_config_path: Path | None = None

    if force_fresh and not dry_run:
        config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for step in config.get("steps") or []:
            if isinstance(step, dict):
                step["cache"] = False
                step["force_rerun"] = True
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", prefix=f"{path.stem}.fresh.", dir=path.parent, delete=False, encoding="utf-8") as handle:
            yaml.dump(config, handle, default_flow_style=False, allow_unicode=True, sort_keys=False)
            temp_config_path = Path(handle.name)
            command_path = temp_config_path

    command = [
        sys.executable,
        "main.py",
        "--experiment",
        name,
        "pipeline",
        "run",
        str(command_path),
        "--debug",
    ]
    selected_global_set = global_set
    if not selected_global_set:
        try:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            selected_global_set = pipeline_global_set(loaded)
        except Exception:
            selected_global_set = ""

    if selected_global_set:
        command.extend(["--global-set", selected_global_set])
    else:
        global_path = _find_global_file(path)
        if global_path:
            command.extend(["--global-config", str(global_path)])
    if dry_run:
        command.append("--dry-run")
    if force_fresh and not dry_run:
        command.append("--fresh-scope")
    if source_mode:
        command.extend(["--source-mode", source_mode])

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_env = {
                **os.environ,
                "PYTHONUNBUFFERED": "1",
                "TMPDIR": tmpdir,
                "FILELINE_PIPELINE_PATH": pipeline_rel_path,
            }
            if source_mode:
                run_env["FILELINE_SOURCE_MODE"] = source_mode
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=900,
                env=run_env,
            )
    finally:
        if temp_config_path and temp_config_path.exists():
            temp_config_path.unlink()

    feedback = _parse_run_feedback(result.stdout[-30000:], result.stderr[-30000:]) if result.returncode != 0 else {
        "summary": "Pipeline finished successfully.",
        "errorType": "",
        "failedStage": "",
        "failedProcessor": "",
        "hint": "",
    }

    return {
        "ok": result.returncode == 0,
        "returnCode": result.returncode,
        "stdout": result.stdout[-30000:],
        "stderr": result.stderr[-30000:],
        "command": command,
        "forceFresh": force_fresh,
        "sourceMode": source_mode or "",
        **feedback,
    }


@app.get("/api/processors")
def list_processors(experiment: str | None = None) -> list[dict[str, Any]]:
    if experiment:
        load_processors_from_dir(PIPELINES_ROOT / experiment / "processors")
        with experiment_context(experiment):
            load_processors_from_dir(experiment_manager.base_path / "processors")
    processors = []
    for name, info in sorted(ProcessorRegistry._processors.items()):
        module = getattr(info["func"], "__module__", "")
        source_file = info.get("source_file", "")
        category = "other"
        if experiment and (
            f"FileLine-Pipelines/{experiment}/processors" in source_file
            or f"experiments/{experiment}/processors" in source_file
        ):
            category = f"{experiment} custom"
        elif "processes." in module and "_operations" in module:
            category = module.split("processes.", 1)[1].split("_operations", 1)[0]
        # 提取函数签名参数（过滤框架参数）
        sig_params = []
        try:
            sig = inspect.signature(info["func"])
            for p_name, p in sig.parameters.items():
                if p_name in ("input_path", "input_paths", "output_path", "output_dir", "kwargs", "args"):
                    continue
                default = None if p.default is inspect.Parameter.empty else p.default
                if default is not None and not isinstance(default, (str, int, float, bool, list, dict, tuple)):
                    default = str(default)
                sig_params.append({
                    "name": p_name,
                    "default": default,
                    "hasDefault": p.default is not inspect.Parameter.empty,
                })
        except Exception:
            pass
        processors.append(
            {
                "name": name,
                "category": category,
                "inputType": info.get("input_type"),
                "outputType": info.get("output_type"),
                "outputExt": info.get("output_ext"),
                "hash": info.get("hash"),
                "module": module,
                "sourceFile": source_file,
                "description": _processor_description(name, info),
                "signature": sig_params,
            }
        )
    return processors


@app.get("/")
def root():
    if (WEB_DIST / "index.html").exists():
        return FileResponse(WEB_DIST / "index.html")
    return PlainTextResponse("FileLine API is running. Build the React app with: cd web && npm run build")


@app.head("/")
def root_head():
    if (WEB_DIST / "index.html").exists():
        return FileResponse(WEB_DIST / "index.html")
    return PlainTextResponse("")


if WEB_DIST.exists():
    app.mount("/assets", StaticFiles(directory=WEB_DIST / "assets"), name="assets")


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
def api_fallback(path: str, request: Request):
    request_path = request.url.path
    for route in app.routes:
        route_path = getattr(route, "path", "")
        if not route_path.startswith("/api/") or route_path == "/api/{path:path}":
            continue
        route_regex = getattr(route, "path_regex", None)
        route_methods = getattr(route, "methods", None) or set()
        if route_regex and route_regex.match(request_path) and request.method not in route_methods:
            return JSONResponse(
                status_code=405,
                content={
                    "error": "Method Not Allowed",
                    "message": f"{request.method} is not supported for this API route.",
                    "path": request_path,
                    "allowedMethods": sorted(route_methods),
                },
            )
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": "API route does not exist.",
            "path": request_path,
        },
    )


@app.get("/{path:path}")
def spa_fallback(path: str):
    if path.startswith("api/"):
        return JSONResponse(
            status_code=404,
            content={
                "error": "Not Found",
                "message": "API route does not exist.",
                "path": f"/{path}",
            },
        )
    target = WEB_DIST / path
    if target.exists() and target.is_file():
        return FileResponse(target)
    index = WEB_DIST / "index.html"
    if index.exists():
        return FileResponse(index)
    return PlainTextResponse("FileLine API is running. React dist is missing.", status_code=404)
