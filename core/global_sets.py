"""Global style/parameter sets shared by CLI and Web."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from .base import experiment_manager

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINES_ROOT = PROJECT_ROOT / "FileLine-Pipelines"
PLACEHOLDER_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")


def slugify_global_name(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip()).strip("._")
    return slug or "default"


def global_roots(experiment: str | None = None) -> list[Path]:
    roots = [PIPELINES_ROOT / "_globals"]
    exp = experiment or experiment_manager.current_experiment
    if exp:
        roots.append(experiment_manager.project_root / "experiments" / exp / "globals")
    return roots


def parse_legacy_global_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _stringify_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, dict)):
        return yaml.safe_dump(value, default_flow_style=True, sort_keys=False).strip()
    return str(value)


def parse_global_set_text(text: str) -> tuple[dict[str, str], dict[str, Any]]:
    """Parse either new YAML global set format or legacy KEY=value text."""
    try:
        parsed = yaml.safe_load(text)
    except Exception:
        parsed = None
    if isinstance(parsed, dict) and isinstance(parsed.get("variables"), dict):
        values: dict[str, str] = {}
        for key, raw in parsed["variables"].items():
            if isinstance(raw, dict) and "value" in raw:
                value = raw.get("value")
            else:
                value = raw
            values[str(key)] = _stringify_value(value)
        return values, parsed
    values = parse_legacy_global_text(text)
    metadata = {
        "name": "legacy",
        "description": "Legacy KEY=value global config",
        "variables": {key: {"type": "string", "value": value} for key, value in values.items()},
    }
    return values, metadata


def global_set_path(name: str, experiment: str | None = None, create_scope: str = "experiment") -> Path:
    slug = slugify_global_name(name)
    roots = global_roots(experiment)
    for root in reversed(roots):
        candidate = root / f"{slug}.yaml"
        if candidate.exists():
            return candidate
    root = roots[-1] if create_scope == "experiment" and len(roots) > 1 else roots[0]
    return root / f"{slug}.yaml"


def list_global_sets(experiment: str | None = None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for root in global_roots(experiment):
        if not root.exists():
            continue
        scope = "experiment" if "experiments" in root.parts else "shared"
        for path in sorted(root.glob("*.yaml")):
            name = path.stem
            key = f"{scope}:{name}"
            if key in seen:
                continue
            seen.add(key)
            try:
                values, metadata = parse_global_set_text(path.read_text(encoding="utf-8"))
            except Exception:
                values, metadata = {}, {}
            items.append({
                "name": str(metadata.get("name") or name),
                "id": name,
                "description": str(metadata.get("description") or ""),
                "scope": scope,
                "path": str(path),
                "values": values,
                "variableCount": len(values),
            })
    return items


def read_global_set(name: str, experiment: str | None = None) -> dict[str, Any]:
    path = global_set_path(name, experiment)
    if not path.exists():
        raise FileNotFoundError(name)
    text = path.read_text(encoding="utf-8")
    values, metadata = parse_global_set_text(text)
    return {
        "name": str(metadata.get("name") or path.stem),
        "id": path.stem,
        "description": str(metadata.get("description") or ""),
        "scope": "experiment" if "experiments" in path.parts else "shared",
        "path": str(path),
        "text": text,
        "values": values,
        "metadata": metadata,
    }


def save_global_set(
    name: str,
    text: str,
    experiment: str | None = None,
    description: str = "",
    scope: str = "experiment",
) -> dict[str, Any]:
    slug = slugify_global_name(name)
    path = global_set_path(slug, experiment, create_scope=scope)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not text.strip():
        text = yaml.safe_dump({
            "name": slug,
            "description": description,
            "variables": {},
        }, sort_keys=False)
    path.write_text(text, encoding="utf-8")
    return read_global_set(slug, experiment)


def duplicate_global_set(source: str, target: str, experiment: str | None = None) -> dict[str, Any]:
    src = read_global_set(source, experiment)
    target_slug = slugify_global_name(target)
    text = src["text"]
    try:
        payload = yaml.safe_load(text)
    except Exception:
        payload = None
    if isinstance(payload, dict):
        payload["name"] = target_slug
        text = yaml.safe_dump(payload, default_flow_style=False, allow_unicode=True, sort_keys=False)
    return save_global_set(target_slug, text, experiment=experiment)


def load_global_values(
    global_set: str | None = None,
    global_config: str | Path | None = None,
    overrides: dict[str, str] | None = None,
    experiment: str | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    values: dict[str, str] = {}
    info: dict[str, Any] = {"source": "", "set": "", "values": values}
    if global_set:
        item = read_global_set(global_set, experiment)
        values.update(item["values"])
        info.update({"source": item["path"], "set": item["id"], "scope": item["scope"]})
    elif global_config:
        path = Path(global_config)
        parsed, metadata = parse_global_set_text(path.read_text(encoding="utf-8"))
        values.update(parsed)
        info.update({"source": str(path), "set": path.stem, "scope": "file", "metadata": metadata})
    if overrides:
        values.update({str(k): str(v) for k, v in overrides.items()})
        info["overrides"] = dict(overrides)
    info["values"] = dict(values)
    return values, info


def replace_in_text(content: str, variables: dict[str, str]) -> str:
    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return str(variables.get(var_name, match.group(0)))
    return PLACEHOLDER_RE.sub(replacer, content)


def used_variables(content: str) -> list[str]:
    return sorted(set(PLACEHOLDER_RE.findall(content)))


def resolve_text(content: str, variables: dict[str, str]) -> dict[str, Any]:
    used = used_variables(content)
    resolved = replace_in_text(content, variables)
    missing = sorted(set(PLACEHOLDER_RE.findall(resolved)))
    return {
        "resolvedText": resolved,
        "used": {key: variables[key] for key in used if key in variables},
        "usedVariables": used,
        "missing": missing,
    }


def global_set_affected(name: str, experiment: str | None = None) -> list[dict[str, Any]]:
    values = read_global_set(name, experiment)["values"]
    keys = set(values)
    affected: list[dict[str, Any]] = []
    if not PIPELINES_ROOT.exists():
        return affected
    for path in sorted(PIPELINES_ROOT.rglob("*.y*ml")):
        if ".git" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        used = set(used_variables(text))
        matched = sorted(used & keys)
        if matched:
            affected.append({
                "path": str(path.relative_to(PIPELINES_ROOT)).replace("\\", "/"),
                "variables": matched,
            })
    return affected


def legacy_global_candidates(yaml_path: Path) -> list[Path]:
    return [
        yaml_path.with_suffix(".global"),
        yaml_path.parent / f"{yaml_path.parent.name}.global",
        PIPELINES_ROOT / "plot.global",
    ]
