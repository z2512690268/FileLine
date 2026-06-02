import importlib
from pathlib import Path


def _auto_register():
    modules_dir = Path(__file__).parent
    # 加载子目录（如 plot_operations/）
    for p in modules_dir.glob("*/"):
        if p.is_dir() and not p.name.startswith("_"):
            importlib.import_module(f"processes.{p.name}")
    # 加载根目录的 .py（如 demo.py）
    for p in modules_dir.glob("*.py"):
        if p.stem not in ("__init__", "__pycache__"):
            importlib.import_module(f"processes.{p.stem}")


_auto_register()