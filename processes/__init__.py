import importlib
from pathlib import Path

# 自动加载所有子模块
def _auto_register():
    modules_dir = Path(__file__).parent
    for p in modules_dir.glob("*/"):
        if p.is_dir() and not p.name.startswith('_'):
            module_name = f"processes.{p.name}"
            importlib.import_module(module_name)

_auto_register()