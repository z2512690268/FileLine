import pkgutil
import importlib

__all__ = []

# 遍历当前包下的所有模块
for _, module_name, _ in pkgutil.iter_modules(__path__):
    # 动态导入模块
    module = importlib.import_module(f'.{module_name}', __name__)
    # 将模块添加到当前包的命名空间
    globals()[module_name] = module
    # 添加到__all__列表以便from package import *时导出
    __all__.append(module_name)