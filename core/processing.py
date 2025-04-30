# core/processing.py
import inspect
from pathlib import Path
from functools import wraps
from typing import Dict, Callable, Union, List
import hashlib
from .models import DataEntry, Tag
from .storage import FileStorage
class ProcessorRegistry:
    """处理函数注册中心（支持任意文件类型）"""
    _processors: Dict[str, Dict] = {}

    @classmethod
    def _calculate_hash(cls, func: Callable) -> str:
        """计算函数代码的哈希值"""
        # 获取函数源代码（包括装饰器）
        source = inspect.getsource(func)
        # 去除空行和前后空格
        cleaned = '\n'.join([line.strip() for line in source.splitlines() if line.strip()])
        # 生成SHA256哈希
        return hashlib.sha256(cleaned.encode('utf-8')).hexdigest()[:8]  # 取前8位作为版本号

    @classmethod
    def register(cls, name: str, input_type: str = "single", output_ext: str = ".txt"):
        """注册处理函数的装饰器
        
        Args:
            name: 处理器名称
            input_type: 输入类型 (single/multi)
        """
        def decorator(func: Callable):
            sig = inspect.signature(func)
            func_hash = cls._calculate_hash(func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            
            cls._processors[name] = {
                "func": wrapper,
                "input_type": input_type,
                "output_ext": output_ext,
                "params": list(sig.parameters.keys())[1:],  # 排除第一个路径参数
                "hash": func_hash
            }
            return wrapper
        return decorator

    @classmethod
    def get_processor(cls, name: str) -> Dict:
        """获取注册的处理器"""
        if name not in cls._processors:
            raise KeyError(f"未注册的处理器: {name}")
        return cls._processors[name]


class DataProcessor:
    def __init__(self, storage: FileStorage, db_session):
        self.storage = storage
        self.session = db_session
    
    def run(self, 
           processor_name: str,
           input_ids: Union[int, List[int]],
           **params) -> DataEntry:
        """执行数据处理流程"""
        processor = ProcessorRegistry.get_processor(processor_name)
        input_type = processor["input_type"]
        
        # 获取输入路径
        if input_type == "single":
            if not isinstance(input_ids, int):
                if len(input_ids) != 1:
                    raise ValueError("单个输入类型只能输入单个数据记录")
                input_ids = input_ids[0]
            input_path = self._get_single_path(input_ids)
            output_path, result_tags = self._execute_processor(processor, input_path, params)
        elif input_type == "multi":
            input_paths = self._get_multiple_paths(input_ids)
            output_path, result_tags = self._execute_processor(processor, input_paths, params)
        else:
            raise ValueError(f"未知输入类型: {input_type}")

        # 创建数据记录
        entry = DataEntry(
            type='processed',
            path=str(output_path),
            description=f"Processed by {processor_name}, id: {input_ids}, params: {params}"
        )

        # 添加自动生成的标签
        self._add_auto_tags(entry, result_tags)

        self.session.add(entry)
        self.session.commit()
        return entry

    def _get_single_path(self, input_id: int) -> Path:
        """获取单个输入路径"""
        data = self.session.get(DataEntry, input_id)
        entry = {
            "path": Path(data.path),
            "tags": [t.name for t in data.tags],
            "id": data.id
        }
        return entry

    def _get_multiple_paths(self, input_ids: List[int]) -> List[dict]:
        """获取多个输入路径及元数据"""
        entries = []
        for i in input_ids:
            entry = self.session.get(DataEntry, i)
            entries.append({
                "path": Path(entry.path),
                "tags": [t.name for t in entry.tags],
                "id": entry.id
            })
        return entries
    
    def _execute_processor(self, processor: dict, input_paths: Union[dict, List[dict]], params: dict) -> Path:
        """执行处理函数"""
        self._validate_params(params, processor["params"])
        
        # 调用处理函数
        output_path = self.storage.create_processed_file(ext=processor["output_ext"])
        # 调用处理函数并获取返回值
        result_tags = processor["func"](input_paths, output_path=output_path, **params)
        
        # 标准化返回值格式
        if result_tags is None:
            result_tags = []
        elif isinstance(result_tags, str):
            result_tags = [result_tags]
        elif not isinstance(result_tags, (list, tuple)):
            raise ValueError("处理函数返回值必须是字符串或列表")
        
        return output_path, result_tags

    def _validate_params(self, given: dict, expected: list):
        """参数验证"""
        extra = set(given.keys()) - set(expected)
        if extra:
            raise ValueError(f"非法参数: {extra}，可用参数: {expected}")

    def _add_auto_tags(self, entry: DataEntry, tags: list):
        """添加自动生成的标签"""
        for tag_name in tags:
            tag = self.session.query(Tag).filter_by(name=tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
                self.session.add(tag)
            entry.tags.append(tag)