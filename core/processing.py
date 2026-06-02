# core/processing.py
import inspect
import shutil
import tempfile
from pathlib import Path
from functools import wraps
from typing import Dict, Callable, Union, List, Type, Optional, Set, Tuple
import hashlib
from dataclasses import dataclass
from .models import DataEntry, Tag
from .storage import FileStorage

class ProcessorRegistry:
    """处理函数注册中心（支持任意文件类型）"""
    _processors: Dict[str, Dict] = {}

    @classmethod
    def dependencies(cls, deps):
        """注解，用于声明函数或类的依赖项"""
        #TODO 功能测试失败
        def decorator(obj):
            if not hasattr(obj, '__dependencies__'):
                obj.__dependencies__ = []
            for dep in deps:
                if dep not in obj.__dependencies__:
                    obj.__dependencies__.append(dep)
            return obj
        return decorator

    @classmethod
    def register(cls, name: Optional[str] = None, input_type: str = "single",
                 output_type: str = "single", output_ext: str = ".txt"):
        """注册处理函数的装饰器

        Args:
            name: 处理器名称
            input_type: 输入类型 (single/multi/none)
            output_type: 输出类型 (single/multi)
        """
        if input_type not in {"single", "multi", "none"}:
            raise ValueError(f"无效的input_type：{input_type}")
        if output_type not in {"single", "multi"}:
            raise ValueError(f"无效的output_type：{output_type}")
        if not output_ext.startswith("."):
            output_ext = f".{output_ext}"
        def decorator(func: Callable):
            _name = name or func.__name__
            if _name in cls._processors:
                raise ValueError(f"处理器 {_name} 已注册")
            sig = inspect.signature(func)
            func_hash = cls._calculate_hash(func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            cls._processors[_name] = {
                "func": wrapper,
                "input_type": input_type,
                "output_type": output_type,
                "output_ext": output_ext,
                "hash": func_hash
            }
            return wrapper
        return decorator


    @classmethod
    def _collect_source_code(cls, obj: Union[Callable, Type], visited: Optional[Set[int]] = None) -> str:
        if visited is None:
            visited = set()
        obj_id = id(obj)
        if obj_id in visited:
            return ''
        visited.add(obj_id)
        
        # 获取对象的源代码
        try:
            source = inspect.getsource(obj)
        except OSError:
            # 无法获取源代码（如C扩展），则返回空字符串
            source = ''
        cleaned = '\n'.join([line.strip() for line in source.splitlines() if line.strip()])
        
        # 收集依赖项的源代码
        deps = getattr(obj, '__dependencies__', [])
        deps_source = []
        for dep in deps:
            if inspect.isclass(dep) or inspect.isfunction(dep):
                dep_source = cls._collect_source_code(dep, visited)
                deps_source.append(dep_source)
            # 其他类型忽略
        # 合并所有源代码
        total_source = cleaned + ''.join(deps_source)
        return total_source

    @classmethod
    def _calculate_hash(cls, func: Callable) -> str:
        """计算函数及其依赖项代码的哈希值"""
        total_source = cls._collect_source_code(func)
        return hashlib.sha256(total_source.encode('utf-8')).hexdigest()[:8]  # 取前8位作为版本号

    @classmethod
    def get_processor(cls, name: str) -> Dict:
        """获取注册的处理器"""
        if name not in cls._processors:
            raise KeyError(f"未注册的处理器: {name}")
        return cls._processors[name]

@dataclass
class InputPath:
    path: Path
    original_path: Optional[Path]
    tags: List[str]
    id: int

class DataProcessor:
    def __init__(self, storage: FileStorage, db_session):
        self.storage = storage
        self.session = db_session
    
    def run(self, processor_name: str, input_ids, **params) -> Union[DataEntry, List[DataEntry]]:
        """执行数据处理流程，单输出返回 DataEntry，多输出返回 List[DataEntry]"""
        processor = ProcessorRegistry.get_processor(processor_name)
        input_type = processor["input_type"]
        output_type = processor.get("output_type", "single")

        # ---------- 准备输入 ----------
        if input_type == "single":
            if not isinstance(input_ids, int):
                if len(input_ids) == 0:
                    raise ValueError("单个输入类型不能为空")
                if len(input_ids) != 1:
                    raise ValueError("单个输入类型只能输入单个数据记录")
                input_ids = input_ids[0]
            input_path, parent_entries = self._get_single_path(input_ids)
            proc_input = input_path
        elif input_type == "multi":
            input_paths, parent_entries = self._get_multiple_paths(input_ids)
            proc_input = input_paths
        elif input_type == "none":
            parent_entries = []
            proc_input = None
        else:
            raise ValueError(f"未知输入类型: {input_type}")

        # ---------- 执行 ----------
        if output_type == "multi":
            return self._run_multi(processor, processor_name, params, proc_input, parent_entries)
        else:
            return self._run_single(processor, processor_name, params, proc_input, parent_entries)

    def _run_single(self, processor, processor_name, params, proc_input, parent_entries) -> DataEntry:
        """单输出执行"""
        if proc_input is not None:
            # input_type = single / multi
            output_path, entry = self.storage.create_processed_file(
                ext=processor["output_ext"], session=self.session
            )
            result_tags = processor["func"](proc_input, output_path=output_path, **params)
        else:
            # input_type = none
            output_path, entry = self.storage.create_processed_file(
                ext=processor["output_ext"], session=self.session
            )
            result_tags = processor["func"](output_path=output_path, **params)
            entry.parents = []

        if result_tags is None:
            result_tags = []
        elif isinstance(result_tags, str):
            result_tags = [result_tags]
        elif not isinstance(result_tags, (list, tuple)):
            raise ValueError("处理函数返回值必须是字符串或列表")

        desc_args = f"id: {proc_input.id if hasattr(proc_input, 'id') else 'none'}, params: {params}" \
            if proc_input is not None else f"params: {params}"
        entry.description = f"Processed by {processor_name}, {desc_args}"
        if parent_entries:
            entry.parents = parent_entries
        self._add_auto_tags(entry, result_tags)
        self.session.commit()
        return entry

    def _build_entry_from_tempfile(self, tmpdir, filename, raw_tags, processor_name,
                                    desc_extra, parent_entries):
        """从临时文件创建 DataEntry（带语义命名）"""
        from pathlib import Path as _P
        src = tmpdir / filename
        ext = src.suffix
        stem = _P(filename).stem
        tags = [raw_tags] if isinstance(raw_tags, str) else list(raw_tags)
        _, entry = self.storage.create_processed_file(ext=ext, session=self.session)
        semantic = f"{entry.id}_{stem}{ext}" if stem else f"{entry.id}{ext}"
        dst = _P(entry.path).parent / semantic
        shutil.copy(src, dst)
        entry.path = str(dst)
        entry.description = f"Processed by {processor_name}, {desc_extra}"
        if parent_entries:
            entry.parents = parent_entries
        self._add_auto_tags(entry, tags)
        return entry

    def _run_multi(self, processor, processor_name, params, proc_input, parent_entries):
        """多输出: 返回 list 或 dict (命名组)"""
        tmpdir = Path(tempfile.mkdtemp())
        try:
            kwargs = {"output_dir": tmpdir, **params}
            results = processor["func"](proc_input, **kwargs) if proc_input is not None else processor["func"](**kwargs)
            if not results:
                return {} if isinstance(results, dict) else []

            # dict → 命名组:  {"group_a": [(fname, tags), ...], ...}
            if isinstance(results, dict):
                output = {}
                for group_name, items in results.items():
                    group_entries = []
                    for item in (items or []):
                        fname = item[0] if isinstance(item, (list, tuple)) else str(item)
                        raw = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else []
                        e = self._build_entry_from_tempfile(
                            tmpdir, fname, raw, processor_name,
                            f"group={group_name}", parent_entries)
                        group_entries.append(e)
                    output[group_name] = group_entries
                self.session.commit()
                return output

            # list → 扁平多输出 (原有行为)
            created = []
            for item in results:
                fname = item[0] if isinstance(item, (list, tuple)) else str(item)
                raw = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else []
                desc = f"id: {proc_input.id if hasattr(proc_input, 'id') else 'none'}, params: {params}" \
                    if proc_input is not None else f"params: {params}"
                e = self._build_entry_from_tempfile(tmpdir, fname, raw, processor_name, desc, parent_entries)
                created.append(e)
            self.session.commit()
            return created
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def _get_single_path(self, input_id: int) -> Path:
        """获取单个输入路径"""
        data = self.session.get(DataEntry, input_id)
        entry = InputPath(
            path=Path(data.path),
            original_path=Path(data.original_path) if data.original_path else None,
            tags=[t.name for t in data.tags],
            id=data.id
        )
        return entry, [data]

    def _get_multiple_paths(self, input_ids: List[int]) -> List[dict]:
        """获取多个输入路径及元数据"""
        entries = []
        datas = []
        for i in input_ids:
            entry = self.session.get(DataEntry, i)
            entries.append(
                InputPath(
                    path=Path(entry.path),
                    original_path=Path(entry.original_path) if entry.original_path else None,
                    tags=[t.name for t in entry.tags],
                    id=entry.id
                )
            )
            datas.append(entry)
        return entries, datas
    
    def _execute_processor(self, processor: dict, input_paths, params: dict):
        """执行处理函数，返回 (result_tags, entry)"""
        output_path, entry = self.storage.create_processed_file(
            ext=processor["output_ext"], session=self.session
        )
        result_tags = processor["func"](input_paths, output_path=output_path, **params)

        # 标准化返回值格式
        if result_tags is None:
            result_tags = []
        elif isinstance(result_tags, str):
            result_tags = [result_tags]
        elif not isinstance(result_tags, (list, tuple)):
            raise ValueError("处理函数返回值必须是字符串或列表")

        return result_tags, entry

    def _add_auto_tags(self, entry: DataEntry, tags: list):
        """添加自动生成的标签"""
        for tag_name in tags:
            tag = self.session.query(Tag).filter_by(name=tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
                self.session.add(tag)
            entry.tags.append(tag)