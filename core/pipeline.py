# core/pipeline.py
from pathlib import Path
from typing import List, Dict, Union, Set
from dataclasses import dataclass
from copy import deepcopy
import glob
from typing import Optional
from sqlalchemy.orm import Session
import hashlib
import json
import os
import shutil
import fnmatch
import re
from sqlalchemy import func
from .processing import DataProcessor, ProcessorRegistry
from .models import DataEntry, Tag, StepCache, FileMTimeCache
from .storage import FileStorage

@dataclass
class PipelineStep:
    processor: str           # 注册的处理函数名称
    inputs: Union[str, List[str]]  # 输入源标识符
    params: Dict            # 处理参数
    output_var: str         # 输出变量名
    cache: str            # 是否使用缓存
    force_rerun: bool        # 是否强制重新运行
    export: Optional[str]   # 输出文件导出名(不包括扩展名)

@dataclass
class IncludeSpec:
    """单个包含模式的配置"""
    path: str              # Glob路径模式
    re_pattern: Optional[str] = None  # 针对该模式的正则表达式
    tags: Optional[List[str]] = None  # 该模式独有的标签

@dataclass
class InitialLoadConfig:
    include_patterns: List[IncludeSpec]       # 包含的glob模式列表
    exclude_patterns: List[str] = None  # 排除的glob模式列表
    data_type: str = "raw"    # 数据类型(raw/processed/plot)
    tags: Optional[List[str]] = None  # 自动添加的标签

class PipelineRunner:
    def __init__(self, storage: FileStorage, session: Session):
        self.storage = storage
        self.session = session
        self.context = {}
    
    def execute(self, 
               initial_load: InitialLoadConfig,
               steps: List[PipelineStep],
               debug: bool = False) -> Dict:
        """执行带初始加载的流水线"""
        # 1. 初始文件加载
        input_ids = self._load_initial_files(initial_load, debug)
        self.context["initial"] = input_ids
        
        # 2. 执行处理步骤
        processor = DataProcessor(self.storage, self.session)
        
        for step in steps:
            resolved_ids = self._resolve_inputs(step.inputs)

            # 检查缓存
            process_desc = ProcessorRegistry.get_processor(step.processor)
            step_hash = self._generate_step_hash(
                processor=step.processor,
                func_hash=process_desc["hash"],
                input_ids=resolved_ids,
                params=step.params
            )
            
            cached = self.session.query(StepCache).filter(
                StepCache.input_hash == step_hash
            ).order_by(StepCache.created_at.desc()).first()
            
            if cached and not step.force_rerun and step.cache:
                self.context[step.output_var] = [cached.output_id]
                entry = self.session.query(DataEntry).get(cached.output_id)
                if step.export:
                    export_path = self.storage.create_export_file(step.export, entry.id)
                    shutil.copy(entry.path, export_path)
                if debug:
                    print("Pipeline Step: ", step.processor, "Inputs: ", step.inputs, "Params: ", step.params)
                    print("Cached Output ID: ", cached.output_id)
                    print("Cached Output Path: ", entry.path)
                    if step.export:
                        print("Exported To Path: ", export_path)
                    print("-------------------------------------------")
                continue
                
            entry = processor.run(
                processor_name=step.processor,
                input_ids=resolved_ids,
                **step.params
            )

            # 记录缓存
            if step.cache:
                self.session.add(StepCache(
                    input_hash=step_hash,
                    output_id=entry.id,
                ))

            if step.export:
                export_path = self.storage.create_export_file(step.export, entry.id)
                shutil.copy(entry.path, export_path)
            
            if debug:
                print("Pipeline Step: ", step.processor, "Inputs: ", step.inputs, "Params: ", step.params)
                print("Generated Output ID: ", entry.id)
                print("Generated Output Path: ", entry.path) 
                if step.export:
                    print("Exported To Path: ", export_path)
                print("-------------------------------------------")

            self.context[step.output_var] = [entry.id]
            self._log_step(step, entry.id)
        
        return self.context

    def _get_file_mtime(self, file_path: str) -> float:
        """获取文件修改时间（UTC时间戳）"""
        return os.path.getmtime(file_path)

    def _load_initial_files(self, config: InitialLoadConfig,
                                debug: bool = False) -> List[int]:
        """加载初始文件到系统（支持包含/排除模式）"""
        # 收集所有包含文件
        file_tags: Dict[str, List[str]] = {}
        all_included: Set[str] = set()
        for spec in config.include_patterns:
            pattern = spec.path
            tags = spec.tags or []
            matches = glob.glob(pattern, recursive=True)

            # 正则二次过滤
            if spec.re_pattern:
                try:
                    pattern = re.compile(spec.re_pattern)
                    matches = [m for m in matches if pattern.search(m)]
                except re.error as e:
                    raise ValueError(f"无效的正则表达式 '{spec.re_pattern}': {e}")

            all_included.update(matches)
            for file in matches:
                file_tags.setdefault(file, []).extend(tags)
        
        # 处理排除模式
        if config.exclude_patterns:
            all_excluded = set()
            for exclude_pattern in config.exclude_patterns:
                for file in all_included:
                    if Path(file).match(exclude_pattern):  # 使用 Path.match
                        all_excluded.add(file)
            all_included -= all_excluded
        
        matched_files = sorted(all_included)
        
        if not matched_files:
            raise FileNotFoundError(
                f"未找到匹配文件。包含模式: {config.include_patterns}，排除模式: {config.exclude_patterns}"
            )
        
        entries = []
        for file_path in matched_files:
            file_tags[file_path].append(file_path)
            current_mtime = self._get_file_mtime(file_path)
            # 查询缓存记录
            cache = self.session.query(FileMTimeCache).filter(
                FileMTimeCache.file_path == file_path
            ).order_by(FileMTimeCache.created_at.desc()).first()

            if cache and cache.last_mtime == current_mtime:
                entry = self.session.query(DataEntry).get(cache.data_entry_id)
                if debug:
                    print(f"已缓存初始文件： {file_path} ，ID: {entry.id}")
            else:
                # 存储文件
                stored_path = self.storage.store_raw_data(file_path)
                # 创建数据条目
                entry = DataEntry(
                    type=config.data_type,
                    path=str(stored_path),
                    original_path=str(file_path),
                    description=f"自动加载自: {file_path}",
                    tags=[]
                )
                self.session.add(entry)
                self.session.flush()
                # 更新缓存
                self.session.add(FileMTimeCache(
                    file_path=file_path,
                    data_entry_id=entry.id,
                    last_mtime=current_mtime
                ))
                if debug:
                    print(f"重新加载初始文件 {file_path} ，ID: {entry.id}")

            if file_tags.get(file_path):
                for tag_name in file_tags[file_path]:
                    tag = self.session.query(Tag).filter_by(name=tag_name).first()
                    if not tag:
                        tag = Tag(name=tag_name)
                        self.session.add(tag)
                    entry.tags.append(tag)

            # 添加标签
            if config.tags:
                for tag_name in config.tags:
                    tag = self.session.query(Tag).filter_by(name=tag_name).first()
                    if not tag:
                        tag = Tag(name=tag_name)
                        self.session.add(tag)
                    entry.tags.append(tag)
            
            entries.append(entry)
        
        self.session.commit()
        if debug:
            print("-------------------------------------------")
        return [e.id for e in entries]
 
    def _resolve_inputs(self, inputs: Union[str, List[str]]) -> List[int]:
        """解析输入源"""
        if isinstance(inputs, str):
            return self.context.get(inputs, [])
        
        resolved = []
        for input_key in inputs:
            resolved.extend(self.context.get(input_key, []))
        return resolved

    def _log_step(self, step: PipelineStep, output_id: int):
        """记录流水线步骤信息"""
        entry = self.session.query(DataEntry).get(output_id)
        history = f"Pipeline Step: {step.processor}, "
        history += f"Inputs: {step.inputs}\n"
        entry.description = history + entry.description
        self.session.commit()

    def _generate_step_hash(self, processor: str, func_hash: str, input_ids: List[int], params: dict) -> str:
        """生成步骤唯一哈希"""
        sorted_params = json.dumps(params, sort_keys=True)
        components = [
            processor,
            func_hash,
            ','.join(sorted(map(str, input_ids))),
            sorted_params
        ]
        return hashlib.sha256('|'.join(components).encode()).hexdigest()