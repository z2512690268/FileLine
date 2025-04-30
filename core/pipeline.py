# core/pipeline.py
from pathlib import Path
from typing import List, Dict, Union
from dataclasses import dataclass
from copy import deepcopy
import glob
from typing import Optional
from sqlalchemy.orm import Session
import hashlib
import json
import os
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


@dataclass
class InitialLoadConfig:
    path_pattern: str         # 支持glob的通配符路径
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
            step_hash = self._generate_step_hash(
                processor=step.processor,
                input_ids=resolved_ids,
                params=step.params
            )
            
            cached = self.session.query(StepCache).filter(
                StepCache.input_hash == step_hash
            ).order_by(StepCache.created_at.desc()).first()
            
            if cached and not step.force_rerun and step.cache:
                self.context[step.output_var] = [cached.output_id]
                if debug:
                    print("Pipeline Step: ", step.processor, "Inputs: ", step.inputs, "Params: ", step.params)
                    print("Cached Output ID: ", cached.output_id)
                    entry = self.session.query(DataEntry).get(cached.output_id)
                    print("Cached Output Path: ", entry.path)
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

            if debug:
                print("Pipeline Step: ", step.processor, "Inputs: ", step.inputs, "Params: ", step.params)
                print("Generated Output ID: ", entry.id)
                print("Generated Output Path: ", entry.path) 
                print("-------------------------------------------")
            self.context[step.output_var] = [entry.id]
            self._log_step(step, entry.id)
        
        return self.context

    def _get_file_mtime(self, file_path: str) -> float:
        """获取文件修改时间（UTC时间戳）"""
        return os.path.getmtime(file_path)

    def _load_initial_files(self, config: InitialLoadConfig,
                                debug: bool = False) -> List[int]:
        """加载初始文件到系统"""
        matched_files = glob.glob(config.path_pattern, recursive=True)
        if not matched_files:
            raise FileNotFoundError(f"未找到匹配文件: {config.path_pattern}")
        
        entries = []
        for file_path in matched_files:
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
                stored_path = self._store_by_type("raw", file_path)
                # 创建数据条目
                entry = DataEntry(
                    type=config.data_type,
                    path=str(stored_path),
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

    def _store_by_type(self, data_type: str, src_path: str) -> Path:
        """按类型存储文件"""
        if data_type == "raw":
            return self.storage.store_raw_data(src_path)
        elif data_type == "processed":
            return self.storage.create_processed_file()
        elif data_type == "plot":
            return self.storage.create_plot_file()
        else:
            raise ValueError(f"未知数据类型: {data_type}")
        
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
        history = f"Pipeline Step: {step.processor}\n"
        history += f"Inputs: {step.inputs}\n"
        history += f"Params: {step.params}\n"
        entry.description = history + entry.description
        self.session.commit()

    def _generate_step_hash(self, processor: str, input_ids: List[int], params: dict) -> str:
        """生成步骤唯一哈希"""
        sorted_params = json.dumps(params, sort_keys=True)
        components = [
            processor,
            ','.join(sorted(map(str, input_ids))),
            sorted_params
        ]
        return hashlib.sha256('|'.join(components).encode()).hexdigest()