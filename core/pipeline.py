# core/pipeline.py
import click
from pathlib import Path
from .base import experiment_manager
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
    processor: str                     # 注册的处理函数名称
    inputs: Union[str, List[str]]      # 输入源标识符
    params: Dict                       # 处理参数
    output_var: str                    # 输出变量名
    outputs: Optional[Dict[str, str]] = None  # 命名多输出: {group: var}
    cache: str = True                  # 是否使用缓存
    force_rerun: bool = False          # 是否强制重新运行
    export: Optional[str] = None       # 输出文件导出名(不包括扩展名)

@dataclass
class IncludeSpec:
    """单个包含模式的配置"""
    path: str                          # Glob路径模式
    re_pattern: Optional[str] = None   # 针对该模式的正则表达式
    tags: Optional[List[str]] = None   # 该模式独有的标签
    source: str = "initial"            # 输出到 pipeline 的变量名 (默认 initial)
    sort_by: Optional[str] = None      # 排序: name_asc/name_desc/mtime/mtime_asc
    sort_key: Optional[str] = None     # 提取排序键的 regex (如 "(\\d{8})" 取日期)
    limit: Optional[int] = None        # 取前 N 个 (与 sort_by/sort_key 配合)

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
        # 0. 加载实验自带的 processor (如从 .flxp 导入的)
        exp_proc_dir = experiment_manager.base_path / "processors"
        if exp_proc_dir.is_dir():
            from .processing import load_processors_from_dir
            load_processors_from_dir(exp_proc_dir)

        # 1. 初始文件加载 (返回 {source: [ids]})
        sources = self._load_initial_files(initial_load, debug)
        for src_name, src_ids in sources.items():
            self.context[src_name] = src_ids
        
        # 2. 执行处理步骤
        processor = DataProcessor(self.storage, self.session)
        
        for step in steps:
            resolved_ids = self._resolve_inputs(step.inputs)

            # 检查缓存
            process_desc = ProcessorRegistry.get_processor(step.processor)

            # ── 多输出→单输入 foreach: 对每个输入执行一次, 收集所有结果 ──
            if process_desc["input_type"] == "single" and len(resolved_ids) > 1:
                if debug:
                    print(f"  Foreach x{len(resolved_ids)}: {step.processor} ← {step.inputs}")
                all_entries = []
                for rid in resolved_ids:
                    r = processor.run(processor_name=step.processor, input_ids=rid, **step.params)
                    inp_entry = self.session.query(DataEntry).get(rid)
                    # 语义命名 + 标签继承
                    in_stem = Path(inp_entry.path).stem if inp_entry else str(rid)
                    outputs = [r] if not isinstance(r, list) else r
                    for oe in outputs:
                        src_path = Path(oe.path)
                        semantic = f"{oe.id}_{in_stem}{src_path.suffix}"
                        new_path = src_path.parent / semantic
                        if src_path.exists():
                            shutil.move(str(src_path), str(new_path))
                        oe.path = str(new_path)
                        if inp_entry:
                            for t in inp_entry.tags:
                                tag_name = t.name
                                if tag_name not in {x.name for x in oe.tags}:
                                    tag = self.session.query(Tag).filter_by(name=tag_name).first()
                                    if tag:
                                        oe.tags.append(tag)
                    all_entries.extend(outputs)
                entries = all_entries
                if step.export:
                    export_path = self.storage.create_export_file(step.export, entries[0].id)
                    shutil.copy(entries[0].path, export_path)
                if debug:
                    for e in entries:
                        print(f"  Output ID: {e.id}, Path: {e.path}")
                    print("-------------------------------------------")
                self.context[step.output_var] = [e.id for e in entries]
                self._log_step(step, entries[0].id)
                continue

            step_hash = self._generate_step_hash(
                processor=step.processor,
                func_hash=process_desc["hash"],
                input_ids=resolved_ids,
                params=step.params
            )

            # ── 多输出缓存 (StepCache 多行同 hash, 含 group_name) ──
            if process_desc["output_type"] == "multi" and step.cache and not step.force_rerun:
                cached_rows = self.session.query(StepCache).filter(
                    StepCache.input_hash == step_hash
                ).order_by(StepCache.id).all()
                if cached_rows:
                    cached_ids = [c.output_id for c in cached_rows]
                    if all(self.session.query(DataEntry).get(eid) for eid in cached_ids):
                        if step.outputs:
                            # 命名多输出: 按 group_name 恢复 context
                            for c in cached_rows:
                                if c.group_name and c.group_name in step.outputs:
                                    self.context[step.outputs[c.group_name]] = [c.output_id]
                        else:
                            self.context[step.output_var] = cached_ids
                        if debug:
                            print(f"  [cache] multi: {[c.output_id for c in cached_rows]}")
                            print("-------------------------------------------")
                        continue

            # ── 单输出缓存检查 ──
            if process_desc["output_type"] != "multi":
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
            if process_desc["output_type"] == "multi" and step.cache and not step.force_rerun:
                cached_rows = self.session.query(StepCache).filter(
                    StepCache.input_hash == step_hash
                ).order_by(StepCache.id).all()
                if cached_rows:
                    cached_ids = [c.output_id for c in cached_rows]
                    # 验证所有条目都存在
                    if all(self.session.query(DataEntry).get(eid) for eid in cached_ids):
                        self.context[step.output_var] = cached_ids
                        if debug:
                            print(f"  [cache] multi-output: {cached_ids}")
                            print("-------------------------------------------")
                        continue

            if debug:
                print("Pipeline Step: ", step.processor, "Inputs: ", step.inputs, "Params: ", step.params)

            result = processor.run(
                processor_name=step.processor,
                input_ids=resolved_ids,
                **step.params
            )

            # ── 命名多输出: processor 返回 {"group_a": [entries], ...} ──
            if isinstance(result, dict):
                for group_name, group_entries in result.items():
                    var_name = step.output_var
                    if step.outputs and group_name in step.outputs:
                        var_name = step.outputs[group_name]
                    elif group_name != step.output_var:
                        var_name = f"{step.output_var}_{group_name}"
                    self.context[var_name] = [e.id for e in group_entries]
                    # 缓存 (含 group_name)
                    if step.cache:
                        for e in group_entries:
                            self.session.add(StepCache(
                                input_hash=step_hash, output_id=e.id, group_name=group_name))
                    if debug:
                        print(f"  [{step.output_var}] group '{group_name}' → ${var_name}: {[e.id for e in group_entries]}")
                if debug:
                    print("-------------------------------------------")
                continue

            is_multi = isinstance(result, list)
            entries = result if is_multi else [result]

            # 记录缓存（单输出）
            if step.cache and not is_multi:
                self.session.add(StepCache(
                    input_hash=step_hash,
                    output_id=entries[0].id,
                ))

            # 导出
            if step.export:
                first = entries[0]
                export_path = self.storage.create_export_file(step.export, first.id)
                shutil.copy(first.path, export_path)

            if debug:
                for e in entries:
                    print(f"  Output ID: {e.id}, Path: {e.path}")
                if step.export:
                    print(f"  Exported To: {export_path}")
                print("-------------------------------------------")

            # 缓存多输出 (list)
            if step.cache and is_multi:
                for e in entries:
                    self.session.add(StepCache(input_hash=step_hash, output_id=e.id))

            self.context[step.output_var] = [e.id for e in entries]
            self._log_step(step, entries[0].id)

        # 记录撤销日志 (包含初始加载和所有步骤)
        all_ids = []
        for var, ids in self.context.items():
            all_ids.extend(ids)
        if all_ids:
            from .undo import UndoLog
            UndoLog().record(all_ids, f"pipeline ({len(all_ids)} entries)")

        return self.context

    def _get_file_mtime(self, file_path: str) -> float:
        """获取文件修改时间（UTC时间戳）"""
        return os.path.getmtime(file_path)

    def _load_initial_files(self, config: InitialLoadConfig,
                                debug: bool = False) -> Dict[str, List[int]]:
        """加载初始文件, 返回 {source_name: [entry_id, ...]}"""
        # source_mode=raw: 直接用 DB 已有的 raw 数据
        exp_config = experiment_manager.get_experiments().get(experiment_manager.current_experiment, {})
        if exp_config.get("source_mode") == "raw":
            existing = self.session.query(DataEntry).filter(
                DataEntry.type == config.data_type
            ).order_by(DataEntry.id).all()
            if existing:
                if debug:
                    click.echo(f"  [source_mode=raw] 复用 {len(existing)} 条已有记录")
                # 按 source 分组 (默认 initial)
                result = {"initial": [e.id for e in existing]}
                if debug:
                    for e in existing:
                        click.echo(f"    {e.id}: {e.path}")
                    print("-------------------------------------------")
                return result

        source_buckets: Dict[str, List[Path]] = {}
        spec_tags: Dict[str, List[str]] = {}
        for spec in config.include_patterns:
            matches = glob.glob(spec.path, recursive=True)

            # 正则过滤
            if spec.re_pattern:
                try:
                    _re = re.compile(spec.re_pattern)
                    matches = [m for m in matches if _re.search(m)]
                except re.error as e:
                    raise ValueError(f"无效的正则表达式 '{spec.re_pattern}': {e}")

            # 排序 + limit
            if spec.sort_by or spec.sort_key:
                if spec.sort_key:
                    try:
                        _kr = re.compile(spec.sort_key)
                        def _kf(f, _r=_kr):
                            m = _r.search(str(f)); return m.group(1) if m else ""
                    except re.error:
                        _kf = str
                else:
                    _kf = str
                if spec.sort_by == "name_desc":
                    matches.sort(key=_kf, reverse=True)
                elif spec.sort_by == "mtime":
                    matches.sort(key=lambda f: os.path.getmtime(f), reverse=True)
                elif spec.sort_by == "mtime_asc":
                    matches.sort(key=lambda f: os.path.getmtime(f))
                else:
                    matches.sort(key=_kf)
            if spec.limit is not None and spec.limit > 0:
                matches = matches[:spec.limit]

            # 全局排除
            for ex in (config.exclude_patterns or []):
                matches = [m for m in matches if not Path(m).match(ex)]

            src = spec.source
            source_buckets.setdefault(src, []).extend(matches)
            spec_tags.setdefault(src, []).extend(spec.tags or [])

        if not any(v for v in source_buckets.values()):
            raise FileNotFoundError(
                f"未找到匹配文件: {[s.path for s in config.include_patterns]}"
            )

        result: Dict[str, List[int]] = {}
        for src, files in source_buckets.items():
            files = sorted(set(files))
            src_entries = []
            for file_path in files:
                current_mtime = self._get_file_mtime(file_path)
                cache = self.session.query(FileMTimeCache).filter(
                    FileMTimeCache.file_path == file_path
                ).order_by(FileMTimeCache.created_at.desc()).first()

                if cache and cache.last_mtime == current_mtime:
                    entry = self.session.query(DataEntry).get(cache.data_entry_id)
                    if entry is None:
                        self.session.delete(cache)
                        self.session.flush()
                        cache = None
                    if debug:
                        print(f"已缓存初始文件： {file_path} ，ID: {entry.id}")
                else:
                    entry = self.storage.store_raw_data(file_path, self.session)
                    entry.type = config.data_type
                    entry.description = f"自动加载自: {file_path}"
                    self.session.flush()
                    self.session.add(FileMTimeCache(
                        file_path=file_path, data_entry_id=entry.id, last_mtime=current_mtime
                    ))
                    if debug:
                        print(f"重新加载初始文件 {file_path} ，ID: {entry.id}")

                # 每源标签 + 全局标签
                for tn in (spec_tags.get(src, []) + (config.tags or [])):
                    tag = self.session.query(Tag).filter_by(name=tn).first()
                    if not tag:
                        tag = Tag(name=tn)
                        self.session.add(tag)
                    if tag not in entry.tags:
                        entry.tags.append(tag)

                src_entries.append(entry)

            self.session.commit()
            result[src] = [e.id for e in src_entries]
            if debug:
                for e in src_entries:
                    print(f"  [{src}] ID {e.id}: {e.path}")

        if debug:
            print("-------------------------------------------")
        return result
 
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