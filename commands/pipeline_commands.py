# commands/pipeline_commands.py
import inspect
import json
import os
import click
import yaml
from pathlib import Path
import shutil
import re
from core.pipeline import PipelineRunner, PipelineStep, InitialLoadConfig, IncludeSpec
from core.storage import FileStorage
from core.models import DataEntry, Tag
from core.base import get_session, experiment_manager
from core.global_sets import (
    load_global_values,
    parse_legacy_global_text,
    replace_in_text as replace_global_text,
    used_variables,
)

@click.group()
def pipeline():
    """流水线处理命令"""
    pass

def parse_simple_config(config_text: str) -> dict:
    """解析 key=value 格式的配置文本"""
    return parse_legacy_global_text(config_text)

def replace_in_text(content: str, variables: dict) -> str:
    """带严格模式检查的文本替换"""
    return replace_global_text(content, variables)

def validate_placeholders(content: str):
    """检查未替换的占位符"""
    remaining = set(used_variables(content))
    if remaining:
        raise click.UsageError(
            f"发现未替换的配置变量: {', '.join(remaining)}\n"
            "解决方案：使用 --global-set 或 --global-config 指定变量组, 并确保所有待替换变量都在其中。\n"
        )


def _parse_override_options(pairs: tuple[str, ...]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for item in pairs or ():
        if "=" not in item:
            raise click.UsageError("--set 需要 KEY=value 格式")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise click.UsageError("--set 的变量名不能为空")
        overrides[key] = value.strip()
    return overrides


def _global_context(global_config: str | None, global_set: str | None, set_values: tuple[str, ...]):
    if global_config and global_set:
        raise click.UsageError("--global-config 和 --global-set 只能选择一种")
    overrides = _parse_override_options(set_values)
    values, info = load_global_values(
        global_set=global_set,
        global_config=global_config,
        overrides=overrides,
        experiment=experiment_manager.current_experiment,
    )
    return values, info


def _record_config_path(config_file: str) -> str:
    env_path = os.environ.get("FILELINE_PIPELINE_PATH")
    if env_path:
        return env_path.replace("\\", "/")
    path = Path(config_file).resolve()
    root = (Path.cwd() / "FileLine-Pipelines").resolve()
    try:
        return str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        return Path(config_file).name

def _match_initial_files(config: dict) -> list:
    """根据 YAML 的 initial_load 配置模拟文件匹配, 返回 [(spec_index, path), ...]"""
    import glob as _glob
    results = []
    for si, p in enumerate(config["initial_load"]["include"]):
        matches = _glob.glob(p["path"], recursive=True)
        # 正则过滤
        if p.get("regex"):
            try:
                _re = re.compile(p["regex"])
                matches = [m for m in matches if _re.search(m)]
            except re.error:
                pass
        # 排除全局 exclude
        for ex in config["initial_load"].get("exclude", []):
            matches = [m for m in matches if not Path(m).match(ex)]
        # 排序 + limit
        sort_by = p.get("sort_by")
        sort_key_re = p.get("sort_key")
        if sort_by or sort_key_re:
            if sort_key_re:
                try:
                    _kr = re.compile(sort_key_re)
                    def _kf(f, _r=_kr):
                        m = _r.search(str(f)); return m.group(1) if m else ""
                except re.error:
                    _kf = str
            else:
                _kf = str
            if sort_by == "name_desc":
                matches.sort(key=_kf, reverse=True)
            elif sort_by == "mtime":
                matches.sort(key=lambda f: os.path.getmtime(f), reverse=True)
            elif sort_by == "mtime_asc":
                matches.sort(key=lambda f: os.path.getmtime(f))
            else:
                matches.sort(key=_kf)
        if p.get("limit"):
            matches = matches[:p["limit"]]
        for m in matches:
            results.append((si, m))
    return results


def _print_dry_run(config_file, global_config, global_set, set_values, debug):
    """打印初始加载匹配结果, 不导入"""
    variables, global_info = _global_context(global_config, global_set, set_values)
    raw = Path(config_file).read_text()
    processed = replace_in_text(raw, variables)
    validate_placeholders(processed)
    config = yaml.safe_load(processed)

    inc = config["initial_load"]["include"]
    click.echo(f"═══ Dry-Run: {config_file} ═══")
    if global_info.get("set") or global_info.get("source"):
        click.echo(f"  变量组: {global_info.get('set') or Path(global_info.get('source', '')).name}")
    click.echo(f"  包含模式 ({len(inc)} 个):")
    for i, p in enumerate(inc):
        extra = []
        if p.get("regex"): extra.append(f"regex={p['regex']}")
        if p.get("sort_by"): extra.append(f"sort={p['sort_by']}")
        if p.get("sort_key"): extra.append(f"key=/{p['sort_key']}/")
        if p.get("limit"): extra.append(f"limit={p['limit']}")
        click.echo(f"    [{i}] glob: {p['path']}  ({', '.join(extra)})" if extra else f"    [{i}] glob: {p['path']}")

    matches = _match_initial_files(config)
    click.echo(f"\n  匹配到 {len(matches)} 个文件:")
    for si, m in matches:
        click.echo(f"    [{si}] {m}")

    click.echo(f"\n═══ 步骤 ({len(config['steps'])}) ═══")
    for i, s in enumerate(config["steps"]):
        out = s.get("output") or ",".join(s.get("outputs", {}).values())
        inp = s.get("inputs", "initial")
        if isinstance(inp, list):
            inp = ",".join(inp)
        click.echo(f"    {i+1}. {s['processor']:25s}  {inp:15s}  → {out}")


@pipeline.command()
@click.argument("config_file")
@click.option("--global-config", type=click.Path(exists=True),
             help="全局配置文件路径（包含变量定义）")
@click.option("--global-set", "global_set", help="使用 FileLine 全局变量组/样式预设")
@click.option("--set", "set_values", multiple=True, help="临时覆盖变量, 格式 KEY=value，可重复")
@click.option("--debug/--no-debug", default=True)
@click.option("--dry-run", is_flag=True, help="仅预览匹配文件, 不导入不执行")
@click.option("--fresh-scope", is_flag=True, help="为本次运行创建新的缓存作用域")
@click.option("--source-mode", type=click.Choice(["version", "raw", "external", "auto"]), default=None,
              help="本次运行的数据源模式: version=复用当前pipeline版本的raw数据, raw=复用已登记raw数据, external/auto=按YAML真实源重新匹配/拉取")
def run(config_file, global_config, global_set, set_values, debug, dry_run, fresh_scope, source_mode):
    """运行带文件加载的流水线"""
    if dry_run:
        _print_dry_run(config_file, global_config, global_set, set_values, debug)
        return

    # 读取变量定义
    variables, global_info = _global_context(global_config, global_set, set_values)

    # 处理主配置
    raw_config = Path(config_file).read_text()
    processed_config = replace_in_text(raw_config, variables)

    # 检查未替换的占位符
    validate_placeholders(processed_config)

    config = yaml.safe_load(processed_config)

    # 解析初始加载配置
    load_config = InitialLoadConfig(
        include_patterns=[
            IncludeSpec(
                path=p["path"],
                re_pattern=p.get("regex", None),
                tags=p.get("tags", []),
                source=p.get("source", "initial"),
                remote=p.get("remote", None),
                sort_by=p.get("sort_by", None),
                sort_key=p.get("sort_key", None),
                limit=p.get("limit", None),
            ) for p in config["initial_load"]["include"]
        ],
        exclude_patterns=config["initial_load"].get("exclude", []),
        data_type=config["initial_load"].get("type", "raw"),
        tags=config["initial_load"].get("global_tags", []),
    )

    # 解析处理步骤
    steps = []
    for step in config["steps"]:
        out_var = step.get("output", "")
        out_dict = step.get("outputs", None)
        steps.append(PipelineStep(
            processor=step["processor"],
            inputs=step.get("inputs", "initial"),
            params=step.get("params", {}),
            output_var=out_var,
            outputs=out_dict,
            cache=step.get("cache", True),
            force_rerun=step.get("force_rerun", False),
            export=step.get("export", None)
        ))

    # ── 加载 pipeline 同目录的 processor ──
    from core.processing import load_processors_from_dir
    pip_proc = Path(config_file).parent / "processors"
    if pip_proc.is_dir():
        load_processors_from_dir(pip_proc)
    if experiment_manager.current_experiment:
        exp_proc = experiment_manager.base_path / "processors"
        if exp_proc.is_dir():
            load_processors_from_dir(exp_proc)

    from core.pipeline_versions import PipelineVersionManager
    processor_snapshot = PipelineVersionManager.build_processor_snapshot(
        step["processor"] for step in config["steps"]
    )

    # ── 自动快照: 把 pipeline/processor 当前版本存入实验目录 ──
    exp_name = experiment_manager.current_experiment
    if exp_name:
        snap_pip_dir = experiment_manager.project_root / "experiments" / exp_name / "pipelines"
        snap_proc_dir = experiment_manager.project_root / "experiments" / exp_name / "processors"
        snap_pip_dir.mkdir(parents=True, exist_ok=True)
        snap_proc_dir.mkdir(parents=True, exist_ok=True)

        # 复制 pipeline YAML (原样保留, 含 remote)
        import shutil as _sh
        yaml_dst = snap_pip_dir / Path(config_file).name
        _sh.copy2(config_file, yaml_dst)
        if global_config:
            _sh.copy2(global_config, snap_pip_dir / Path(global_config).name)

        # 复制用到的 processor 源文件
        for step in config["steps"]:
            pname = step["processor"]
            try:
                proc_info = ProcessorRegistry.get_processor(pname)
                src_file = Path(proc_info.get("source_file", ""))
                if src_file.exists() and src_file.suffix == ".py":
                    if not str(src_file).startswith(str(snap_proc_dir)):
                        dst = snap_proc_dir / src_file.name
                        _sh.copy2(str(src_file), str(dst))
            except Exception:
                pass

    # 执行流水线
    with get_session() as session:
        pvm = PipelineVersionManager()
        record_config = _record_config_path(config_file)
        run_cache_scope = pvm.create_fresh_scope() if fresh_scope else pvm.get_current_cache_scope(record_config)
        requested_source_mode = source_mode or os.environ.get("FILELINE_SOURCE_MODE") or ""
        if requested_source_mode == "version" and not fresh_scope:
            current_version = pvm.get_current(record_config)
            if current_version and (current_version.get("config_snapshot", "") or "") == (processed_config or ""):
                export_id = current_version.get("export_id")
                export_name = current_version.get("export_name", "")
                if export_id and export_name:
                    entry = session.query(DataEntry).get(export_id)
                    if entry and Path(entry.path).exists():
                        export_path = FileStorage().create_export_file(export_name, entry.id, session)
                        shutil.copyfile(entry.path, export_path)
                        session.commit()
                        click.echo(
                            f"  版本 #{current_version['id']} 已复用 "
                            f"(versioned data + unchanged pipeline, no new run)"
                        )
                        click.echo(f"  导出到: {export_path}")
                        return
        previous_source_override = os.environ.get("FILELINE_SOURCE_MODE")
        previous_pipeline_path = os.environ.get("FILELINE_PIPELINE_PATH")
        if source_mode:
            os.environ["FILELINE_SOURCE_MODE"] = source_mode
        os.environ["FILELINE_PIPELINE_PATH"] = record_config
        runner = PipelineRunner(FileStorage(), session, cache_scope=run_cache_scope)
        try:
            result = runner.execute(load_config, steps, debug)
        finally:
            if previous_source_override is None:
                os.environ.pop("FILELINE_SOURCE_MODE", None)
            else:
                os.environ["FILELINE_SOURCE_MODE"] = previous_source_override
            if previous_pipeline_path is None:
                os.environ.pop("FILELINE_PIPELINE_PATH", None)
            else:
                os.environ["FILELINE_PIPELINE_PATH"] = previous_pipeline_path
         # 处理所有最终输出配置
        final_outputs = config.get("final_output", [])
        for output_config in final_outputs:
            output_name = output_config["name"]
            export_config = output_config.get("export", None)
            entry_ids = result.get(output_name, [])

            if not entry_ids:
                click.echo(f"未找到输出变量: {output_name}")
                continue

            for entry_id in entry_ids:
                entry = session.query(DataEntry).get(entry_id)
                if entry is None:
                    continue
                click.echo(f"输出 {output_name}: ID={entry_id}, 文件={entry.path}")

                # 导出结果（多输出时只导出第一个）
                if export_config and entry_id == entry_ids[0]:
                    export_path = runner.storage.create_export_file(export_config, entry.id)
                    shutil.copyfile(entry.path, export_path)
                    click.echo(f"  导出到: {export_path}")

        # ── 记录版本 ──
        exp_name = experiment_manager.current_experiment
        if exp_name and result:
            all_ids = []
            export_id = None
            export_name = ""
            for var, ids in result.items():
                all_ids.extend(ids)
            # 取 final_output 第一个导出的 entry id 和 name
            for oc in final_outputs:
                oids = result.get(oc["name"], [])
                if oids:
                    export_id = oids[0]
                    export_name = oc.get("export", "")
                    break
            vid = pvm.record_run(all_ids, config_file=record_config,
                                 export_id=export_id, export_name=export_name,
                                 cache_scope=run_cache_scope,
                                 config_snapshot=processed_config,
                                 processor_snapshot=processor_snapshot,
                                 global_set=global_info.get("set", ""),
                                 global_values_snapshot=json.dumps(global_info, ensure_ascii=False, sort_keys=True))
            click.echo(f"  版本 #{vid} 已记录")
            # 执行成功后设置 source_mode=raw: 后续自动用缓存数据
            exps = experiment_manager.get_experiments()
            if exp_name in exps:
                exps[exp_name]["source_mode"] = "raw"
                experiment_manager._save_experiments(exps)


@pipeline.command("history")
@click.option("--config-file", help="只查看指定 pipeline 的版本 (如 fmrl/timeline.yaml)")
def history(config_file):
    """列出管道运行版本历史"""
    from core.pipeline_versions import PipelineVersionManager
    from core.base import get_session
    from core.models import DataEntry

    pvm = PipelineVersionManager()
    versions = pvm.list_versions(config_file or "")
    if not versions:
        click.secho("暂无管道运行记录", fg="yellow")
        return

    with get_session() as session:
        for v in versions:
            marker = ">" if v.get("status") == "active" else " "
            vid = v["id"]
            ts = v["timestamp"][:19]
            cfg = v.get("config_file", "")
            n = len(v.get("entry_ids", []))
            export_id = v.get("export_id")
            export_info = ""
            if export_id:
                entry = session.query(DataEntry).get(export_id)
                if entry:
                    export_info = f"  export: {Path(entry.path).name}"
            click.echo(f"{marker} #{vid:<4d} [{ts}] {cfg} ({n} entries){export_info}")


def _stamp_version(version):
    """在 exports 目录写入 .version 文件, 标识当前导出对应的版本"""
    from core.pipeline_versions import PipelineVersionManager
    PipelineVersionManager()._stamp_version((version or {}).get("config_file", ""))


def _restore_config_snapshot(version):
    snapshot = (version or {}).get("config_snapshot", "")
    config_file = (version or {}).get("config_file", "")
    if not snapshot or not config_file:
        return
    root = (Path.cwd() / "FileLine-Pipelines").resolve()
    target = (root / config_file).resolve()
    if root not in target.parents:
        click.secho(f"跳过 YAML 恢复: 非法 pipeline 路径 {config_file}", fg="yellow")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(snapshot, encoding="utf-8")
    click.echo(f"  pipeline YAML 已恢复: {target}")


def _restore_processor_snapshot(version):
    snapshot = (version or {}).get("processor_snapshot", "")
    if not snapshot:
        return
    from core.pipeline_versions import PipelineVersionManager
    restored = PipelineVersionManager.restore_processor_snapshot(snapshot)
    if restored:
        click.echo(f"  processors 已恢复: {len(restored)} 个文件")


@pipeline.command("switch")
@click.argument("version_id", type=int)
def switch_version(version_id):
    """切换到指定版本的导出文件, 同时恢复 pipeline 快照"""
    from core.pipeline_versions import PipelineVersionManager
    from core.base import get_session
    from core.models import DataEntry
    from pathlib import Path as P

    pvm = PipelineVersionManager()
    target = pvm.get_version(version_id)
    if not target:
        click.secho(f"版本 #{version_id} 不存在", fg="red")
        return

    current = pvm.get_current(target.get("config_file", ""))
    if current and current["id"] == version_id:
        click.secho(f"版本 #{version_id} 已经是当前版本", fg="yellow")
        return

    export_id = target.get("export_id")
    if not export_id:
        click.secho(f"版本 #{version_id} 没有导出文件", fg="yellow")
        _restore_config_snapshot(target)
        _restore_processor_snapshot(target)
        pvm.set_current(version_id)
        _stamp_version(target)
        click.secho(f"已切换到版本 #{version_id}", fg="green")
        return

    with get_session() as session:
        entry = session.query(DataEntry).get(export_id)
        if not entry or not P(entry.path).exists():
            click.secho(f"版本 #{version_id} 的导出文件 (ID={export_id}) 已不存在", fg="red")
            return

        export_name = target.get("export_name", "")
        if export_name:
            base = experiment_manager.base_path
            export_path = P(base) / "exports" / export_name
            shutil.copyfile(entry.path, str(export_path))
            click.secho(f"已切换到版本 #{version_id}, 导出文件已更新:", fg="green")
            click.echo(f"  {export_path}")
        else:
            click.secho(f"已切换到版本 #{version_id} (export 文件: {entry.path})", fg="green")

    pvm.set_current(version_id)
    _restore_config_snapshot(target)
    _restore_processor_snapshot(target)
    _stamp_version(target)


def _resolve_version_scope(pvm, config_file):
    if config_file:
        return config_file
    active = pvm.list_active_versions()
    if len(active) == 1:
        return active[0].get("config_file", "")
    if not active:
        click.secho("当前没有激活的版本", fg="yellow")
    else:
        click.secho("当前实验有多个 active pipeline 版本，请用 --config-file 指定要切换的 pipeline:", fg="yellow")
        for v in active:
            click.echo(f"  #{v['id']} {v.get('config_file', '')}")
    return None


@pipeline.command("prev")
@click.option("--config-file", help="指定 pipeline (如 fmrl/timeline.yaml)")
def switch_prev(config_file):
    """切换到上一版本"""
    from core.pipeline_versions import PipelineVersionManager
    pvm = PipelineVersionManager()
    scope = _resolve_version_scope(pvm, config_file)
    if not scope:
        return
    versions = pvm.list_versions(scope)
    current = pvm.get_current(scope)
    if not current:
        click.secho("当前没有激活的版本", fg="yellow")
        return
    # versions 是最新的在前, 找 active 之后的那个 (时间更早的)
    for i, v in enumerate(versions):
        if v["id"] == current["id"]:
            if i + 1 < len(versions):
                # 递归调用 switch
                ctx = click.get_current_context()
                ctx.invoke(switch_version, version_id=versions[i + 1]["id"])
            else:
                click.secho("已经是最早的版本", fg="yellow")
            return


@pipeline.command("next")
@click.option("--config-file", help="指定 pipeline (如 fmrl/timeline.yaml)")
def switch_next(config_file):
    """切换到下一版本"""
    from core.pipeline_versions import PipelineVersionManager
    pvm = PipelineVersionManager()
    scope = _resolve_version_scope(pvm, config_file)
    if not scope:
        return
    versions = pvm.list_versions(scope)
    current = pvm.get_current(scope)
    if not current:
        click.secho("当前没有激活的版本", fg="yellow")
        return
    # versions 是最新的在前, 找 active 之前的那个 (时间更新的)
    for i, v in enumerate(versions):
        if v["id"] == current["id"]:
            if i > 0:
                ctx = click.get_current_context()
                ctx.invoke(switch_version, version_id=versions[i - 1]["id"])
            else:
                click.secho("已经是最新的版本", fg="yellow")
            return
