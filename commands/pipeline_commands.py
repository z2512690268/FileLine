# commands/pipeline_commands.py
import inspect
import click
import yaml
from pathlib import Path
import shutil
import re
from core.pipeline import PipelineRunner, PipelineStep, InitialLoadConfig, IncludeSpec
from core.storage import FileStorage
from core.models import DataEntry, Tag
from core.base import get_session, experiment_manager

@click.group()
def pipeline():
    """流水线处理命令"""
    pass

def parse_simple_config(config_text: str) -> dict:
    """解析 key=value 格式的配置文本"""
    config = {}
    for line in config_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
    return config

def replace_in_text(content: str, variables: dict) -> str:
    """带严格模式检查的文本替换"""
    pattern = re.compile(r"\$\{([A-Z0-9_]+)\}")  # 严格匹配大写变量
    
    def replacer(match):
        var_name = match.group(1)
        return variables.get(var_name, match.group(0))  # 保留未替换的原始格式
        
    return pattern.sub(replacer, content)

def validate_placeholders(content: str):
    """检查未替换的占位符"""
    remaining = set(re.findall(r"\$\{([A-Z0-9_]+)\}", content))
    if remaining:
        raise click.UsageError(
            f"发现未替换的配置变量: {', '.join(remaining)}\n"
            "解决方案：使用 --global-config 指定全局配置文件, 并确保所有待替换变量都在其中。\n"
        )
    
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


def _print_dry_run(config_file, global_config, debug):
    """打印初始加载匹配结果, 不导入"""
    variables = {}
    if global_config:
        variables = parse_simple_config(Path(global_config).read_text())
    raw = Path(config_file).read_text()
    processed = replace_in_text(raw, variables)
    validate_placeholders(processed)
    config = yaml.safe_load(processed)

    inc = config["initial_load"]["include"]
    click.echo(f"═══ Dry-Run: {config_file} ═══")
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
@click.option("--debug/--no-debug", default=True)
@click.option("--dry-run", is_flag=True, help="仅预览匹配文件, 不导入不执行")
def run(config_file, global_config, debug, dry_run):
    """运行带文件加载的流水线"""
    if dry_run:
        _print_dry_run(config_file, global_config, debug)
        return

    # 读取变量定义
    variables = {}
    if global_config:
        var_text = Path(global_config).read_text()
        variables = parse_simple_config(var_text)

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

    # ── 自动快照: 把 pipeline/processor 当前版本存入实验目录 ──
    exp_name = experiment_manager.current_experiment
    if exp_name:
        from core.processing import load_processors_from_dir
        load_processors_from_dir(
            experiment_manager.project_root / "experiments" / exp_name / "processors"
        )
        snap_pip_dir = experiment_manager.project_root / "experiments" / exp_name / "pipelines"
        snap_proc_dir = experiment_manager.project_root / "experiments" / exp_name / "processors"
        snap_pip_dir.mkdir(parents=True, exist_ok=True)
        snap_proc_dir.mkdir(parents=True, exist_ok=True)

        # 复制 pipeline YAML
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
        runner = PipelineRunner(FileStorage(), session)
        result = runner.execute(load_config, steps, debug)
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

"""
initial_load:
  path: ""  # 匹配子目录
  type: raw
  tags: [origin]

steps:
  - processor: parse_runtime
    inputs: initial
    output: parsed
    cache: False

  - processor: csv_multiplier
    inputs: parsed
    output: multiplied
    params:
      multiplier: 2.0

final_output: multiplied
"""