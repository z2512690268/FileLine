# commands/pipeline_commands.py
import click
import yaml
from pathlib import Path
import shutil
import re
from core.pipeline import PipelineRunner, PipelineStep, InitialLoadConfig, IncludeSpec
from core.storage import FileStorage
from core.models import DataEntry, Tag
from core.base import get_session

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
    
@pipeline.command()
@click.argument("config_file")
@click.option("--global-config", type=click.Path(exists=True),
             help="全局配置文件路径（包含变量定义）")
@click.option("--debug/--no-debug", default=True)
def run(config_file, global_config, debug):
    """运行带文件加载的流水线"""
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
                sort_by=p.get("sort_by", None),
                sort_key=p.get("sort_key", None),
                limit=p.get("limit", None),
            ) for p in config["initial_load"]["include"]
        ],
        exclude_patterns=config["initial_load"].get("exclude", []),
        data_type=config["initial_load"].get("type", "raw"),
        tags=config["initial_load"].get("global_tags", [])
    )
    
    # 解析处理步骤 (支持 output 单变量 或 outputs 命名多输出)
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