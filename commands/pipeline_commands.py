# commands/pipeline_commands.py
import click
import yaml
from pathlib import Path
from core.pipeline import PipelineRunner, PipelineStep, InitialLoadConfig
from core.storage import FileStorage
from core.models import DataEntry, Tag
from core.base import get_session

@click.group()
def pipeline():
    """流水线处理命令"""
    pass

@pipeline.command()
@click.argument("config_file")
@click.option("--debug/--no-debug", default=False)
def run(config_file, debug):
    """运行带文件加载的流水线"""
    config = yaml.safe_load(Path(config_file).read_text())
    
    # 解析初始加载配置
    load_config = InitialLoadConfig(
        path_pattern=config["initial_load"]["path"],
        data_type=config["initial_load"].get("type", "raw"),
        tags=config["initial_load"].get("tags", [])
    )
    
    # 解析处理步骤
    steps = [
        PipelineStep(
            processor=step["processor"],
            inputs=step.get("inputs", "initial"),
            params=step.get("params", {}),
            output_var=step["output"],
            cache=step.get("cache", True),
            force_rerun=step.get("force_rerun", False)
        ) for step in config["steps"]
    ]
    
    # 执行流水线
    with get_session() as session:
        runner = PipelineRunner(FileStorage(), session)
        result = runner.execute(load_config, steps, debug)
        click.echo(f"最终输出ID: {result[config['final_output']][0]}")
        # 获取entry
        entry = session.query(DataEntry).get(result[config['final_output']][0])
        click.echo(f"输出文件路径: {entry.path}")
# 示例YAML配置
"""
initial_load:
  path: "******"  # 使用**匹配子目录
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