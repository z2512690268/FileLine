# 修改 main.py
import click
from core.base import experiment_manager
from commands.data_commands import data
from commands.process_commands import process
from commands.experiment_commands import experiment
from commands.pipeline_commands import pipeline
from typing import Optional

@click.group()
@click.option("--experiment", "-e", help="临时指定实验名称（不持久化）")
@click.option("--confirm-exp", is_flag=True, help="执行前要求确认当前实验")
@click.pass_context
def cli(ctx, experiment: Optional[str], confirm_exp: bool):
    """实验管理平台"""
    if experiment:
        experiment_manager.set_current(experiment, persist=False)

    cur = experiment_manager.current_experiment
    if cur is None:
        click.secho("⚠ 当前没有激活的实验，请先创建或选择", fg="yellow", err=True)
    else:
        click.secho(f"═══════════════════════════════════════════", fg="bright_black")
        click.secho(f"  当前实验: {cur}", fg="cyan", bold=True)
        click.secho(f"═══════════════════════════════════════════", fg="bright_black")

    if confirm_exp and cur:
        click.confirm(f"当前实验是 [{cur}]，确认继续？", abort=True)

    # 延迟初始化数据库
    try:
        from core.base import init_db
        init_db()
    except (RuntimeError, TypeError, KeyError):
        pass

cli.add_command(data)
cli.add_command(process)
cli.add_command(experiment)
cli.add_command(pipeline)

if __name__ == "__main__":
    cli()