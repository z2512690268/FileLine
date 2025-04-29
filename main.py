# 修改 main.py
import click
from core.base import experiment_manager
from commands.data_commands import data
from commands.process_commands import process
from commands.experiment_commands import experiment
from typing import Optional

@click.group()
@click.option("--experiment", "-e", help="临时指定实验名称（不持久化）")
def cli(experiment: Optional[str]):
    """实验管理平台"""
    if experiment:
        # 临时指定实验（不持久化）
        experiment_manager.set_current(experiment, persist=False)
    elif experiment_manager.current_experiment is None:
        # 自动加载持久化的当前实验
        pass
    
    # 延迟初始化数据库
    try:
        from core.base import init_db
        init_db()
    except RuntimeError as e:
        if experiment is None:
            click.secho("提示：当前没有激活的实验，请先创建或选择实验", fg="yellow")

cli.add_command(data)
cli.add_command(process)
cli.add_command(experiment)

if __name__ == "__main__":
    cli()