# 新增 commands/experiment_commands.py
import click
from pathlib import Path
from tabulate import tabulate
from core.base import experiment_manager

@click.group()
def experiment():
    """实验管理命令"""
    click.echo(f"当前实验环境 ：{experiment_manager.current_experiment}")
    pass

@experiment.command()
@click.argument("name")
@click.option("--description", help="实验描述")
def create(name, description):
    """创建新实验"""
    try:
        experiment_manager.create(name, description)
        click.secho(f"成功创建实验: {name}", fg='green')
    except Exception as e:
        click.secho(f"创建失败: {str(e)}", fg='red')

@experiment.command()
@click.argument("name")
def use(name):
    """切换当前实验"""
    try:
        experiment_manager.set_current(name)
        click.secho(f"已切换到实验: {name}", fg='green')
    except ValueError as e:
        click.secho(str(e), fg='red')

@experiment.command()
def list():
    """列出所有实验"""
    experiments = experiment_manager.get_experiments()
    table = []
    for name, config in experiments.items():
        table.append([
            name,
            config['description'],
            Path(config['database']).parent,
            config['created_at']
        ])
    click.echo(tabulate(table, 
        headers=["名称", "描述", "目录", "创建时间"],
        tablefmt="fancy_grid"))

@experiment.command()
@click.argument("name")
def delete(name):
    """删除实验"""
    try:
        experiments = experiment_manager.get_experiments()
        if name not in experiments:
            raise ValueError(f"实验 {name} 不存在")
        exp_dir = experiment_manager.base_path
        if exp_dir.exists():
            import shutil
            shutil.rmtree(exp_dir)
        del experiments[name]
        experiment_manager._save_experiments(experiments)
        click.secho(f"成功删除实验: {name}", fg='green')
        experiment_manager.set_current(list(experiments.keys())[0])
    except Exception as e:
        click.secho(f"删除失败: {str(e)}", fg='red')