# commands/data_commands/__init__.py
import click
from .add import add_cmd
from .tag import tag_cmd
from .list_recent import list_recent_cmd
from .list_between import list_between_cmd
from .show import show_cmd
from .trace import trace_cmd
@click.group()
def data():
    """数据管理命令集"""
    pass

data.add_command(add_cmd, name="add")
data.add_command(tag_cmd, name="tag")
data.add_command(list_recent_cmd, name="list-recent")
data.add_command(list_between_cmd, name="list-between")
data.add_command(show_cmd, name="show")
data.add_command(trace_cmd, name="trace")