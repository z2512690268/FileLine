# commands/data_commands/add.py
import click
from pathlib import Path
from core.base import get_session
from core.models import DataEntry
from core.storage import FileStorage

@click.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--description", help="数据描述信息")
def add_cmd(file_path, description):
    """添加原始数据文件"""
    storage = FileStorage()
    target_path = storage.store_raw_data(file_path, 'raw')
    
    try:
        with get_session() as session:
            entry = DataEntry(
                type='raw',
                path=str(target_path.absolute()),
                description=description
            )
            session.add(entry)
            session.commit()
            entry_id = entry.id
        click.secho(f"成功添加数据 ID: {entry_id}", fg='green')
    except Exception as e:
        click.secho(f"添加失败: {str(e)}", fg='red')