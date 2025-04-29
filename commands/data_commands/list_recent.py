# commands/data_commands/list_recent.py
import click
from datetime import datetime
from sqlalchemy import desc
from core.base import get_session
from core.models import DataEntry

def format_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

@click.command()
@click.option("--limit", default=5, show_default=True, 
             help="显示最近的记录数量")
def list_recent_cmd(limit):
    """显示最近的实验数据条目"""
    with get_session() as session:
        entries = session.query(DataEntry).order_by(
            desc(DataEntry.timestamp)
        ).limit(limit).all()

        if not entries:
            click.echo("暂无数据记录")
            return

        click.echo(f"\n最新 {limit} 条数据记录：")
        for entry in entries:
            tags = ', '.join(t.name for t in entry.tags) or "无"
            click.echo(f"ID: {entry.id}")
            click.echo(f"  路径: {entry.path}")
            click.echo(f"  类型: {entry.type.upper()}")
            click.echo(f"  描述: {entry.description}")
            click.echo(f"  时间: {format_ts(entry.timestamp)}")
            click.echo(f"  标签: {tags}")
            click.echo("-" * 60)