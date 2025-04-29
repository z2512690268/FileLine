# commands/data_commands/list_between.py
import click
from datetime import datetime
from sqlalchemy import between
from core.base import get_session
from core.models import DataEntry

def adjust_time_range(start: datetime, end: datetime) -> tuple:
    """调整时间范围到全天"""
    if start.time() == datetime.min.time():
        start = start.replace(hour=0, minute=0, second=0)
    if end.time() == datetime.min.time():
        end = end.replace(hour=23, minute=59, second=59)
    return start, end

@click.command()
@click.argument("start", type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]))
@click.argument("end", type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]))
def list_between_cmd(start, end):
    """按时间范围查询数据条目"""
    start, end = adjust_time_range(start, end)
    
    with get_session() as session:
        query = session.query(DataEntry).filter(
            DataEntry.timestamp.between(start, end)
        ).order_by(DataEntry.timestamp)
        
        if not query.count():
            click.secho(f"{start} ~ {end} 期间无数据", fg='yellow')
            return

        click.echo(f"\n时间段 {start} 至 {end} 的数据：")
        for entry in query:
            click.echo(f"[ID:{entry.id}] {entry.description}")
            click.echo(f"路径: {entry.path}")
            click.echo(f"时间: {entry.timestamp.isoformat(sep=' ')}")
            click.echo("-" * 60)