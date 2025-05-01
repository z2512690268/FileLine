# commands/data_commands/show.py
import click
from sqlalchemy import or_, and_
from core.base import get_session
from core.models import DataEntry, Tag
from datetime import datetime

def format_ts(dt: datetime) -> str:
    """统一时间格式化"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

@click.command()
@click.option("--id", "-i", multiple=True, type=int, help="筛选指定ID的数据")
@click.option("--tag", "-t", multiple=True, help="筛选包含指定标签的数据")
@click.option("--type", type=click.Choice(['raw', 'processed', 'plot']), help="按数据类型筛选")
@click.option("--limit", default=20, show_default=True, help="最大显示条目数")
def show_cmd(id, tag, type, limit):
    """根据条件展示数据条目"""
    with get_session() as session:
        # 构建查询条件
        query = session.query(DataEntry)
        filters = []
        
        if id:
            filters.append(DataEntry.id.in_(id))
        
        if tag:
            tag_conditions = [Tag.name == t for t in tag]
            tag_subq = session.query(DataEntry.id).join(DataEntry.tags).filter(or_(*tag_conditions)).subquery()
            filters.append(DataEntry.id.in_(tag_subq))
        
        if type:
            filters.append(DataEntry.type == type)
        
        if filters:
            query = query.filter(and_(*filters))
        
        entries = query.order_by(DataEntry.timestamp.desc()).limit(limit).all()

        if not entries:
            click.secho("未找到匹配的数据条目", fg='yellow')
            return

        # 按照指定格式输出
        click.echo(f"\n找到 {len(entries)} 条匹配记录：")
        for idx, entry in enumerate(entries, 1):
            tags = ', '.join(t.name for t in entry.tags) or "无"
            desc = entry.description
            click.echo(f"【记录 {idx}】")
            click.echo(f"  ID: {entry.id}")
            click.echo(f"  路径: {entry.path}")
            click.echo(f"  类型: {entry.type.upper()}")
            click.echo(f"  描述: {desc}")
            click.echo(f"  时间: {format_ts(entry.timestamp)}")
            click.echo(f"  标签: {tags}")
            click.echo("-" * 60)