# commands/data_commands/tag.py
import click
from core.base import get_session
from core.models import DataEntry, Tag

@click.command()
@click.argument("data_id", type=int)
@click.argument("tags", nargs=-1)
def tag_cmd(data_id, tags):
    """为数据条目添加标签"""
    with get_session() as session:
        entry = session.get(DataEntry, data_id)
        if not entry:
            click.secho(f"未找到ID为 {data_id} 的数据", fg='red')
            return

        for tag_name in tags:
            tag = session.query(Tag).filter_by(name=tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
                session.add(tag)
            if tag not in entry.tags:
                entry.tags.append(tag)
        
        session.commit()
        click.secho(f"成功为ID {data_id} 添加标签: {', '.join(tags)}", fg='green')