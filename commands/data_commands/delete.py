"""data delete: 删除数据条目并清理关联文件与缓存"""
import click
from pathlib import Path
from sqlalchemy import or_
from core.base import get_session
from core.models import DataEntry, FileMTimeCache, StepCache, DataRelationship
from core.storage import FileStorage


@click.command()
@click.argument("ids", type=int, nargs=-1, required=True)
@click.option("--yes", "-y", is_flag=True, help="跳过确认")
def delete_cmd(ids, yes):
    """删除指定 ID 的数据条目（同时清理磁盘文件与缓存）"""
    storage = FileStorage()
    with get_session() as session:
        entries = session.query(DataEntry).filter(DataEntry.id.in_(ids)).all()
        if not entries:
            click.secho("未找到匹配的数据条目", fg="yellow")
            return

        if not yes:
            click.echo("将删除以下条目：")
            for e in entries:
                click.echo(f"  ID {e.id}: {e.path} ({e.type})")
            click.confirm("确认删除？", abort=True)

        for entry in entries:
            # 1. 删除磁盘文件
            file_path = Path(entry.path)
            if file_path.exists():
                file_path.unlink()
                click.echo(f"  删除文件: {file_path}")

            # 2. 删除导出元信息中的引用及对应文件
            for name in list(storage._meta_cache.keys()):
                if storage._meta_cache[name].get("id") == entry.id:
                    export_file = storage.base_path / "exports" / name
                    if export_file.exists():
                        export_file.unlink()
                        click.echo(f"  删除导出文件: {export_file.relative_to(storage.base_path)}")
                    del storage._meta_cache[name]
                    click.echo(f"  清理 exports.meta: {name}")
            storage._save_exports_meta()

            # 3. 清理缓存表
            session.query(FileMTimeCache).filter(
                FileMTimeCache.data_entry_id == entry.id
            ).delete()
            session.query(StepCache).filter(
                StepCache.output_id == entry.id
            ).delete()

            # 4. 清理血缘关系
            session.query(DataRelationship).filter(
                or_(DataRelationship.parent_id == entry.id,
                    DataRelationship.child_id == entry.id)
            ).delete()

            # 5. 删除条目自身
            session.delete(entry)
            click.secho(f"  已删除条目 ID {entry.id}", fg="green")

        session.commit()
        from core.base import experiment_manager
        click.secho(f"[{experiment_manager.current_experiment}] 成功删除 {len(entries)} 条记录", fg="green")
