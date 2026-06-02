"""data undo: 撤回上一步操作"""
import click
from pathlib import Path
from sqlalchemy import or_
from core.base import get_session, experiment_manager
from core.models import DataEntry, FileMTimeCache, StepCache, DataRelationship
from core.storage import FileStorage
from core.undo import UndoLog


@click.command()
@click.option("--steps", default=1, help="撤回步数")
@click.option("--list", "list_only", is_flag=True, help="仅查看可撤回的操作")
@click.option("--yes", "-y", is_flag=True, help="跳过确认")
def undo_cmd(steps, list_only, yes):
    """撤回最近的操作（删除产生的数据和文件）"""
    log = UndoLog()

    if log.count == 0:
        click.secho("当前实验没有可撤回的操作", fg="yellow")
        return

    batches = log.peek(steps)
    click.secho(f"当前实验: {experiment_manager.current_experiment}", fg="cyan")
    for i, b in enumerate(batches):
        desc = b.get("description", "")
        n = len(b["entry_ids"])
        click.echo(f"  #{i+1}  [{b['timestamp'][:19]}] {desc or f'{n} 条记录'}")
        click.echo(f"       ID: {', '.join(str(e) for e in b['entry_ids'])}")

    if list_only:
        return

    if not yes:
        click.confirm(f"将撤回以上 {len(batches)} 步，确认？", abort=True)

    # 执行撤回
    storage = FileStorage()
    with get_session() as session:
        for batch in log.pop(steps):
            ids = batch["entry_ids"]
            entries = session.query(DataEntry).filter(DataEntry.id.in_(ids)).all()

            for entry in entries:
                # 删除磁盘文件
                fp = Path(entry.path)
                if fp.exists():
                    fp.unlink()
                # 清理 exports.meta 和导出文件
                for name in list(storage._meta_cache.keys()):
                    if storage._meta_cache[name].get("id") == entry.id:
                        export_file = storage.base_path / "exports" / name
                        if export_file.exists():
                            export_file.unlink()
                        del storage._meta_cache[name]
                # 清理缓存
                session.query(FileMTimeCache).filter(
                    FileMTimeCache.data_entry_id == entry.id
                ).delete()
                session.query(StepCache).filter(
                    StepCache.output_id == entry.id
                ).delete()
                # 清理血缘
                session.query(DataRelationship).filter(
                    or_(DataRelationship.parent_id == entry.id,
                        DataRelationship.child_id == entry.id)
                ).delete()
                session.delete(entry)

        storage._save_exports_meta()
        session.commit()

    click.secho(f"已撤回 {len(batches)} 步，删除 {sum(len(b['entry_ids']) for b in batches)} 条记录", fg="green")
