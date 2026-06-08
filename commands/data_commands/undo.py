"""data undo: 回退到上一版本（非破坏性）；--hard 可执行旧版破坏性撤销"""
import click
from pathlib import Path
from sqlalchemy import or_
from core.base import get_session, experiment_manager
from core.models import DataEntry, FileMTimeCache, StepCache, DataRelationship
from core.storage import FileStorage
from core.undo import UndoLog


@click.command()
@click.option("--steps", default=1, help="回退步数")
@click.option("--list", "list_only", is_flag=True, help="仅查看可回退的版本")
@click.option("--yes", "-y", is_flag=True, help="跳过确认")
@click.option("--hard", is_flag=True, help="破坏性撤销: 删除 DB 记录和磁盘文件")
@click.option("--config-file", help="指定要回退的 pipeline (如 fmrl/timeline.yaml)")
def undo_cmd(steps, list_only, yes, hard, config_file):
    """回退到上一管道运行版本。

    默认行为 (非破坏性):
      通过 pipeline 版本管理切换 current 指针并更新 exports/ 文件,
      不会删除 DB 记录和磁盘数据。

    --hard:
      执行旧版破坏性撤销, 真正删除 DataEntry 记录和磁盘文件。
    """
    if hard:
        _hard_undo(steps, list_only, yes)
        return

    # ── 非破坏性回退 ──
    from core.pipeline_versions import PipelineVersionManager
    from core.models import DataEntry

    pvm = PipelineVersionManager()
    versions = pvm.list_versions(config_file or "")

    if not versions:
        click.secho("当前实验没有管道运行记录", fg="yellow")
        return

    active_versions = [v for v in reversed(versions) if v.get("status") == "active"]
    superseded = [v for v in reversed(versions) if v.get("status") == "superseded"]

    click.secho(f"当前实验: {experiment_manager.current_experiment}", fg="cyan")

    if list_only:
        if active_versions:
            click.echo("当前版本:")
            for v in active_versions:
                click.echo(f"  > #{v['id']} [{v['timestamp'][:19]}] {v.get('config_file','')}")
        if superseded:
            click.echo(f"可回退的历史版本 ({len(superseded)}):")
            for v in superseded[-5:]:
                click.echo(f"    #{v['id']} [{v['timestamp'][:19]}] {v.get('config_file','')}")
        return

    if not config_file:
        all_active = pvm.list_active_versions()
        if len(all_active) > 1:
            click.secho("当前实验有多个 active pipeline 版本，请用 --config-file 指定要回退的 pipeline:", fg="yellow")
            for v in all_active:
                click.echo(f"  #{v['id']} {v.get('config_file', '')}")
            return
        config_file = all_active[0].get("config_file", "") if all_active else ""

    current = pvm.get_current(config_file or "")
    if not current:
        click.secho("当前没有激活的版本", fg="yellow")
        return

    # 执行回退
    for _ in range(min(steps, len(superseded))):
        prev = pvm.undo_last(config_file or "")
        if not prev:
            click.secho("没有更多可回退的版本", fg="yellow")
            break

    # 更新 exports + .version stamp (undo_last 内部已调用 _stamp_version,
    # 这里再补一次以确保 exports/ 文件也更新)
    new_current = pvm.get_current(config_file or "")
    if new_current and new_current.get("export_id"):
        export_id = new_current["export_id"]
        export_name = new_current.get("export_name", "")
        with get_session() as session:
            entry = session.query(DataEntry).get(export_id)
            if entry and Path(entry.path).exists() and export_name:
                import shutil
                export_path = experiment_manager.base_path / "exports" / export_name
                shutil.copyfile(entry.path, str(export_path))
                pvm._stamp_version(config_file or "")
                click.secho(f"已回退到版本 #{new_current['id']}, 导出文件已更新", fg="green")
                click.echo(f"  {export_path}")
                return
    click.secho(f"已回退到版本 #{new_current['id']}" if new_current else "回退完成", fg="green")


def _hard_undo(steps, list_only, yes):
    """旧版破坏性撤销: 删除 DB 记录和磁盘文件"""
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

    storage = FileStorage()
    with get_session() as session:
        for batch in log.pop(steps):
            ids = batch["entry_ids"]
            entries = session.query(DataEntry).filter(DataEntry.id.in_(ids)).all()

            for entry in entries:
                fp = Path(entry.path)
                if fp.exists():
                    fp.unlink()
                storage.remove_export_meta(entry.id)
                session.query(FileMTimeCache).filter(
                    FileMTimeCache.data_entry_id == entry.id
                ).delete()
                session.query(StepCache).filter(
                    StepCache.output_id == entry.id
                ).delete()
                session.query(DataRelationship).filter(
                    or_(DataRelationship.parent_id == entry.id,
                        DataRelationship.child_id == entry.id)
                ).delete()
                session.delete(entry)

        session.commit()

    click.secho(f"已撤回 {len(batches)} 步，删除 {sum(len(b['entry_ids']) for b in batches)} 条记录", fg="green")
