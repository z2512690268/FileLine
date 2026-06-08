"""data check: 检查 DB 与文件系统的一致性"""
import click
from pathlib import Path
from sqlalchemy import or_
from core.base import get_session
from core.models import DataEntry, FileMTimeCache, StepCache, DataRelationship
from core.storage import FileStorage


@click.command()
@click.option("--fix", is_flag=True, help="自动修复发现的问题")
def check_cmd(fix):
    """检查 DB 记录与磁盘文件的一致性"""
    storage = FileStorage()
    issues = []

    with get_session() as session:
        # ── 1. 检查所有 DataEntry 的磁盘文件 ──
        all_entries = session.query(DataEntry).all()
        missing_files = []
        for entry in all_entries:
            if not Path(entry.path).exists():
                missing_files.append(entry)
                issues.append(f"  ID {entry.id} [{entry.type}] 文件缺失: {entry.path}")

        if missing_files:
            click.secho(f"\n[DB 条目] 发现 {len(missing_files)} 条记录的文件已不存在:", fg="red")
            for e in missing_files:
                click.echo(f"  ID {e.id}: {e.path}")

            if fix:
                click.echo("--- 清理缺失文件的 DB 条目 ---")
                ids = [e.id for e in missing_files]
                session.query(FileMTimeCache).filter(
                    FileMTimeCache.data_entry_id.in_(ids)
                ).delete(synchronize_session=False)
                session.query(StepCache).filter(
                    StepCache.output_id.in_(ids)
                ).delete(synchronize_session=False)
                session.query(DataRelationship).filter(
                    or_(DataRelationship.parent_id.in_(ids),
                        DataRelationship.child_id.in_(ids))
                ).delete(synchronize_session=False)
                session.query(DataEntry).filter(
                    DataEntry.id.in_(ids)
                ).delete(synchronize_session=False)
                click.secho(f"  已删除 {len(ids)} 条悬挂 DB 记录", fg="green")
        else:
            click.secho("[DB 条目] 所有记录的文件均存在 ✓", fg="green")

        # ── 2. 扫描存储目录中的孤儿文件 ──
        known_paths = set()
        for entry in all_entries:
            p = Path(entry.path)
            known_paths.add(p.resolve())
        orphaned = []
        for subdir in ["raw", "processed", "exports"]:
            dir_path = storage.base_path / subdir
            if not dir_path.exists():
                continue
            for f in dir_path.rglob("*"):
                if f.is_file() and f.suffix != ".meta":
                    if f.resolve() not in known_paths:
                        orphaned.append(f)
        if orphaned:
            click.secho(f"\n[孤儿文件] 发现 {len(orphaned)} 个无 DB 关联的文件:", fg="yellow")
            for f in orphaned[:20]:
                click.echo(f"  {f.relative_to(storage.base_path)}")
            if len(orphaned) > 20:
                click.echo(f"  ... 及另外 {len(orphaned) - 20} 个")

            if fix:
                click.echo("--- 删除孤儿文件 ---")
                count = 0
                for f in orphaned:
                    f.unlink()
                    count += 1
                click.secho(f"  已清理 {count} 个孤儿文件", fg="green")
        else:
            click.secho("[孤儿文件] 无孤儿文件 ✓", fg="green")

        # ── 3. 检查 exports.meta 一致性 ──
        meta_orphans = []
        for name, info in list(storage._meta_cache.items()):
            meta_path = storage.base_path / "exports" / name
            if not meta_path.exists():
                meta_orphans.append(name)
                issues.append(f"  exports.meta 条目 '{name}' 指向的文件已不存在")
            else:
                db_entry = session.get(DataEntry, info["id"])
                if db_entry is None:
                    meta_orphans.append(name)
                    issues.append(f"  exports.meta 条目 '{name}' 指向的 DB ID {info['id']} 已不存在")

        if meta_orphans:
            click.secho(f"\n[exports.meta] 发现 {len(meta_orphans)} 条无效引用:", fg="red")
            for name in meta_orphans:
                click.echo(f"  {name}")

            if fix:
                from core.models import ExportMeta
                for name in meta_orphans:
                    del storage._meta_cache[name]
                    session.query(ExportMeta).filter(
                        ExportMeta.name == name
                    ).delete()
                session.commit()
                click.secho("  已清理 exports.meta", fg="green")
        else:
            click.secho("[exports.meta] 一致性检查通过 ✓", fg="green")

        # ── 4. 检查悬挂缓存 ──
        stale_step = session.query(StepCache).filter(
            ~StepCache.output_id.in_(
                session.query(DataEntry.id).scalar_subquery()
            )
        ).all()
        stale_mtime = session.query(FileMTimeCache).filter(
            ~FileMTimeCache.data_entry_id.in_(
                session.query(DataEntry.id).scalar_subquery()
            )
        ).all()
        if stale_step or stale_mtime:
            click.secho("\n[缓存表] 发现悬挂引用:", fg="yellow")
            if stale_step:
                click.echo(f"  StepCache: {len(stale_step)} 条")
            if stale_mtime:
                click.echo(f"  FileMTimeCache: {len(stale_mtime)} 条")

            if fix:
                for c in stale_step:
                    session.delete(c)
                for c in stale_mtime:
                    session.delete(c)
                session.commit()
                click.secho("  已清理悬挂缓存", fg="green")
        else:
            click.secho("[缓存表] 无悬挂引用 ✓", fg="green")

    # ── 汇总 ──
    session.commit()
    if not issues:
        click.secho("\n一致性检查完全通过 ✓", fg="green")
    else:
        if not fix:
            click.secho(f"\n发现 {len(issues)} 个问题，使用 --fix 自动修复", fg="yellow")
