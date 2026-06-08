"""data delete: remove data entries and clean related files/caches."""
import json
from pathlib import Path

import click
from sqlalchemy import or_

from core.base import get_session
from core.models import (
    DataEntry,
    DataRelationship,
    ExportMeta,
    FileMTimeCache,
    PipelineVersion,
    StepCache,
)
from core.storage import FileStorage


REFERENCE_CONFIRM_PHRASE = "DELETE REFERENCED DATA"


def _version_entry_ids(row: PipelineVersion) -> set[int]:
    try:
        return {int(item) for item in json.loads(row.entry_ids or "[]")}
    except Exception:
        return set()


def _referenced_delete_warnings(session, ids: set[int]) -> list[str]:
    warnings: list[str] = []

    export_rows = session.query(ExportMeta).filter(ExportMeta.data_entry_id.in_(ids)).all()
    for row in export_rows:
        warnings.append(f"export '{row.name}' points to data entry {row.data_entry_id}")

    for row in session.query(PipelineVersion).all():
        referenced = set()
        if row.export_id in ids:
            referenced.add(int(row.export_id))
        referenced.update(_version_entry_ids(row) & ids)
        if referenced:
            label = row.config_file or "unknown pipeline"
            warnings.append(
                f"pipeline version #{row.id} ({row.status or 'unknown'}, {label}) references IDs "
                f"{', '.join(str(item) for item in sorted(referenced))}"
            )

    child_rows = session.query(DataRelationship).filter(
        DataRelationship.parent_id.in_(ids),
        ~DataRelationship.child_id.in_(ids),
    ).all()
    for row in child_rows:
        warnings.append(f"data entry {row.parent_id} is a parent of data entry {row.child_id}")

    return warnings


def _confirm_referenced_delete(warnings: list[str], force_referenced: bool, confirm_referenced: str) -> None:
    if not warnings:
        return

    click.secho("Refusing unsafe delete: selected entries are still referenced.", fg="red")
    for warning in warnings:
        click.echo(f"  - {warning}")

    if not force_referenced:
        click.echo(
            "\nUse --force-referenced only when you understand that CLI/frontend lineage, "
            "versions, or exports may lose these entries."
        )
        raise click.Abort()

    if confirm_referenced != REFERENCE_CONFIRM_PHRASE:
        typed = click.prompt(
            f'Type "{REFERENCE_CONFIRM_PHRASE}" to delete referenced data',
            default="",
            show_default=False,
        )
        if typed != REFERENCE_CONFIRM_PHRASE:
            raise click.Abort()


@click.command()
@click.argument("ids", type=int, nargs=-1, required=True)
@click.option("--yes", "-y", is_flag=True, help="Skip the normal delete confirmation.")
@click.option(
    "--force-referenced",
    is_flag=True,
    help="Allow deleting entries that are referenced by versions, exports, or downstream data.",
)
@click.option(
    "--confirm-referenced",
    default="",
    help=f'Exact phrase required with --force-referenced: "{REFERENCE_CONFIRM_PHRASE}".',
)
def delete_cmd(ids, yes, force_referenced, confirm_referenced):
    """Delete data entries, files, export metadata, caches, and lineage rows."""
    storage = FileStorage()
    requested_ids = set(ids)
    with get_session() as session:
        entries = session.query(DataEntry).filter(DataEntry.id.in_(requested_ids)).all()
        if not entries:
            click.secho("No matching data entries found.", fg="yellow")
            return

        found_ids = {entry.id for entry in entries}
        missing_ids = requested_ids - found_ids
        if missing_ids:
            click.secho(f"Missing IDs: {', '.join(str(item) for item in sorted(missing_ids))}", fg="yellow")

        warnings = _referenced_delete_warnings(session, found_ids)
        _confirm_referenced_delete(warnings, force_referenced, confirm_referenced)

        if not yes:
            click.echo("The following entries will be deleted:")
            for entry in entries:
                click.echo(f"  ID {entry.id}: {entry.path} ({entry.type})")
            click.confirm("Confirm delete?", abort=True)

        for entry in entries:
            file_path = Path(entry.path)
            if file_path.exists():
                file_path.unlink()
                click.echo(f"  removed file: {file_path}")

            storage.remove_export_meta(entry.id)

            session.query(FileMTimeCache).filter(
                FileMTimeCache.data_entry_id == entry.id
            ).delete()
            session.query(StepCache).filter(
                StepCache.output_id == entry.id
            ).delete()

            session.query(DataRelationship).filter(
                or_(
                    DataRelationship.parent_id == entry.id,
                    DataRelationship.child_id == entry.id,
                )
            ).delete()

            session.delete(entry)
            click.secho(f"  deleted entry ID {entry.id}", fg="green")

        session.commit()
        from core.base import experiment_manager

        click.secho(
            f"[{experiment_manager.current_experiment}] deleted {len(entries)} data entr"
            f"{'y' if len(entries) == 1 else 'ies'}",
            fg="green",
        )
