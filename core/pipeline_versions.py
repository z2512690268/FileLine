"""管道运行版本管理: 通过 SQLite DB 记录每次 pipeline run"""
import json
import hashlib
from datetime import datetime
from typing import Optional, List, Iterable
import uuid
from pathlib import Path
from .base import get_session, experiment_manager

DEFAULT_CACHE_SCOPE = "legacy"


class PipelineVersionManager:
    """管理实验的管道运行版本历史 (数据存储在 SQLite DB 中)"""

    def record_run(self, entry_ids: List[int], config_file: str = "",
                   export_id: Optional[int] = None,
                   export_name: str = "",
                   cache_scope: Optional[str] = None,
                   config_snapshot: Optional[str] = None,
                   processor_snapshot: Optional[str] = None,
                   global_set: Optional[str] = None,
                   global_values_snapshot: Optional[str] = None) -> int:
        """记录一次管道运行, 返回版本 ID"""
        from .models import PipelineVersion, DataEntry
        with get_session() as session:
            cache_scope = cache_scope or self._current_scope_from_session(session, config_file)
            result_hash = self._calculate_result_hash(
                session,
                entry_ids=[export_id] if export_id else entry_ids,
                config_snapshot=config_snapshot,
                processor_snapshot=processor_snapshot,
            )
            matching_versions = [
                row for row in session.query(PipelineVersion).all()
                if self.config_matches(row.config_file, config_file)
            ]
            for row in matching_versions:
                if (
                    (row.result_hash or "") == result_hash
                    and (row.export_name or "") == (export_name or "")
                ):
                    for other in matching_versions:
                        if other.id != row.id and (other.status or "") == "active":
                            other.status = "superseded"
                    row.status = "active"
                    row.global_set = global_set
                    row.global_values_snapshot = global_values_snapshot
                    session.commit()
                    return row.id
            active = [row for row in matching_versions if (row.status or "") == "active"]
            for v in active:
                v.status = "superseded"

            version = PipelineVersion(
                timestamp=datetime.now(),
                config_file=config_file,
                entry_ids=json.dumps(entry_ids),
                export_id=export_id,
                export_name=export_name,
                cache_scope=cache_scope,
                config_snapshot=config_snapshot,
                processor_snapshot=processor_snapshot,
                global_set=global_set,
                global_values_snapshot=global_values_snapshot,
                result_hash=result_hash,
                status="active",
            )
            session.add(version)
            session.flush()
            vid = version.id
            session.commit()
            return vid

    def list_versions(self, config_file: str = "") -> List[dict]:
        """返回所有版本, 最新的在前"""
        from .models import PipelineVersion
        with get_session() as session:
            rows = session.query(PipelineVersion).order_by(
                PipelineVersion.id.desc()
            ).all()
            if config_file:
                rows = [row for row in rows if self.config_matches(row.config_file, config_file)]
            return [self._row_to_dict(r) for r in rows]

    def list_active_versions(self) -> List[dict]:
        return [v for v in self.list_versions() if v.get("status") == "active"]

    def get_version(self, version_id: int) -> Optional[dict]:
        from .models import PipelineVersion
        with get_session() as session:
            r = session.query(PipelineVersion).get(version_id)
            return self._row_to_dict(r) if r else None

    def get_current(self, config_file: str = "") -> Optional[dict]:
        from .models import PipelineVersion
        with get_session() as session:
            rows = session.query(PipelineVersion).filter(
                PipelineVersion.status == "active"
            ).all()
            if config_file:
                r = next((row for row in rows if self.config_matches(row.config_file, config_file)), None)
            else:
                r = rows[0] if rows else None
            return self._row_to_dict(r) if r else None

    def get_current_cache_scope(self, config_file: str = "") -> str:
        current = self.get_current(config_file)
        if current and current.get("cache_scope"):
            return current["cache_scope"]
        return DEFAULT_CACHE_SCOPE

    def create_fresh_scope(self) -> str:
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"scope_{stamp}_{uuid.uuid4().hex[:8]}"

    def set_current(self, version_id: int) -> Optional[dict]:
        from .models import PipelineVersion
        with get_session() as session:
            target = session.query(PipelineVersion).get(version_id)
            if not target:
                session.commit()
                return None
            # 清掉所有 active
            active = [
                row for row in session.query(PipelineVersion).filter(PipelineVersion.status == "active").all()
                if self.config_matches(row.config_file, target.config_file)
            ]
            for v in active:
                v.status = "superseded"
            # 设目标为 active
            target.status = "active"
            session.commit()
            return self._row_to_dict(target) if target else None

    def undo_last(self, config_file: str = "") -> Optional[dict]:
        """回退到上一版本"""
        versions = self.list_versions(config_file)
        for i, v in enumerate(versions):
            if v.get("status") == "active":
                if i + 1 < len(versions):
                    result = self.set_current(versions[i + 1]["id"])
                    self._stamp_version(result.get("config_file", "") if result else config_file)
                    return result
                return None
        return None

    def delete_version(self, version_id: int) -> dict:
        """删除一个非 active 版本, 并清理该版本独占的 processed 文件。"""
        from .models import (
            DataEntry,
            DataRelationship,
            ExportMeta,
            FileMTimeCache,
            PipelineVersion,
            StepCache,
        )

        with get_session() as session:
            target = session.query(PipelineVersion).get(version_id)
            if not target:
                return {"deleted": False, "reason": "not_found", "versionId": version_id}
            if (target.status or "") == "active":
                return {"deleted": False, "reason": "active_version", "versionId": version_id}

            target_entry_ids = self._version_entry_ids(target)
            remaining_versions = session.query(PipelineVersion).filter(PipelineVersion.id != version_id).all()
            protected_ids: set[int] = set()
            for row in remaining_versions:
                protected_ids.update(self._version_entry_ids(row))

            candidate_ids = set(target_entry_ids) - protected_ids
            entries = session.query(DataEntry).filter(DataEntry.id.in_(candidate_ids)).all() if candidate_ids else []
            deletable_ids = {entry.id for entry in entries if entry.type == "processed"}
            if deletable_ids:
                external_children = session.query(DataRelationship).filter(
                    DataRelationship.parent_id.in_(deletable_ids),
                    ~DataRelationship.child_id.in_(deletable_ids),
                ).all()
                for relation in external_children:
                    deletable_ids.discard(relation.parent_id)

            base_path = experiment_manager.base_path.resolve()
            freed_bytes = 0
            deleted_files: list[str] = []
            deleted_entries = 0

            if deletable_ids:
                for meta in session.query(ExportMeta).filter(ExportMeta.data_entry_id.in_(deletable_ids)).all():
                    export_path = base_path / "exports" / meta.name
                    if self._safe_unlink(export_path, base_path):
                        freed_bytes += self._last_unlinked_size
                        deleted_files.append(str(export_path))
                    session.delete(meta)

                for entry in session.query(DataEntry).filter(DataEntry.id.in_(deletable_ids)).all():
                    path = Path(entry.path)
                    if self._safe_unlink(path, base_path):
                        freed_bytes += self._last_unlinked_size
                        deleted_files.append(str(path))
                    session.query(FileMTimeCache).filter(FileMTimeCache.data_entry_id == entry.id).delete()
                    session.query(StepCache).filter(StepCache.output_id == entry.id).delete()
                    session.query(DataRelationship).filter(
                        (DataRelationship.parent_id == entry.id) | (DataRelationship.child_id == entry.id)
                    ).delete(synchronize_session=False)
                    session.delete(entry)
                    deleted_entries += 1

            session.delete(target)
            session.commit()

        removed_blobs = self.prune_processor_blobs()
        freed_bytes += sum(item.get("size", 0) for item in removed_blobs)
        deleted_files.extend(item.get("path", "") for item in removed_blobs)
        return {
            "deleted": True,
            "versionId": version_id,
            "deletedEntries": deleted_entries,
            "deletedFiles": deleted_files,
            "freedBytes": freed_bytes,
            "keptEntries": len(candidate_ids) - deleted_entries,
        }

    def storage_report(self, config_file: str = "") -> dict:
        """估算当前实验的存储占用和按版本可清理空间。"""
        from .models import DataEntry, PipelineVersion

        base_path = experiment_manager.base_path.resolve()
        with get_session() as session:
            versions = session.query(PipelineVersion).order_by(PipelineVersion.id.desc()).all()
            if config_file:
                versions = [row for row in versions if self.config_matches(row.config_file, config_file)]
            all_versions = session.query(PipelineVersion).all()
            all_entries = session.query(DataEntry).all()
            entry_sizes = {entry.id: self._file_size(entry.path) for entry in all_entries}
            entry_types = {entry.id: entry.type for entry in all_entries}

            version_items = []
            for row in versions:
                own_ids = self._version_entry_ids(row)
                other_ids: set[int] = set()
                for other in all_versions:
                    if other.id != row.id:
                        other_ids.update(self._version_entry_ids(other))
                exclusive_processed = [
                    entry_id for entry_id in own_ids
                    if entry_id not in other_ids and entry_types.get(entry_id) == "processed"
                ]
                version_items.append({
                    "id": row.id,
                    "status": row.status or "",
                    "configFile": row.config_file or "",
                    "exportName": row.export_name or "",
                    "timestamp": row.timestamp.isoformat() if row.timestamp else "",
                    "entryCount": len(own_ids),
                    "exclusiveProcessedEntries": len(exclusive_processed),
                    "reclaimableBytes": 0 if (row.status or "") == "active" else sum(entry_sizes.get(i, 0) for i in exclusive_processed),
                    "deletable": (row.status or "") != "active",
                })

        by_dir = self._directory_sizes(base_path)
        db_size = self._database_size()
        total_bytes = sum(by_dir.values()) + db_size
        pipeline_reclaimable = sum(item["reclaimableBytes"] for item in version_items)
        active_versions = sum(1 for item in version_items if item["status"] == "active")
        return {
            "basePath": str(base_path),
            "totalBytes": total_bytes,
            "databaseBytes": db_size,
            "byCategory": by_dir,
            "pipeline": {
                "configFile": config_file or "",
                "versionCount": len(version_items),
                "activeVersionCount": active_versions,
                "reclaimableBytes": pipeline_reclaimable,
            },
            "versions": version_items,
        }

    def _stamp_version(self, config_file: str = ""):
        """在 exports 目录写 .version 便利文件 (供人类查看)"""
        v = self.get_current(config_file)
        if not v:
            return
        stamp = experiment_manager.base_path / "exports" / ".version"
        stamp.parent.mkdir(parents=True, exist_ok=True)
        ts = v["timestamp"][:19]
        vid = v["id"]
        cfg = v.get("config_file", "")
        stamp.write_text(
            f"#{vid}  {ts}  {cfg}\n"
            f"export: {v.get('export_name', '?')}  (entry ID {v.get('export_id', '?')})\n",
            encoding="utf-8"
        )

    @staticmethod
    def _row_to_dict(row) -> dict:
        if row is None:
            return None
        entry_ids = row.entry_ids
        if isinstance(entry_ids, str):
            try:
                entry_ids = json.loads(entry_ids)
            except (json.JSONDecodeError, TypeError):
                entry_ids = []
        return {
            "id": row.id,
            "timestamp": row.timestamp.isoformat() if hasattr(row.timestamp, 'isoformat') else str(row.timestamp),
            "config_file": row.config_file or "",
            "entry_ids": entry_ids,
            "export_id": row.export_id,
            "export_name": row.export_name or "",
            "cache_scope": row.cache_scope or DEFAULT_CACHE_SCOPE,
            "config_snapshot": row.config_snapshot or "",
            "processor_snapshot": getattr(row, "processor_snapshot", None) or "",
            "global_set": getattr(row, "global_set", None) or "",
            "global_values_snapshot": getattr(row, "global_values_snapshot", None) or "",
            "result_hash": row.result_hash or "",
            "status": row.status or "",
        }

    @staticmethod
    def _version_entry_ids(row) -> set[int]:
        ids: set[int] = set()
        raw_ids = row.entry_ids or "[]"
        try:
            parsed = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        except (json.JSONDecodeError, TypeError):
            parsed = []
        for item in parsed or []:
            try:
                ids.add(int(item))
            except (TypeError, ValueError):
                continue
        if getattr(row, "export_id", None):
            ids.add(int(row.export_id))
        return ids

    @staticmethod
    def _calculate_result_hash(session, entry_ids: List[int],
                               config_snapshot: Optional[str] = None,
                               processor_snapshot: Optional[str] = None) -> str:
        from .models import DataEntry
        digest = hashlib.sha256()
        if config_snapshot:
            digest.update(b"config:")
            digest.update(config_snapshot.encode("utf-8"))
        if processor_snapshot:
            digest.update(b"processor:")
            digest.update(processor_snapshot.encode("utf-8"))
        for entry_id in entry_ids or []:
            entry = session.query(DataEntry).get(entry_id)
            if not entry:
                digest.update(str(entry_id).encode("utf-8"))
                digest.update(b":missing")
                continue
            path = Path(entry.path)
            digest.update(str(path.suffix).encode("utf-8"))
            if path.exists():
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
            else:
                digest.update(str(entry_id).encode("utf-8"))
                digest.update(b":missing-file")
        return digest.hexdigest()

    @staticmethod
    def build_processor_snapshot(processor_names: Iterable[str]) -> str:
        """保存当前运行会用到的 processor 源码, 让历史版本可以再次执行。"""
        from .processing import ProcessorRegistry

        files: dict[str, dict] = {}
        blob_dir = PipelineVersionManager._processor_blob_dir()
        for processor_name in processor_names:
            try:
                proc_info = ProcessorRegistry.get_processor(processor_name)
            except Exception:
                continue
            source_file = Path(proc_info.get("source_file", ""))
            if not source_file.exists() or source_file.suffix != ".py":
                continue
            for py_file in sorted(source_file.parent.glob("*.py")):
                if py_file.name == "__init__.py" or py_file.stem.startswith("_"):
                    continue
                try:
                    content = py_file.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    content = py_file.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                restore_name = py_file.name
                existing = files.get(restore_name)
                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                if existing and existing.get("hash") != content_hash:
                    stem = py_file.stem
                    restore_name = f"{stem}_{content_hash[:8]}{py_file.suffix}"
                blob_name = f"{content_hash}{py_file.suffix}"
                if blob_dir:
                    blob_dir.mkdir(parents=True, exist_ok=True)
                    blob_path = blob_dir / blob_name
                    if not blob_path.exists():
                        blob_path.write_text(content, encoding="utf-8")
                files[restore_name] = {
                    "processor": processor_name,
                    "filename": restore_name,
                    "original_path": str(py_file.resolve()),
                    "hash": content_hash,
                    "blob": f"processor_snapshots/{blob_name}",
                }
        if not files:
            return ""
        payload = {
            "format": 1,
            "files": [files[name] for name in sorted(files)],
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def restore_processor_snapshot(processor_snapshot: str) -> List[Path]:
        """把版本里的 processor 快照恢复到当前实验 processors 目录。"""
        if not processor_snapshot:
            return []
        try:
            payload = json.loads(processor_snapshot)
        except (json.JSONDecodeError, TypeError):
            return []
        files = payload.get("files", []) if isinstance(payload, dict) else []
        if not files:
            return []

        target_dir = experiment_manager.base_path / "processors"
        target_dir.mkdir(parents=True, exist_ok=True)
        restored: List[Path] = []
        for item in files:
            filename = Path(str(item.get("filename", ""))).name
            if not filename or not filename.endswith(".py"):
                continue
            content = item.get("content")
            if content is None:
                blob = str(item.get("blob", ""))
                blob_path = experiment_manager.base_path / blob
                try:
                    content = blob_path.read_text(encoding="utf-8")
                except OSError:
                    continue
            target = target_dir / filename
            target.write_text(content, encoding="utf-8")
            restored.append(target)
        if restored:
            from .processing import load_processors_from_dir
            load_processors_from_dir(target_dir)
        return restored

    @staticmethod
    def _processor_blob_dir() -> Optional[Path]:
        try:
            return experiment_manager.base_path / "processor_snapshots"
        except Exception:
            return None

    def prune_processor_blobs(self) -> list[dict]:
        blob_dir = self._processor_blob_dir()
        if not blob_dir or not blob_dir.is_dir():
            return []
        referenced: set[str] = set()
        from .models import PipelineVersion
        with get_session() as session:
            for row in session.query(PipelineVersion).all():
                snapshot = getattr(row, "processor_snapshot", "") or ""
                try:
                    payload = json.loads(snapshot)
                except (json.JSONDecodeError, TypeError):
                    continue
                for item in payload.get("files", []) if isinstance(payload, dict) else []:
                    blob = str(item.get("blob", ""))
                    if blob:
                        referenced.add(Path(blob).name)
        removed = []
        base_path = experiment_manager.base_path.resolve()
        for path in blob_dir.glob("*"):
            if not path.is_file() or path.name in referenced:
                continue
            size = path.stat().st_size
            if self._safe_unlink(path, base_path):
                removed.append({"path": str(path), "size": size})
        return removed

    _last_unlinked_size = 0

    @classmethod
    def _safe_unlink(cls, path: Path, base_path: Path) -> bool:
        cls._last_unlinked_size = 0
        try:
            resolved = path.resolve()
        except OSError:
            return False
        if base_path != resolved and base_path not in resolved.parents:
            return False
        if not resolved.is_file():
            return False
        cls._last_unlinked_size = resolved.stat().st_size
        resolved.unlink()
        return True

    @staticmethod
    def _file_size(path: str) -> int:
        try:
            p = Path(path)
            return p.stat().st_size if p.is_file() else 0
        except OSError:
            return 0

    @staticmethod
    def _database_size() -> int:
        try:
            config = experiment_manager.get_experiments().get(experiment_manager.current_experiment, {})
            db_path = Path(config.get("database", ""))
            if not db_path.is_absolute():
                db_path = experiment_manager.project_root / db_path
            return db_path.stat().st_size if db_path.is_file() else 0
        except OSError:
            return 0

    @staticmethod
    def _directory_sizes(base_path: Path) -> dict[str, int]:
        categories = {
            "raw": 0,
            "processed": 0,
            "exports": 0,
            "processors": 0,
            "processorSnapshots": 0,
            "pipelines": 0,
            "other": 0,
        }
        mapping = {
            "raw": "raw",
            "processed": "processed",
            "exports": "exports",
            "processors": "processors",
            "processor_snapshots": "processorSnapshots",
            "pipelines": "pipelines",
        }
        if not base_path.is_dir():
            return categories
        for path in base_path.rglob("*"):
            if not path.is_file():
                continue
            try:
                rel = path.relative_to(base_path)
                top = rel.parts[0] if rel.parts else ""
                categories[mapping.get(top, "other")] += path.stat().st_size
            except OSError:
                continue
        return categories

    @staticmethod
    def _current_scope_from_session(session, config_file: str = "") -> str:
        from .models import PipelineVersion
        rows = session.query(PipelineVersion).filter(
            PipelineVersion.status == "active"
        ).all()
        if config_file:
            row = next((item for item in rows if PipelineVersionManager.config_matches(item.config_file, config_file)), None)
        else:
            row = rows[0] if rows else None
        if row and getattr(row, "cache_scope", None):
            return row.cache_scope
        return DEFAULT_CACHE_SCOPE

    @staticmethod
    def normalize_config_file(config_file: str) -> str:
        return str(config_file or "").replace("\\", "/").strip()

    @staticmethod
    def config_matches(stored_config: str, pipeline_path: str) -> bool:
        stored = PipelineVersionManager.normalize_config_file(stored_config)
        target = PipelineVersionManager.normalize_config_file(pipeline_path)
        if not target:
            return True
        if not stored:
            return False
        if stored == target:
            return True
        target_name = Path(target).name
        stored_name = Path(stored).name
        if stored_name == target_name:
            return True
        target_stem = Path(target_name).stem
        return bool(target_stem and stored_name.startswith(f"{target_stem}.fresh."))
