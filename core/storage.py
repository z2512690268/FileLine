# core/storage.py
import logging
import shutil
from pathlib import Path
from datetime import datetime
from .base import experiment_manager, get_session

logger = logging.getLogger(__name__)


class FileStorage:
    @property
    def base_path(self) -> Path:
        """动态获取当前实验的存储目录"""
        return Path(experiment_manager.base_path)

    def __init__(self):
        self._setup_directories()
        self._meta_cache = self._load_meta()

    def _setup_directories(self):
        self.base_path.mkdir(exist_ok=True)
        (self.base_path/"raw").mkdir(exist_ok=True)
        (self.base_path/"processed").mkdir(exist_ok=True)
        (self.base_path/"exports").mkdir(exist_ok=True)

    def _load_meta(self) -> dict:
        """从 DB 加载导出元数据到内存缓存"""
        from .models import ExportMeta
        cache = {}
        try:
            with get_session() as session:
                rows = session.query(ExportMeta).all()
                for r in rows:
                    cache[r.name] = {
                        "id": r.data_entry_id,
                        "created_at": r.created_at.isoformat() if r.created_at else "",
                    }
        except Exception:
            logger.exception("FileStorage._load_meta failed")
        return cache

    def _save_meta(self, name: str, file_id: int, session=None):
        """保存单条导出元数据到 DB + 缓存"""
        from .models import ExportMeta
        self._meta_cache[name] = {
            "id": file_id,
            "created_at": datetime.now().isoformat(),
        }
        own_session = session is None
        try:
            s = session or get_session()
            try:
                existing = s.query(ExportMeta).get(name)
                if existing:
                    existing.data_entry_id = file_id
                    existing.created_at = datetime.now()
                else:
                    s.add(ExportMeta(
                        name=name,
                        data_entry_id=file_id,
                        created_at=datetime.now(),
                    ))
                if own_session:
                    s.commit()
            finally:
                if own_session:
                    s.close()
        except Exception:
            logger.exception("FileStorage._save_meta failed")

    def store_raw_data(self, file_path, session):
        """存储原始数据，先创建条目获取 ID，以 {id}{ext} 命名"""
        from .models import DataEntry
        ext = Path(file_path).suffix
        entry = DataEntry(
            type='raw',
            original_path=str(Path(file_path).absolute()),
        )
        session.add(entry)
        session.flush()
        target_path = self.base_path / "raw" / f"{entry.id}{ext}"
        shutil.copy(file_path, target_path)
        entry.path = str(target_path)
        return entry

    def create_processed_file(self, ext=".csv", session=None):
        """创建处理文件，先创建条目获取 ID，以 {id}{ext} 命名，返回 (path, entry)"""
        from .models import DataEntry
        entry = DataEntry(type='processed')
        if session:
            session.add(entry)
            session.flush()
        target_path = self.base_path / "processed" / f"{entry.id}{ext}"
        entry.path = str(target_path)
        return target_path, entry

    def create_export_file(self, name: str, file_id: int, session=None) -> Path:
        """创建导出文件并更新元信息"""
        target_path = self.base_path / "exports" / name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self._save_meta(name, file_id, session)
        return target_path

    def remove_export_meta(self, entry_id: int, session=None):
        """删除指定 entry_id 的导出元数据"""
        from .models import ExportMeta
        for name, meta in list(self._meta_cache.items()):
            if meta.get("id") == entry_id:
                del self._meta_cache[name]
                export_file = self.base_path / "exports" / name
                if export_file.exists():
                    export_file.unlink()
        own_session = session is None
        try:
            s = session or get_session()
            try:
                s.query(ExportMeta).filter(
                    ExportMeta.data_entry_id == entry_id
                ).delete()
                if own_session:
                    s.commit()
            finally:
                if own_session:
                    s.close()
        except Exception:
            logger.exception("FileStorage.remove_export_meta failed")
