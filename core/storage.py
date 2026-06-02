# core/storage.py
import shutil
from pathlib import Path
import json
from datetime import datetime
from .base import experiment_manager


class FileStorage:
    @property
    def base_path(self) -> Path:
        """动态获取当前实验的存储目录"""
        return Path(experiment_manager.base_path)

    def __init__(self):
        self._setup_directories()
        self._load_exports_meta()

    def _setup_directories(self):
        self.base_path.mkdir(exist_ok=True)
        (self.base_path/"raw").mkdir(exist_ok=True)
        (self.base_path/"processed").mkdir(exist_ok=True)
        (self.base_path/"exports").mkdir(exist_ok=True)

    def _load_exports_meta(self):
        """加载元信息文件到内存"""
        meta_path = self.base_path/"exports"/"exports.meta"
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                self._meta_cache = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._meta_cache = {}

    def _save_exports_meta(self):
        """将内存中的元信息写入文件"""
        meta_path = self.base_path/"exports"/"exports.meta"
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(self._meta_cache, f, indent=2, ensure_ascii=False)

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
    
    def create_export_file(self, name: str, file_id: int) -> Path:
        """创建导出文件并更新元信息
        Args:
            name:    导出文件名（可包含子目录，如 "reports/sales.csv"）
            file_id: 必须提供的文件标识符
        Returns:
            导出文件的完整路径
        """
        # 构建目标路径并确保目录存在
        target_path = self.base_path/"exports"/name
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # 更新内存中的元信息
        self._meta_cache[name] = {
            "id": file_id,
            "created_at": datetime.now().isoformat()
        }

        # 写入磁盘
        self._save_exports_meta()
        return target_path