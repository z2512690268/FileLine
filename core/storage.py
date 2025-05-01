# core/storage.py
import shutil
from pathlib import Path
import uuid
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

    def store_raw_data(self, file_path):
        """存储原始数据"""
        unique_id = uuid.uuid4().hex
        ext = Path(file_path).suffix
        target_path = self.base_path/"raw"/f"{unique_id}{ext}"
        shutil.copy(file_path, target_path)
        return target_path
    
    def create_processed_file(self, ext=".csv"):
        """创建处理后的文件"""
        unique_id = uuid.uuid4().hex
        target_path = self.base_path/"processed"/f"{unique_id}{ext}"
        return target_path
    
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