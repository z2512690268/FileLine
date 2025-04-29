# core/storage.py
import shutil
from pathlib import Path
import uuid
from .base import experiment_manager

class FileStorage:
    @property
    def base_path(self) -> Path:
        """动态获取当前实验的存储目录"""
        return Path(experiment_manager.base_path)

    def __init__(self):
        self._setup_directories()
    
    def _setup_directories(self):
        self.base_path.mkdir(exist_ok=True)
        (self.base_path/"raw").mkdir(exist_ok=True)
        (self.base_path/"processed").mkdir(exist_ok=True)
        (self.base_path/"plots").mkdir(exist_ok=True)
    
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
    
    def create_plot_file(self, ext=".pdf"):
        """创建图表文件"""
        unique_id = uuid.uuid4().hex
        target_path = self.base_path/"plots"/f"{unique_id}{ext}"
        return target_path