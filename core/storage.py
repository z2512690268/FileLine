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

    def _get_global_sequence_number(self) -> int:
        """获取全局序号（跨所有目录和文件类型）"""
        counter_file = self.base_path / ".sequence_counter"
        
        try:
            if counter_file.exists():
                current = int(counter_file.read_text().strip())
                # print(f"Current sequence number: {current}")
            else:
                current = 0
            
            next_num = current + 1
            counter_file.write_text(str(next_num))
            # print(f"Next sequence number: {next_num}")
            return next_num
            
        except (ValueError, IOError) as e:
            # 如果读取失败，从现有文件中推断最大序号
            return self._recover_sequence_from_files()
    
    def _recover_sequence_from_files(self) -> int:
        """从现有文件中恢复序号计数器"""
        print("Conter file not found!")
        print("Recovering sequence number from existing files...")
        max_seq = 0
        
        # 扫描所有目录中的文件
        for check_dir in ["raw", "processed"]:
            check_path = self.base_path / check_dir
            if check_path.exists():
                for file in check_path.glob("*_*_*.*"):
                    try:
                        # 解析文件名格式: YYYYMMDD_HHMMSS_XXXXXX.ext
                        name_parts = file.stem.split("_")
                        if len(name_parts) >= 3:
                            seq_str = name_parts[2]  # 第三部分是序号
                            seq_num = int(seq_str)
                            max_seq = max(max_seq, seq_num)
                    except (ValueError, IndexError):
                        continue
        
        # 更新计数器文件
        next_seq = max_seq + 1
        counter_file = self.base_path / ".sequence_counter"
        counter_file.write_text(str(next_seq))
        return next_seq
    
    def _get_timestamp_filename(self, ext: str, directory: str) -> str:
        """生成时间戳格式的文件名
        Args:
            ext: 文件扩展名
            directory: 目标目录 (raw/processed)
        Returns:
            完整的文件名，格式: YYYYMMDD_HHMMSS_XXXXXX.ext (6位序号)
        """
        # 获取当前时间戳
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        
        # 获取全局序号
        sequence = self._get_global_sequence_number()
        
        # 生成文件名: YYYYMMDD_HHMMSS_000001.ext (6位序号)
        filename = f"{timestamp}_{sequence:06d}{ext}"
        return filename
    
    def store_raw_data(self, file_path):
        """存储原始数据"""
        ext = Path(file_path).suffix
        filename = self._get_timestamp_filename(ext, "raw")
        target_path = self.base_path/"raw"/filename
        shutil.copy(file_path, target_path)
        return target_path
    
    def create_processed_file(self, ext=".csv"):
        """创建处理后的文件"""
        filename = self._get_timestamp_filename(ext, "processed")
        target_path = self.base_path/"processed"/filename
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