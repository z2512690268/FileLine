# 修改 core/experiment.py
import json
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

class ExperimentManager:
    _CONFIG_DIR = Path.cwd() / ".expr_config"
    _CURRENT_FILE = _CONFIG_DIR / "current_experiment"

    @property
    def base_path(self) -> Path:
        """获取实验根目录"""
        return Path.cwd() / "experiments" / self.current_experiment

    def __init__(self):
        self._CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        self.current_experiment: Optional[str] = self._load_current()

    def _load_current(self) -> Optional[str]:
        """从文件加载当前实验"""
        if self._CURRENT_FILE.exists():
            return self._CURRENT_FILE.read_text().strip()
        return None

    def set_current(self, name: str, persist: bool = True):
        """设置当前实验（可选持久化）"""
        if name not in self.get_experiments():
            raise ValueError(f"实验 {name} 不存在")
        
        self.current_experiment = name
        if persist:
            self._CURRENT_FILE.write_text(name)

    def get_experiments(self) -> Dict:
        """获取所有实验配置"""
        config_file = self._CONFIG_DIR / "experiments.json"
        if not config_file.exists():
            return {}
        return json.loads(config_file.read_text())

    def create(self, name: str, description: str = "") -> None:
        """创建新实验（新增方法）"""
        experiments = self.get_experiments()
        
        if name in experiments:
            raise ValueError(f"实验 {name} 已存在")
            
        # 创建实验目录
        exp_dir = Path.cwd() / "experiments" / name
        exp_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化数据库路径
        db_path = exp_dir / f"{name}.db"
        
        # 存储配置
        experiments[name] = {
            "database": str(db_path.absolute()),
            "data_root": str(exp_dir/"data"),
            "description": description,
            "created_at": datetime.now().isoformat()
        }
        
        self._save_experiments(experiments)
        
        # 创建数据目录
        (exp_dir/"data/raw").mkdir(parents=True, exist_ok=True)
        (exp_dir/"data/processed").mkdir(parents=True, exist_ok=True)
        (exp_dir/"data/plots").mkdir(parents=True, exist_ok=True)

    def _save_experiments(self, data: Dict) -> None:
        """保存实验配置"""
        config_file = self._CONFIG_DIR / "experiments.json"
        config_file.write_text(json.dumps(data, indent=2))