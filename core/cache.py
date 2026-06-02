"""多输出缓存: 存储 hash→{group: [entry_id]} 映射"""
import json
from pathlib import Path
from .base import experiment_manager


class MultiCache:
    def __init__(self):
        base = experiment_manager.base_path
        self._path = base / ".pipeline_cache"
        self._data = self._load()

    def _load(self):
        if self._path.exists():
            return json.loads(self._path.read_text(encoding="utf-8"))
        return {}

    def _save(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def get(self, step_hash: str):
        return self._data.get(step_hash)

    def set(self, step_hash: str, data):
        self._data[step_hash] = data
        self._save()

    def clear(self):
        self._data = {}
        self._save()
