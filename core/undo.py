"""撤销日志：记录每批操作的 entry_id，支持撤回"""
import json
from pathlib import Path
from datetime import datetime
from .base import experiment_manager


class UndoLog:
    def __init__(self):
        base = experiment_manager.base_path
        self._path = base / ".undo_log"
        self._entries = self._load()

    def _load(self):
        if self._path.exists():
            return json.loads(self._path.read_text(encoding="utf-8"))
        return []

    def _save(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._entries, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def record(self, entry_ids, description=""):
        """记录一批 entry_id"""
        if isinstance(entry_ids, int):
            entry_ids = [entry_ids]
        self._entries.append({
            "timestamp": datetime.now().isoformat(),
            "entry_ids": entry_ids,
            "description": description,
        })
        self._save()

    def peek(self, n=1):
        """查看最近 n 批，不删除"""
        return list(reversed(self._entries[-n:])) if self._entries else []

    def pop(self, n=1):
        """弹出最近 n 批并持久化"""
        removed = []
        for _ in range(min(n, len(self._entries))):
            removed.append(self._entries.pop())
        self._save()
        return list(reversed(removed))

    def clear(self):
        self._entries = []
        self._save()

    @property
    def count(self):
        return len(self._entries)
