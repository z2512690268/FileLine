"""撤销日志：通过 SQLite DB 记录每批操作, 支持撤回"""
import json
import logging
from datetime import datetime
from .base import get_session

logger = logging.getLogger(__name__)


class UndoLog:
    def __init__(self):
        pass

    def record(self, entry_ids, description=""):
        """记录一批 entry_id"""
        if isinstance(entry_ids, int):
            entry_ids = [entry_ids]
        from .models import UndoRecord
        try:
            with get_session() as session:
                session.add(UndoRecord(
                    timestamp=datetime.now(),
                    entry_ids=json.dumps(entry_ids),
                    description=description,
                ))
                session.commit()
        except Exception:
            logger.exception("UndoLog.record failed")

    def peek(self, n=1):
        """查看最近 n 批，不删除"""
        from .models import UndoRecord
        try:
            with get_session() as session:
                rows = session.query(UndoRecord).order_by(
                    UndoRecord.id.desc()
                ).limit(n).all()
            result = []
            for r in reversed(rows):
                result.append({
                    "timestamp": r.timestamp.isoformat() if r.timestamp else "",
                    "entry_ids": json.loads(r.entry_ids) if r.entry_ids else [],
                    "description": r.description or "",
                })
            return result
        except Exception:
            return []

    def pop(self, n=1):
        """弹出最近 n 批并持久化"""
        from .models import UndoRecord
        removed = []
        try:
            with get_session() as session:
                rows = session.query(UndoRecord).order_by(
                    UndoRecord.id.desc()
                ).limit(n).all()
                for r in rows:
                    removed.append({
                        "timestamp": r.timestamp.isoformat() if r.timestamp else "",
                        "entry_ids": json.loads(r.entry_ids) if r.entry_ids else [],
                        "description": r.description or "",
                    })
                    session.delete(r)
                session.commit()
        except Exception:
            logger.exception("UndoLog.pop failed")
        return list(reversed(removed))

    def clear(self):
        from .models import UndoRecord
        try:
            with get_session() as session:
                session.query(UndoRecord).delete()
                session.commit()
        except Exception:
            logger.exception("UndoLog.clear failed")

    @property
    def count(self):
        from .models import UndoRecord
        try:
            with get_session() as session:
                return session.query(UndoRecord).count()
        except Exception:
            return 0
