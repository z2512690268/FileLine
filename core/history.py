# core/history.py
from datetime import datetime
import json
from core.models import Operation

class HistoryManager:
    def __init__(self, db_session):
        self.session = db_session
    
    def log_operation(self, data_entry, op_type, params):
        """记录操作历史"""
        op = Operation(
            op_type=op_type,
            parameters=json.dumps(params),
            data_entry=data_entry
        )
        self.session.add(op)
        self.session.commit()