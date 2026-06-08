# 修改后的 core/base.py
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from .experiment import ExperimentManager

logger = logging.getLogger(__name__)
experiment_manager = ExperimentManager()
Base = declarative_base()

# 缓存已初始化过的数据库路径，避免每次请求都跑 migration
_init_db_cache: set[str] = set()

def get_engine():
    """获取当前实验的数据库引擎（带校验）"""
    if experiment_manager.current_experiment is None:
        raise RuntimeError(
            "当前没有激活的实验\n"
            "请使用以下方式之一选择实验：\n"
            "1. 执行命令: experiment use <名称>\n"
            "2. 启动时添加参数: --experiment <名称>"
        )

    config = experiment_manager.get_experiments().get(experiment_manager.current_experiment)
    db_path = config['database']
    # 兼容旧版绝对路径和新的相对路径
    if not Path(db_path).is_absolute():
        db_path = str(experiment_manager.project_root / db_path)
    return create_engine(f"sqlite:///{db_path}",
                       connect_args={"check_same_thread": False, "timeout": 3})

def get_session():
    """获取数据库会话（带自动重连）"""
    return sessionmaker(bind=get_engine())()

def init_db():
    """按需初始化数据库（带迁移缓存，第二次起秒过）"""
    if experiment_manager.current_experiment:
        config = experiment_manager.get_experiments().get(experiment_manager.current_experiment)
        db_path = config['database']
        if not Path(db_path).is_absolute():
            db_path = str(experiment_manager.project_root / db_path)
        if db_path in _init_db_cache:
            return
        engine = get_engine()
        Base.metadata.create_all(bind=engine)
        # 迁移: 为旧库补齐 step_cache / pipeline_versions 所需字段
        try:
            with engine.connect() as conn:
                for sql in [
                    "ALTER TABLE step_cache ADD COLUMN group_name VARCHAR(64)",
                    "ALTER TABLE step_cache ADD COLUMN cache_scope VARCHAR(64)",
                    "ALTER TABLE pipeline_versions ADD COLUMN cache_scope VARCHAR(64)",
                    "ALTER TABLE pipeline_versions ADD COLUMN config_snapshot TEXT",
                    "ALTER TABLE pipeline_versions ADD COLUMN processor_snapshot TEXT",
                    "ALTER TABLE pipeline_versions ADD COLUMN global_set VARCHAR(128)",
                    "ALTER TABLE pipeline_versions ADD COLUMN global_values_snapshot TEXT",
                    "ALTER TABLE pipeline_versions ADD COLUMN result_hash VARCHAR(64)",
                ]:
                    try:
                        conn.execute(text(sql))
                    except Exception:
                        logger.debug("init_db migration: column may already exist for SQL: %s", sql)
                conn.execute(
                    text("UPDATE step_cache SET cache_scope = 'legacy' WHERE cache_scope IS NULL OR cache_scope = ''")
                )
                conn.execute(
                    text("UPDATE pipeline_versions SET cache_scope = 'legacy' WHERE cache_scope IS NULL OR cache_scope = ''")
                )
                conn.commit()
                _init_db_cache.add(db_path)
        except Exception:
            logger.debug("init_db migration: expected on existing databases")
            # 即使 migration 失败也缓存（列已存在不是真错误）
            _init_db_cache.add(db_path)
