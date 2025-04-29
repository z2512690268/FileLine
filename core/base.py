# 修改后的 core/base.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from .experiment import ExperimentManager

experiment_manager = ExperimentManager()
Base = declarative_base()

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
    return create_engine(f"sqlite:///{config['database']}", 
                       connect_args={"check_same_thread": False})

def get_session():
    """获取数据库会话（带自动重连）"""
    return sessionmaker(bind=get_engine())()

def init_db():
    """按需初始化数据库"""
    if experiment_manager.current_experiment:
        Base.metadata.create_all(bind=get_engine())