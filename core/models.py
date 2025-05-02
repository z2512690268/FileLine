from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base

# SQLite支持任意长度字符串，因此下面的长度限制并未生效 


data_tag_association = Table(
    'data_tag', Base.metadata,
    Column('data_id', Integer, ForeignKey('data_entries.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

class DataEntry(Base):
    __tablename__ = 'data_entries'
    
    id = Column(Integer, primary_key=True)
    type = Column(String(20))  # raw/processed/plot
    path = Column(String(256))
    timestamp = Column(DateTime, default=datetime.now)
    description = Column(String(256))
    
    # 数据血缘关系
    parents = relationship("DataEntry",
                          secondary="data_relationship",
                          primaryjoin="DataRelationship.child_id == DataEntry.id",
                          secondaryjoin="DataRelationship.parent_id == DataEntry.id")
    
    tags = relationship("Tag", secondary=data_tag_association)
    operations = relationship("Operation", back_populates="data_entry")

class Tag(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)
    name = Column(String(32), unique=True)

class DataRelationship(Base):
    __tablename__ = 'data_relationship'
    parent_id = Column(Integer, ForeignKey('data_entries.id'), primary_key=True)
    child_id = Column(Integer, ForeignKey('data_entries.id'), primary_key=True)

class Operation(Base):
    __tablename__ = 'operations'
    id = Column(Integer, primary_key=True)
    op_type = Column(String(20))  # add/process/merge/plot
    timestamp = Column(DateTime, default=datetime.now)
    parameters = Column(String(512))  # 存储JSON参数
    data_entry_id = Column(Integer, ForeignKey('data_entries.id'))
    data_entry = relationship("DataEntry", back_populates="operations")

class FileMTimeCache(Base):
    """文件修改时间缓存"""
    __tablename__ = 'file_mtime_cache'
    id = Column(Integer, primary_key=True)
    file_path = Column(String(256), unique=False, index=True)
    data_entry_id = Column(Integer, ForeignKey('data_entries.id'))
    last_mtime = Column(Float)  # 存储时间戳
    created_at = Column(DateTime, default=datetime.now)

class StepCache(Base):
    """步骤缓存记录"""
    __tablename__ = 'step_cache'
    id = Column(Integer, primary_key=True)
    input_hash = Column(String(64), unique=False, index=True)
    output_id = Column(Integer, ForeignKey('data_entries.id'))
    created_at = Column(DateTime, default=datetime.now)