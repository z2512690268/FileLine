# commands/process_commands.py
import click
from typing import List
from core.processing import DataProcessor, ProcessorRegistry
from core.history import HistoryManager
from core.storage import FileStorage
from core.base import get_session
import processes

@click.group()
def process():
    """数据处理命令集"""
    pass

@process.command()
@click.argument("processor_name")
@click.argument("input_ids", type=str)  # 支持逗号分隔的ID列表
@click.option("--param", "-p", multiple=True, help="处理参数，格式：key=value")
def run(processor_name, input_ids, param):
    """执行数据处理操作"""
    with get_session() as session:
        # 解析输入参数
        inputs = _parse_input_ids(input_ids)
        params = _parse_cli_params(param)
        
        # 执行处理
        processor = DataProcessor(FileStorage(), session)
        entry = processor.run(
            processor_name=processor_name,
            input_ids=inputs,
            **params
        )
        
        # 记录操作历史
        HistoryManager(session).log_operation(
            entry, 
            op_type='process',
            params={
                'processor': processor_name,
                'inputs': inputs,
                'params': params
            }
        )
        session.commit()
        click.secho(f"处理成功！生成数据ID: {entry.id}", fg='green')

def _parse_input_ids(input_str: str) -> List[int]:
    """解析输入ID为列表"""
    try:
        if "," in input_str:
            return [int(i.strip()) for i in input_str.split(",")]
        return [int(input_str)]
    except ValueError:
        raise click.BadParameter("输入ID必须是整数或用逗号分隔的整数列表")

def _parse_cli_params(params: tuple) -> dict:
    """解析key=value格式参数"""
    result = {}
    for p in params:
        if "=" not in p:
            raise click.BadParameter(f"无效参数格式: {p}，应使用 key=value")
        key, value = p.split("=", 1)
        result[key.strip()] = _try_convert(value.strip())
    return result

def _try_convert(value: str):
    """尝试转换数据类型"""
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            if value.lower() in ("true", "false"):
                return value.lower() == "true"
            return value