from typing import List
from pathlib import Path
from core.processing import ProcessorRegistry, InputPath

@ProcessorRegistry.register(name="text_merge", input_type="multi", output_ext=".txt")
def merge_text_files(input_paths: List[InputPath], 
                    output_path: Path, 
                    encoding: str = "utf-8",
                    sep: str = "\n"):
    """合并文本文件
    :param encoding: 文件编码 (默认utf-8)
    :param sep: 文件分隔符 (默认换行符)
    """
    with open(output_path, 'w', encoding=encoding) as out_f:
        for i, path_dict in enumerate(input_paths):
            with open(path_dict.path, 'r', encoding=encoding) as in_f:
                if i > 0:
                    out_f.write(sep)
                out_f.write(in_f.read())