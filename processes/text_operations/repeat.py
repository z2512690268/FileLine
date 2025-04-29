from pathlib import Path
from core.processing import ProcessorRegistry

@ProcessorRegistry.register(name="text_repeat", input_type="single", output_ext=".log")
def repeat_text_file(input_path: Path, output_path: Path, repeat_num: int = 1):
    """文本重复处理
    :param repeat_num: 重复次数 (默认1)
    """
    with open(input_path, 'r') as f:
        content = f.read()
    
    with open(output_path, 'w') as f:
        f.write('\n'.join([content]*repeat_num))