import re
from datetime import datetime
from pathlib import Path

def get_step_timestamps(file_meta) -> tuple:
    '''
    Extracts step timestamps from a log file based on the provided file metadata.
    
    @param file_meta: dict, contains metadata about the file including 'file_name' and 'step_num'
    @return: tuple of timestamps (step_1_timestamp, step_num_timestamp)
    '''
    try:
        log_file_name = file_meta['file_name'] + '.log'
        absolute_path = Path(file_meta['absolute_path'])
        # print(f"Reading log file: {log_file_name}") #debug
        log_file_path = absolute_path.with_suffix('')
        # print(f"Log file path: {log_file_path}")
        with open(log_file_path, 'r') as l:
            # 遍历日志文件，查找 Step 记录对应的时间戳
            for line in l:
                line = line.strip()
                # print(line)
                if not line:
                    continue
                # 匹配Step记录
                pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*Step (\d+)'
                step_match = re.search(pattern, line)
                if step_match:
                    timestamp = datetime.strptime(step_match.group(1), '%Y-%m-%d %H:%M:%S.%f')
                    step = int(step_match.group(2))
                    if step == 1:
                        step_1_timestamp = timestamp
                    elif step == int(file_meta['step_num']):
                        step_num_timestamp = timestamp
            print(f"Step 0 Timestamp: {step_1_timestamp}, Step {file_meta['step_num']} Timestamp: {step_num_timestamp}")
            return step_1_timestamp, step_num_timestamp
    except Exception as e:
        print(f"Error reading log file {log_file_name}, error: {e}")
        return None, None