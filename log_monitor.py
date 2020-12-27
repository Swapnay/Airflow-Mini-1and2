import os
basepath = '/Users/syeruvala//airflow/logs/marketvol'
files = []
from pathlib import Path


with os.scandir(basepath) as entries:
    for entry in entries:
        if entry.is_file():
            print('file')
            files.append(entry.path)
        elif entry.is_dir():
            print('dir')
            print(entry.path)
            for path in  Path(entry.path).rglob('20*'):
                for file_path in Path(path).rglob('*.log'):
                    if file_path.is_file():
                        files.append(file_path)


print('files')
for file in files:
    print(file)

error_count = 0
error_list = []
for file in files:
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if 'ERROR' in line:
                error_count += 1
                error_list.append(line)
print('Total errors:', error_count)
for item in error_list:
    print(item)