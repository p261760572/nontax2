# coding=utf-8
import os
import subprocess

import sys


def main():
    local_path = sys.argv[1]
    filenames = os.listdir(local_path)
    for name in filenames:
        if name.endswith('.zip'):
            extract_command = 'unzip -o -d %s %s' % (local_path, os.path.join(local_path, name))
            subprocess.run(extract_command, check=True, shell=True)
            # gzip -f -d %s
            # unzip -o -d %s %s
            # tar -xzf -C %s %s
            # 7za x -aoa -o %s %s


if __name__ == '__main__':
    main()
