# coding=utf-8
import argparse
import logging
import os
import subprocess
from datetime import datetime

import db
import utils
from models import SsFile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host", default='http://127.0.0.1:8080')
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    args = parser.parse_args()

    if not utils.notification_scheduled(args):
        return False

    session = None
    try:
        settle_date = args.date
        now_datetime = datetime.now()
        settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

        session = db.get_session()
        query = session.query(SsFile).filter(SsFile.status == '1')
        rows = query.all()
        # 转dict
        rows = [row.to_dict() for row in rows]

        # 转换路径
        utils.translate(settle_date, rows)

        for row in rows:
            process_mode = row['process_mode']
            local_path = row['local_path']
            extract_command = row['extract_command']
            if extract_command:
                if process_mode == 'all':
                    command = extract_command.replace('{EXDIR}', local_path)

                    logging.info(command)
                    subprocess.run(command, check=True, shell=True)
                else:
                    for file in row['file_list']:
                        local_filename = file['local_filename']
                        required = file['required']

                        # 放弃非必需文件
                        if required != "1" and os.path.isfile(local_filename) == False:
                            logging.warning("文件不存在" + local_filename)
                            continue

                        command = extract_command.replace('{EXDIR}', os.path.dirname(local_filename))
                        command = command.replace('{FILE}', local_filename)

                        logging.info(command)
                        subprocess.run(command, check=True, shell=True)
                        # {EXDIR} {FILE}
                        # gzip -f -d %s
                        # unzip -o -d %s %s
                        # tar -xzf -C %s %s
                        # 7za x -aoa -o %s %s


    except Exception as e:
        utils.notification_executed(args, -1, str(e))
        raise
    else:
        utils.notification_executed(args, 0, '下载完成')
    finally:
        if session is not None:
            session.close()


if __name__ == '__main__':
    main()
