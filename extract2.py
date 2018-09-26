# coding=utf-8
import argparse
import glob
import os
import subprocess
from datetime import datetime

from sqlalchemy import func, tuple_

import db
import utils
from models import SsExtract


def process_extract(settle_date):
    session = None
    try:
        now_datetime = datetime.now()
        settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

        session = db.get_session()
        subquery = session.query(SsExtract.file_group, func.max(SsExtract.start_date)).filter(
            SsExtract.start_date <= settle_date).group_by(SsExtract.file_group)
        query = session.query(SsExtract).filter(
            tuple_(SsExtract.file_group, SsExtract.start_date).in_(subquery)).filter(SsExtract.status == '1')
        rows = query.all()
        # 转dict
        rows = [row.to_dict() for row in rows]

        # 转换路径
        utils.translate(settle_date, rows)

        for row in rows:
            # process_mode = row['process_mode']
            local_path = row['local_path']
            extract_command = row['extract_command']
            pattern = row['pattern']

            # local_path = utils.translate_path(local_path, settle_date, True)

            # 切换工作目录
            os.chdir(local_path)

            # 执行解压
            for local_filename in glob.glob(pattern):  # os.listdir(local_path):
                command = extract_command.replace('{FILE}', local_filename)

                logger.info(command)
                subprocess.run(command, check=True, shell=True)
                # {EXDIR} {FILE}
                # gzip -f -d %s
                # unzip -o -d %s %s
                # tar -xzf -C %s %s
                # 7za x -aoa -o %s %s
    finally:
        if session is not None:
            session.close()


def main():
    global remote_logger
    parser = argparse.ArgumentParser()
    parser.add_argument("host", default='http://127.0.0.1:8080')
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    remote_logger.info('开始解压')

    if not utils.notification_scheduled(args):
        remote_logger.error('调用通知失败')
        return False

    try:
        settle_date = args.date
        process_extract(settle_date)
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.error('解压处理失败')
    else:
        utils.notification_executed(args, 0, '解压完成')
        remote_logger.info('解压完成')


if __name__ == '__main__':
    # 初始化日志
    logger = utils.init_log()
    logger.info('start')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    main()
    # process_extract('20170818')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info('end')
