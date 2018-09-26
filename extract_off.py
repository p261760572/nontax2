# coding=utf-8
import argparse
import glob
import os
import posixpath
import re
import subprocess
from datetime import datetime

from sqlalchemy import func, tuple_

import config
import db
import utils
from models import SsExtract


def replace_path(settle_date):
    settle_yyyy = settle_date[0:4]
    settle_yy = settle_date[2:4]
    settle_mm = settle_date[4:6]
    settle_dd = settle_date[6:8]

    def replace(m):
        text = m.group()
        text = text[1:-1]
        return text.replace("YYYY", settle_yyyy).replace("YY", settle_yy).replace("MM", settle_mm).replace("DD",
                                                                                                           settle_dd)

    return replace


def translate_path(path, sys_date):
    path = re.sub("\{\w+\}", replace_path(sys_date), path or "")
    return path


def translate_local_path(path, filename, sys_date):
    path = translate_path(path, sys_date)
    filename = translate_path(filename, sys_date)
    return os.path.join(config.DATA_ROOT, path, filename).replace("/", os.sep)


def translate_remote_path(path, filename, sys_date):
    path = translate_path(path, sys_date)
    filename = translate_path(filename, sys_date)
    return posixpath.join(path, filename)


def process_extract(settle_date, file_group):
    session = None
    try:
        session = db.get_session()
        subquery = session.query(SsExtract.file_group, func.max(SsExtract.start_date)).filter_by(
            file_group=file_group).filter(
            SsExtract.start_date <= settle_date).group_by(SsExtract.file_group)
        query = session.query(SsExtract).filter(
            tuple_(SsExtract.file_group, SsExtract.start_date).in_(subquery))  # .filter(SsExtract.status == "1")
        ss_extract_list = query.all()

        for ss_extract in ss_extract_list:
            local_path = translate_local_path(ss_extract.local_path, "", settle_date)
            # 切换工作目录
            os.chdir(local_path)

            # 执行解压
            for local_filename in glob.glob(ss_extract.pattern):  # os.listdir(local_path):
                command = ss_extract.extract_command.replace("{FILE}", local_filename)

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
    parser.add_argument("host", default="http://127.0.0.1:8080")
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    parser.add_argument("file_groups")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)
    logger.info("开始解压")
    remote_logger.info("开始解压")

    if not utils.notification_scheduled(args):
        remote_logger.error("调用通知失败")
        return False

    try:
        settle_date = args.date
        for file_group in args.file_groups.split(","):
            process_extract(settle_date, file_group)
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.error("解压处理失败")
    else:
        utils.notification_executed(args, 0, "解压完成")
        remote_logger.info("解压完成")


if __name__ == "__main__":
    # 初始化日志
    logger = utils.init_log()
    logger.info("start")
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    main()
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("end")
