# coding=utf-8
import argparse

import db
import utils


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    parser.add_argument('-n', '--batch-no')
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    remote_logger.info('开始数据检查')

    if not utils.notification_scheduled(args):
        remote_logger.error('调用通知失败')
        return False

    settle_date = args.date

    session = None
    cur = None
    try:
        session = db.get_session()
        connection = session.connection()
        dbapi_conn = connection.connection
        cur = dbapi_conn.cursor()
        cur.callproc("fs.fs_check_zjhf2", [settle_date])
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.error('数据检查失败')
    else:
        utils.notification_executed(args, 0, '数据检查完成')
        remote_logger.info('数据检查完成')
    finally:
        if cur is not None:
            cur.close()
        if session is not None:
            session.close()


if __name__ == '__main__':
    # 初始化日志
    logger = utils.init_log()
    logger.info('start')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    main()
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info('end')
