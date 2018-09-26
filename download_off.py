# coding:utf-8
import argparse
import datetime
import ftplib
import os
import posixpath
import re
import socket
from time import sleep

import cx_Oracle
import paramiko
from sqlalchemy import func, tuple_

import config
import db
import utils
from models import SsDownload


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


def ftp_download(host, port, user, passwd, files, encoding="utf-8"):
    logger.info("ftp://%s:%d" % (host, port))

    ftp = ftplib.FTP()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host, port, timeout=60)  # 连接
    try:
        ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
        for remote_filename, local_filename, required in files:
            logger.info("%s" % (remote_filename))
            remote_logger.info("%s" % (remote_filename))

            local_path = posixpath.dirname(local_filename)
            if not os.path.exists(local_path):
                os.makedirs(local_path, mode=0o755)

            try:
                ftp.cwd(posixpath.dirname(remote_filename).encode(encoding).decode("latin1"))  # 选择操作目录
                file_handler = open(local_filename, "wb")  # 以写模式在本地打开文件
                ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename).encode(encoding).decode("latin1"),
                               file_handler.write)  # 接收服务器上文件并写入本地文件
                file_handler.close()
                logger.info("%s end" % (remote_filename))
            except ftplib.error_perm as e:
                logger.error(str(e), exc_info=1)
                if not required:
                    # 忽略非必需文件
                    pass
                else:
                    raise
    finally:
        try:
            ftp.quit()
        except Exception as e:
            logger.error(str(e), exc_info=1)



def sftp_download(host, port, user, passwd, files):
    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=passwd)

    try:
        sftp = paramiko.SFTPClient.from_transport(transport)
        for remote_filename, local_filename, required in files:
            logger.info(remote_filename)
            local_path = posixpath.dirname(local_filename)
            if not os.path.exists(local_path):
                os.makedirs(local_path, mode=0o755)

            try:
                logger.info("%s" % (remote_filename))
                remote_logger.info("%s" % (remote_filename))
                sftp.get(remote_filename, local_filename)
                logger.info("%s end" % (remote_filename))
            except FileNotFoundError as e:
                logger.error(str(e), exc_info=1)
                if not required:
                    # 忽略非必需文件
                    pass
                else:
                    raise Exception("文件不存在%s" % (posixpath.basename(remote_filename)))

    finally:
        try:
            transport.close()
        except:
            pass


def simple_donwload(settle_date, ss_download):
    # settle_datetime = datetime.strptime(settle_date, "%Y%m%d")
    # if not os.path.exists(local_path):
    #     os.makedirs(local_path, mode=0o755)

    files = []
    for file in ss_download.file_list:
        remote_filename = translate_remote_path(ss_download.remote_path, file.remote_filename, settle_date)
        local_filename = translate_local_path(ss_download.local_path, file.local_filename, settle_date)
        if file.status == "1":
            files.append((remote_filename, local_filename, True if file.required == "1" else False))

    if len(files) > 0:
        if ss_download.server_protocol == 'ftp':
            ftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                         ss_download.server_passwd, files, encoding="gbk")
        elif ss_download.server_protocol == 'sftp':
            sftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                          ss_download.server_passwd, files)
        else:
            raise ValueError("server_protocol:%s" % (ss_download.server_protocol))


def zjhf_donwload(settle_date, ss_download):
    def download():
        if len(files) > 0:
            if ss_download.server_protocol == 'ftp':
                ftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                             ss_download.server_passwd, files, encoding="gbk")
            elif ss_download.server_protocol == 'sftp':
                sftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                              ss_download.server_passwd, files)
            else:
                raise ValueError("server_protocol:%s" % (ss_download.server_protocol))

    global remote_logger
    t2_datetime = datetime.datetime.strptime(next_workday(next_workday(settle_date)), "%Y%m%d")
    except_time = t2_datetime + datetime.timedelta(hours=14, minutes=30)
    deadline = t2_datetime + datetime.timedelta(hours=17, minutes=30)

    files = []
    for file in ss_download.file_list:
        remote_filename = translate_remote_path(ss_download.remote_path, file.remote_filename, settle_date)
        local_filename = translate_local_path(ss_download.local_path, file.local_filename, settle_date)
        if file.status == "1":
            files.append((remote_filename, local_filename, True if file.required == "1" else False))

    while True:
        try:
            download()
            break  # 退出
        except Exception as e:
            logger.error(str(e))

        if datetime.datetime.now() > deadline:
            logger.error("下载资金划付文件失败")
            remote_logger.error("下载资金划付文件失败")
            raise Exception("下载资金划付文件失败")
        elif datetime.datetime.now() > except_time:
            except_time = deadline
            logger.warn("没有资金划付文件")
            remote_logger.warn("没有资金划付文件")

        sleep(300)


def post_download(session, settle_date, ss_download):
    logger.info(settle_date)

    def download(deadline):
        session.execute("truncate table fs_dz_result_post")

        while True:
            # POST数据同步
            result = session.execute("select count(1)  from fs_dz_result_post@pro where settledate=:settledate", {
                "settledate": settle_date
            })

            row = result.fetchone()
            count = row[0]

            if count > 0:
                session.execute(
                    "insert into fs_dz_result_post select * from fs_dz_result_post@pro where settledate=:settledate", {
                        "settledate": settle_date
                    })
                session.commit()
                return True

            if datetime.datetime.now() > deadline:
                break

            # sleep 300
            sleep(300)

        return False

    # D1 12:00
    d1_datetime = datetime.datetime.strptime(settle_date, "%Y%m%d") + datetime.timedelta(days=1, hours=12)
    # T1 14:30
    t1_date = next_workday(settle_date)
    t1_datetime = datetime.datetime.strptime(t1_date, "%Y%m%d") + datetime.timedelta(hours=14, minutes=30)

    if not download(d1_datetime):
        logger.warn("没有POST数据,等待POST数据到T1 14:30")
        remote_logger.warn("没有POST数据,等待POST数据到T1 14:30")
        # 等待
        download(t1_datetime)

    logger.info("import_backup %s %s" % (settle_date, "fs_dz_result_post"))

    connection = session.connection()
    dbapi_conn = connection.connection
    cur = None
    try:
        cur = dbapi_conn.cursor()
        cur.callproc("import_backup", [settle_date, "fs_dz_result_post"])
    finally:
        if cur is not None:
            cur.close()


def cp_download(settle_date, ss_download):
    remote_path = translate_remote_path(ss_download.remote_path, "", settle_date)
    local_path = translate_local_path(ss_download.local_path, "", settle_date)

    files = []

    # 获取文件列表
    if ss_download.server_protocol == 'ftp':
        ftp = ftplib.FTP()
        # ftp.set_debuglevel(2)  # 打开调试
        ftp.connect(ss_download.server_host, ss_download.server_port, timeout=60)
        try:
            ftp.login(ss_download.server_user, ss_download.server_passwd)
            ftp.cwd(remote_path)
            names = ftp.nlst()
            for remote_filename in names:
                if remote_filename[15:15 + 10] == '_' + settle_date + '_':
                    files.append(
                        (posixpath.join(remote_path, remote_filename), os.path.join(local_path, remote_filename),
                         True))
        finally:
            try:
                ftp.quit()  # 退出ftp服务器
            except Exception as e:
                logger.error(str(e), exc_info=1)
    elif ss_download.server_protocol == 'sftp':
        transport = paramiko.Transport((ss_download.server_host, ss_download.server_port))
        try:
            transport.connect(username=ss_download.server_user, password=ss_download.server_passwd)
            sftp = paramiko.SFTPClient.from_transport(transport)
            names = sftp.listdir(remote_path)
            for remote_filename in names:
                if remote_filename[15:15 + 10] == '_' + settle_date + '_':
                    files.append(
                        (posixpath.join(remote_path, remote_filename), os.path.join(local_path, remote_filename),
                         True))
        finally:
            transport.close()
    else:
        raise ValueError("server_protocol:%s" % (ss_download.server_protocol))

    # 下载文件
    if len(files) > 0:
        if ss_download.server_protocol == 'ftp':
            ftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                         ss_download.server_passwd, files, encoding="gbk")
        elif ss_download.server_protocol == 'sftp':
            sftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                          ss_download.server_passwd, files)
        else:
            raise ValueError("server_protocol:%s" % (ss_download.server_protocol))


def cpdz_download(settle_date, ss_download):
    remote_path = translate_remote_path(ss_download.remote_path, "", settle_date)
    local_path = translate_local_path(ss_download.local_path, "", settle_date)

    files = []

    # 获取文件列表
    ftp = ftplib.FTP()
    # ftp.set_debuglevel(2)  # 打开调试
    ftp.connect(ss_download.server_host, ss_download.server_port, timeout=60)
    try:
        ftp.login(ss_download.server_user, ss_download.server_passwd)
        dir_info = []
        ftp.dir(remote_path, lambda x: dir_info.append(x.strip().split()))
        for info in dir_info:
            attr = info[0]  # attribute
            remote_dir = info[-1]
            if attr.startswith('d'):  # 目录
                if remote_dir.startswith('9999') and len(remote_dir) == 8:
                    # 99995500/20170801
                    try:
                        ftp.cwd(posixpath.join(remote_path, remote_dir, settle_date))
                        names = ftp.nlst()
                        for remote_filename in names:
                            files.append((posixpath.join(remote_path, remote_dir, settle_date, remote_filename),
                                          os.path.join(local_path,
                                                       '_'.join([remote_dir, settle_date, remote_filename])),
                                          True))
                    except ftplib.error_perm as e:
                        # 忽略cwd错误
                        pass

        # ftp.set_debuglevel(0)  # 关闭调试
    finally:
        try:
            ftp.quit()  # 退出ftp服务器
        except Exception as e:
            logger.error(str(e), exc_info=1)

    # 下载文件
    if len(files) > 0:
        if ss_download.server_protocol == 'ftp':
            ftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                         ss_download.server_passwd, files, encoding="gbk")
        elif ss_download.server_protocol == 'sftp':
            sftp_download(ss_download.server_host, ss_download.server_port, ss_download.server_user,
                          ss_download.server_passwd, files)
        else:
            raise ValueError("server_protocol:%s" % (ss_download.server_protocol))


def process_download(settle_date, file_group):
    logger.info("process_download")
    session = None
    try:
        session = db.get_session()
        subquery = session.query(SsDownload.file_group, func.max(SsDownload.start_date)).filter_by(
            file_group=file_group).filter(
            SsDownload.start_date <= settle_date).group_by(SsDownload.file_group)
        query = session.query(SsDownload).filter(
            tuple_(SsDownload.file_group, SsDownload.start_date).in_(subquery)).filter_by(status="1")
        ss_download_rows = query.all()

        for ss_donwload in ss_download_rows:
            # real_date = utils.settle_date_delta(settle_date, ss_donwload.delta_days)
            for i in range(3):
                try:
                    if ss_donwload.process_mode == "simple":
                        simple_donwload(settle_date, ss_donwload)
                    elif ss_donwload.process_mode == "zjhf":
                        zjhf_donwload(settle_date, ss_donwload)
                    elif ss_donwload.process_mode == "post":
                        post_download(session, settle_date, ss_donwload)
                    elif ss_donwload.process_mode == "cp":
                        cp_download(settle_date, ss_donwload)
                    elif ss_donwload.process_mode == "cpdz":
                        cpdz_download(settle_date, ss_donwload)
                    else:
                        raise ValueError("process_mode:%s" % (ss_donwload.process_mode))

                    break
                except socket.timeout as e:
                    if i == 2:
                        raise
                    # sleep
                    sleep(60)

    finally:
        if session is not None:
            session.close()

    logger.info("process_download end")


def next_workday(settle_date):
    session, cur = None, None
    result = None
    try:
        session = db.get_session()
        connection = session.connection()
        dbapi_conn = connection.connection.connection
        cur = dbapi_conn.cursor()
        result = cur.callfunc("next_workday", cx_Oracle.STRING, [settle_date])
        dbapi_conn.commit()
    finally:
        if cur is not None:
            cur.close()
        if session is not None:
            session.close()

    return result


def main():
    global remote_logger
    parser = argparse.ArgumentParser()
    parser.add_argument("host", default="http://127.0.0.1:8080")
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    parser.add_argument("file_groups")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args, "下载")

    logger.info("开始下载")
    remote_logger.info("开始下载")

    if not utils.notification_scheduled(args):
        logger.info("调用通知失败")
        remote_logger.error("调用通知失败")
        return False

    try:
        settle_date = args.date
        for file_group in args.file_groups.split(","):
            real_date = settle_date
            # 偏移
            if file_group in ("ZJHF2"):
                real_date = next_workday(settle_date)

            # 划付数据T2 14:30
            # if file_group in ("ZJHF1", "ZJHF2"):
            #     t2_date = next_workday(real_date)
            #     t2_datetime = datetime.datetime.strptime(t2_date, "%Y%m%d") + datetime.timedelta(hours=14, minutes=30)
            #
            #     if datetime.datetime.now() < t2_datetime:
            #         logger.info("下载等待文件组%s" % (file_group))
            #         remote_logger.info("下载等待文件组%s" % (file_group))
            #
            #     while datetime.datetime.now() < t2_datetime:
            #         sleep(60)

            logger.info("下载文件组%s" % (file_group))
            remote_logger.info("下载文件组%s" % (file_group))
            process_download(real_date, file_group)
            logger.info("下载文件组%s结束" % (file_group))
            remote_logger.info("下载文件组%s结束" % (file_group))
    except Exception as e:
        logger.error(str(e), exc_info=1)
        logger.error("下载处理失败")
        utils.notification_executed(args, -1, str(e))
        remote_logger.error(str(e))
        remote_logger.error("下载处理失败")
    else:
        logger.info("下载完成")
        utils.notification_executed(args, 0, "下载完成")
        remote_logger.info("下载完成")


if __name__ == "__main__":
    # 初始化日志
    logger = utils.init_log()
    remote_logger = None

    logger.info("start")

    main()
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # process_download("20171202", "UP")
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # process_download("20171202", "UPDZ")
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    logger.info("end")
