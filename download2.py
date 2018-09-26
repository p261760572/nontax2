# coding:utf-8
import argparse
import json
import os
import posixpath
import shutil
import socket
import ssl
import time
from datetime import datetime
from ftplib import FTP, FTP_TLS, parse227, parse229, error_perm

import paramiko as paramiko
from sqlalchemy import func, tuple_

import db
import utils
from models import SsDownload


class FTPS_IMPLICIT(FTP_TLS):
    def __init__(self, host="", user="", passwd="", acct="", keyfile=None, certfile=None, timeout=30):
        FTP_TLS.__init__(self, host=host, user=user, passwd=passwd, acct=acct, keyfile=keyfile, certfile=certfile,
                         timeout=timeout)

    def connect(self, host="", port=0, timeout=-999):
        if host != "":
            self.host = host
        if port > 0:
            self.port = port
        if timeout != -999:
            self.timeout = timeout
        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        self.af = self.sock.family

        self.sock = ssl.wrap_socket(self.sock, self.keyfile, self.certfile, ssl_version=ssl.PROTOCOL_TLSv1)

        self.file = self.sock.makefile("r")
        self.welcome = self.getresp()

        return self.welcome

    def makepasv(self):
        if self.af == socket.AF_INET:
            host, port = parse227(self.sendcmd("PASV"))
        else:
            host, port = parse229(self.sendcmd("EPSV"), self.sock.getpeername())
        return self.host, port


def ftp_download(host, user, passwd, filenames):
    """
    ftp批量下载文件
    :param host: 服务器
    :param user: 用户名
    :param passwd: 密码
    :param filenames: 文件list，每个文件是一个dict:return:
    """
    failed = []
    ftp = FTP()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host, timeout=60)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可

    for file in filenames:
        if file.get('ok'):
            continue

        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        logger.info(remote_filename)
        remote_logger.info(remote_filename)
        ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        file_handler = open(local_filename, "wb").write  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler)  # 接收服务器上文件并写入本地文件
        file_handler.close()
        logger.info("%s end" % (remote_filename))
        file['ok'] = True

    ftp.set_debuglevel(0)  # 关闭调试
    ftp.quit()  # 退出ftp服务器

    return failed


def ftp_download_dir(host, user, passwd, row):
    """
    ftp批量下载文件
    :param host: 服务器
    :param user: 用户名
    :param passwd: 密码
    :param filenames: 文件list，每个文件是一个tuple:(远程文件,本地文件)
    :return:
    """
    remote_path = row['remote_path']
    local_path = row['local_path']

    failed = []
    ftp = FTP()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host, timeout=60)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
    ftp.cwd(remote_path)  # 选择操作目录
    filenames = ftp.nlst()
    for remote_filename in filenames:
        row['file_list'].append({
            'remote_filename': posixpath.join(remote_path, remote_filename),
            'local_filename': os.path.join(local_path, remote_filename),
            'required': '1'
        })
    # 列表ok
    row['ok'] = True

    for file in row['file_list']:
        if file.get('ok'):
            continue

        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        logger.info(remote_filename)
        remote_logger.info(remote_filename)
        # ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        file_handler = open(local_filename, "wb").write  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler)  # 接收服务器上文件并写入本地文件
        file_handler.close()
        logger.info("%s end" % (remote_filename))
        file['ok'] = True

    ftp.set_debuglevel(0)  # 关闭调试
    ftp.quit()  # 退出ftp服务器

    return failed


def ftps_download(host, user, passwd, filenames):
    """
    ftps批量下载文件
    :param host: 服务器
    :param user: 用户名
    :param passwd: 密码
    :param filenames: 文件list，每个文件是一个tuple:(远程文件,本地文件)
    :return:
    """
    ftp = FTPS_IMPLICIT()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host, 990, timeout=60)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
    for remote_filename, local_filename, required in filenames:
        logger.info(remote_filename)
        remote_logger.info(remote_filename)

        ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        file_handler = open(local_filename, "wb").write  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler)  # 接收服务器上文件并写入本地文件
        file_handler.close()
        logger.info("%s end" % (remote_filename))
    ftp.set_debuglevel(0)  # 关闭调试
    ftp.quit()  # 退出ftp服务器


def sftp_download(host, user, passwd, remote_filename, local_filename):
    """
    sftp批量下载文件
    :param host: 服务器
    :param user: 用户名
    :param passwd: 密码
    :param filenames: 文件list，每个文件是一个tuple:(远程文件,本地文件)
    :return:
    """
    logger.info(remote_filename)
    remote_logger.info(remote_filename)

    transport = paramiko.Transport((host, 22))
    try:
        transport.connect(username=user, password=passwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        # for remote_filename, local_filename in filenames:
        sftp.get(remote_filename, local_filename)
        logger.info("%s end" % (remote_filename))
    finally:
        transport.close()


def file_download(filenames):
    for file in filenames:
        if file.get('ok'):
            continue

        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        shutil.copyfile(remote_filename, local_filename)
        file['ok'] = True


def file_download_dir(row):
    remote_path = row['remote_path']
    local_path = row['local_path']

    filenames = os.listdir(remote_path)
    for remote_filename in filenames:
        row['file_list'].append({
            'remote_filename': os.path.join(remote_path, remote_filename),
            'local_filename': os.path.join(local_path, remote_filename),
            'required': '1'
        })
    # 列表ok
    row['ok'] = True

    for file in row['file_list']:
        if file.get('ok'):
            continue

        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        shutil.copyfile(remote_filename, local_filename)
        file['ok'] = True


def is_done(row):
    done = True
    for file in row['file_list']:
        if file.get("ok") != True:
            done = False
            break
    return done


def download_all(settle_date, rows):
    for row in rows:

        process_mode = row['process_mode']
        server_protocol = row['server_protocol']
        server_host = row['server_host']
        server_user = row['server_user']
        server_passwd = row['server_passwd']
        local_path = row['local_path']

        if not os.path.exists(local_path):
            os.makedirs(local_path, mode=0o755)

        if process_mode == 'all' and not row.get('ok'):
            if server_protocol == 'ftp':
                ftp_download_dir(server_host, server_user, server_passwd, row)
            elif server_protocol == 'file':
                file_download_dir(row)
        elif process_mode == 'one' or row.get('ok'):
            if is_done(row):
                continue

            if server_protocol == 'ftp':
                ftp_download(server_host, server_user, server_passwd, row['file_list'])
            elif server_protocol == 'file':
                file_download(row['file_list'])


def simple_ftp_donwload(settle_date, row):
    settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

    host = row['server_host']
    port = row['server_port']
    user = row['server_user']
    passwd = row['server_passwd']
    remote_path = row['remote_path']
    local_path = row['local_path']
    file_list = row['file_list']

    logger.info(json.dumps(row, ensure_ascii=False))

    seconds = 0
    interval = 180
    # 24小时内
    while seconds < 3 * interval or (datetime.now() - settle_datetime).total_seconds() < 3600 * (24 + 24 + 15):
        try:
            ftp = FTP()
            # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
            ftp.connect(host, port, timeout=60)  # 连接
            ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
            ftp.cwd(remote_path.encode('gbk').decode('latin1'))  # 选择操作目录

            for file in file_list:
                remote_filename = file['remote_filename']
                local_filename = file['local_filename']
                status = file['status']
                required = file['required']
                if status == '1':
                    logger.info(remote_filename)
                    remote_logger.info(remote_filename)

                    # ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
                    try:
                        file_handler = open(local_filename, "wb")  # 以写模式在本地打开文件
                        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename).encode('gbk').decode('latin1'),
                                       file_handler.write)  # 接收服务器上文件并写入本地文件
                        file_handler.close()
                        logger.info("%s end" % (remote_filename))
                    except error_perm as e:
                        # 删除空文件
                        if os.path.exists(local_filename) and os.path.getsize(local_filename) == 0:
                            os.remove(local_filename)

                        if required != '1':
                            # 忽略非必需文件
                            pass
                        else:
                            raise

            # ftp.set_debuglevel(0)  # 关闭调试
            ftp.quit()  # 退出ftp服务器

            # 退出
            break
        except Exception as e:
            # 等待(interval)s
            time.sleep(interval)
            seconds += interval
    else:
        logger.info("ftp://%s:%d%s,下载超时" % (host, port, remote_path))
        remote_logger.info("ftp://%s:%d%s,下载超时" % (host, port, remote_path))
        raise Exception("ftp://%s:%d%s,下载超时" % (host, port, remote_path))


def simple_sftp_donwload(settle_date, row):
    settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

    host = row['server_host']
    port = row['server_port']
    user = row['server_user']
    passwd = row['server_passwd']
    remote_path = row['remote_path']
    local_path = row['local_path']
    file_list = row['file_list']

    seconds = 0
    interval = 180
    # 24小时内
    while seconds < 3 * interval or (datetime.now() - settle_datetime).total_seconds() < 3600 * (24 + 24 + 15):
        try:
            transport = paramiko.Transport((host, port))
            try:
                transport.connect(username=user, password=passwd)
                sftp = paramiko.SFTPClient.from_transport(transport)

                # for remote_filename, local_filename in filenames:

                for file in file_list:
                    remote_filename = file['remote_filename']
                    local_filename = file['local_filename']
                    status = file['status']
                    required = file['required']
                    if status == '1':
                        logger.info(local_filename)
                        logger.info(remote_filename)
                        remote_logger.info(remote_filename)

                        try:
                            sftp.get(remote_filename.encode('utf-8'), local_filename)
                            logger.info("%s end" % (remote_filename))
                        except Exception as e:
                            logger.error(str(e), exc_info=1)
                            # 删除空文件
                            if os.path.exists(local_filename) and os.path.getsize(local_filename) == 0:
                                os.remove(local_filename)

                            if required != '1':
                                # 忽略非必需文件
                                pass
                            else:
                                raise
            finally:
                transport.close()

            # 退出
            break
        except Exception as e:
            logger.error(str(e), exc_info=1)
            # 等待(interval)s
            time.sleep(interval)
            seconds += interval
    else:
        logger.info("sftp://%s:%d%s,下载超时" % (host, port, remote_path))
        remote_logger.info("sftp://%s:%d%s,下载超时" % (host, port, remote_path))
        raise Exception("sftp://%s:%d%s,下载超时" % (host, port, remote_path))


def cp_ftp_download(settle_date, row):
    settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

    host = row['server_host']
    port = row['server_port']
    user = row['server_user']
    passwd = row['server_passwd']
    remote_path = row['remote_path']
    local_path = row['local_path']

    seconds = 0
    interval = 300
    # 24小时内
    while seconds < 3 * interval or (datetime.now() - settle_datetime).total_seconds() < 3600 * (24 + 24 + 15):
        try:
            download_list = []
            ftp = FTP()
            # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
            ftp.connect(host, port, timeout=60)  # 连接
            ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
            ftp.cwd(remote_path)  # 选择操作目录
            filenames = ftp.nlst()
            for remote_filename in filenames:
                if remote_filename[15:15 + 10] == '_' + settle_date + '_':
                    download_list.append({
                        'remote_filename': posixpath.join(remote_path, remote_filename),
                        'local_filename': os.path.join(local_path, remote_filename)
                    })

            for file in download_list:
                remote_filename = file['remote_filename']
                local_filename = file['local_filename']

                logger.info(remote_filename)
                remote_logger.info(remote_filename)

                # ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
                file_handler = open(local_filename, "wb")  # 以写模式在本地打开文件
                ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler.write)  # 接收服务器上文件并写入本地文件
                file_handler.close()
                logger.info("%s end" % (remote_filename))

            ftp.set_debuglevel(0)  # 关闭调试
            ftp.quit()  # 退出ftp服务器

            # TODO 此判断方法有缺陷
            if len(download_list) == 0:
                pass

            # 退出
            break

        except Exception as e:
            # 等待(interval)s
            time.sleep(interval)
            seconds += interval
    else:
        logger.info("ftp://%s:%d%s,下载超时" % (host, port, remote_path))
        remote_logger.info("ftp://%s:%d%s,下载超时" % (host, port, remote_path))
        raise Exception("ftp://%s:%d%s,下载超时" % (host, port, remote_path))


def cp_sftp_download(settle_date, row):
    settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

    host = row['server_host']
    port = row['server_port']
    user = row['server_user']
    passwd = row['server_passwd']
    remote_path = row['remote_path']
    local_path = row['local_path']

    seconds = 0
    interval = 300
    # 24小时内
    while seconds < 3 * interval or (datetime.now() - settle_datetime).total_seconds() < 3600 * (24 + 24 + 15):
        try:
            download_list = []

            transport = paramiko.Transport((host, port))
            try:
                transport.connect(username=user, password=passwd)
                sftp = paramiko.SFTPClient.from_transport(transport)
                # for remote_filename, local_filename in filenames:

                filenames = sftp.listdir(remote_path)
                for remote_filename in filenames:
                    if remote_filename[15:15 + 10] == '_' + settle_date + '_':
                        download_list.append({
                            'remote_filename': posixpath.join(remote_path, remote_filename),
                            'local_filename': os.path.join(local_path, remote_filename)
                        })

                for file in download_list:
                    remote_filename = file['remote_filename']
                    local_filename = file['local_filename']

                    logger.info(remote_filename)
                    remote_logger.info(remote_filename)

                    sftp.get(remote_filename, local_filename)
                    logger.info("%s end" % (remote_filename))

            finally:
                transport.close()

            # TODO 此判断方法有缺陷
            if len(download_list) == 0:
                pass

            # 退出
            break

        except Exception as e:
            # 等待(interval)s
            time.sleep(interval)
            seconds += interval
    else:
        logger.info("sftp://%s:%d%s,下载超时" % (host, port, remote_path))
        remote_logger.info("sftp://%s:%d%s,下载超时" % (host, port, remote_path))
        raise Exception("sftp://%s:%d%s,下载超时" % (host, port, remote_path))


def cpdz_ftp_download(settle_date, row):
    host = row['server_host']
    port = row['server_port']
    user = row['server_user']
    passwd = row['server_passwd']
    remote_path = row['remote_path']
    local_path = row['local_path']

    download_list = []
    ftp = FTP()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host, port, timeout=60)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
    # ftp.cwd(remote_path)  # 选择操作目录
    dir_info = []
    ftp.dir(remote_path, lambda x: dir_info.append(x.strip().split()))
    for info in dir_info:
        attr = info[0]  # attribute
        remote_dir = info[-1]
        if attr.startswith('d'):  # 目录
            if remote_dir.startswith('9999') and len(remote_dir) == 8:
                # 99995500/20170801
                # print(posixpath.join(remote_path, remote_dir, settle_date))
                try:
                    ftp.cwd(posixpath.join(remote_path, remote_dir, settle_date))
                    filenames = ftp.nlst()
                    for remote_filename in filenames:
                        download_list.append({
                            'remote_filename': posixpath.join(remote_path, remote_dir, settle_date, remote_filename),
                            'local_filename': os.path.join(local_path,
                                                           '_'.join([remote_dir, settle_date, remote_filename]))
                        })
                except error_perm as e:
                    # 忽略cwd错误
                    pass

    for file in download_list:
        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        logger.info(remote_filename)
        remote_logger.info(remote_filename)

        ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        if not os.path.exists(os.path.dirname(local_filename)):
            os.makedirs(os.path.dirname(local_filename), mode=0o755)

        file_handler = open(local_filename, "wb")  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler.write)  # 接收服务器上文件并写入本地文件
        file_handler.close()
        logger.info("%s end" % (remote_filename))

    ftp.set_debuglevel(0)  # 关闭调试
    ftp.quit()  # 退出ftp服务器


def post_download(session, settle_date, row):
    settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

    logger.info("开始POST数据同步")
    remote_logger.info("开始POST数据同步")
    # POST数据同步
    seconds = 0
    interval = 300
    # 24小时内
    while seconds < 3 * interval or (datetime.now() - settle_datetime).total_seconds() < 3600 * (24 + 24 + 15):
        result = session.execute("select count(1)  from fs_dz_result_post@pro where settledate=:settledate", {
            "settledate": settle_date
        })

        row = result.fetchone()
        count = row[0]

        if count > 0:
            session.execute("truncate table fs_dz_result_post")
            session.execute(
                "insert into fs_dz_result_post select * from fs_dz_result_post@pro where settledate=:settledate", {
                    "settledate": settle_date
                })
            session.commit()
            break

        # 如果没有数据，等待interval(s)
        time.sleep(interval)
        seconds += interval
    else:
        logger.warn("没有POST数据")
        remote_logger.warn("没有POST数据")
        # logger.info("POST数据同步,失败")
        # remote_logger.info("POST数据同步,失败")
        # raise Exception("下载POST失败")

    logger.info("POST数据同步,完成")
    remote_logger.info("POST数据同步,完成")


def process_download(settle_date):
    logger.info('process_download')
    session = None
    try:
        now_datetime = datetime.now()
        settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

        session = db.get_session()
        subquery = session.query(SsDownload.file_group, func.max(SsDownload.start_date)).filter(
            SsDownload.start_date <= settle_date).group_by(SsDownload.file_group)
        query = session.query(SsDownload).filter(
            tuple_(SsDownload.file_group, SsDownload.start_date).in_(subquery)).filter(SsDownload.status == '1')
        rows = query.all()
        # 转dict
        rows = [row.to_dict() for row in rows]

        # 转换路径
        for row in rows:
            logger.info(json.dumps(row, ensure_ascii=False))
            real_date = utils.settle_date_delta(settle_date, row['delta_days'])
            utils.translate(real_date, row)

            process_mode = row['process_mode']
            server_protocol = row['server_protocol']
            local_path = row['local_path']

            if not os.path.exists(local_path):
                os.makedirs(local_path, mode=0o755)

            if process_mode == 'simple':
                if server_protocol == 'ftp':
                    simple_ftp_donwload(real_date, row)
                elif server_protocol == 'sftp':
                    simple_sftp_donwload(real_date, row)
                else:
                    raise ValueError('server_protocol:%s' % (server_protocol))
            elif process_mode == 'cp':
                if server_protocol == 'ftp':
                    cp_ftp_download(real_date, row)
                elif server_protocol == 'sftp':
                    cp_sftp_download(real_date, row)
                else:
                    raise ValueError('server_protocol:%s' % (server_protocol))
            elif process_mode == 'cpdz':
                cpdz_ftp_download(real_date, row)
            elif process_mode == 'post':
                post_download(session, real_date, row)
            else:
                raise ValueError('process_mode:%s' % (process_mode))
    finally:
        if session is not None:
            session.close()

    logger.info('process_download end')


def main():
    global remote_logger
    parser = argparse.ArgumentParser()
    parser.add_argument("host", default='http://127.0.0.1:8080')
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args, "下载")

    remote_logger.info('开始下载')

    if not utils.notification_scheduled(args):
        remote_logger.error('调用通知失败')
        return False

    try:
        settle_date = args.date
        process_download(settle_date)
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.error(str(e))
        remote_logger.error('下载处理失败')
    else:
        utils.notification_executed(args, 0, '下载完成')
        remote_logger.info('下载完成')


if __name__ == '__main__':
    # 初始化日志
    logger = utils.init_log()
    remote_logger = None

    logger.info('start')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    main()
    # process_download('20170818')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info('end')
