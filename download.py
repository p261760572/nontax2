# coding:utf-8
import argparse
import os
import posixpath
import shutil
import socket
import ssl
import time
from datetime import datetime
from ftplib import FTP, FTP_TLS, parse227, parse229

import paramiko as paramiko

import config
import db
import utils
from models import SsFile


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
    ftp.connect(host)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可

    for file in filenames:
        if file.get('ok'):
            continue

        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        print(posixpath.dirname(remote_filename))
        ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        file_handler = open(local_filename, "wb").write  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler)  # 接收服务器上文件并写入本地文件
        file_handler.close()
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
    ftp.connect(host)  # 连接
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

        # ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        file_handler = open(local_filename, "wb").write  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler)  # 接收服务器上文件并写入本地文件
        file_handler.close()
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
    ftp.connect(host, 990)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可
    for remote_filename, local_filename, required in filenames:
        ftp.cwd(posixpath.dirname(remote_filename))  # 选择操作目录
        file_handler = open(local_filename, "wb").write  # 以写模式在本地打开文件
        ftp.retrbinary("RETR %s" % posixpath.basename(remote_filename), file_handler)  # 接收服务器上文件并写入本地文件
        file_handler.close()
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
    transport = paramiko.Transport((host, 22))
    try:
        transport.connect(username=user, password=passwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        # for remote_filename, local_filename in filenames:
        sftp.get(remote_filename, local_filename)
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

        n = 0
        while True:
            download_all(settle_date, rows)

            # 尝试5次且超过2小时，放弃非必需文件
            if n >= 5 and (datetime.now() - now_datetime).total_seconds() > 3600 * 2:
                for row in rows:
                    for file in row['file_list']:
                        if file.get('required') != '1':
                            file['ok'] = True

            done = True
            for row in rows:
                if not is_done(row):
                    done = False
                    break

            if done:
                break

            # 尝试3次且超过24小时
            if n >= 3 and (datetime.now() - now_datetime).total_seconds() > 3600 * 24:
                raise Exception("下载文件异常")

            time.sleep(300)

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
