# coding:utf-8
import argparse
import ftplib
import os
import posixpath
from ftplib import FTP

import config
from app import utils


def cwd_tree(ftp, dir):
    print(dir)
    if dir != '/':
        try:
            ftp.cwd(dir)
        except ftplib.error_perm:
            cwd_tree(ftp, posixpath.dirname(dir))
            ftp.mkd(dir)
            ftp.cwd(dir)


def ftp_upload(host, user, passwd, files):
    """
    ftp批量上传文件
    :param host: 服务器
    :param user: 用户名
    :param passwd: 密码
    :param files: 文件list，每个文件是一个dict:return:
    """
    ftp = FTP()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host)  # 连接
    ftp.login(user, passwd)  # 登录，如果匿名登录则用空串代替即可

    for file in files:
        if file.get('ok'):
            continue

        remote_filename = file['remote_filename']
        local_filename = file['local_filename']

        logger.info(remote_filename)
        # 切换目录
        cwd_tree(ftp, posixpath.dirname(remote_filename))

        logger.info(local_filename)
        file_handler = open(local_filename, "rb")  # 以读模式在本地打开文件
        ftp.storbinary("STOR %s" % posixpath.basename(remote_filename), file_handler)  # 读取本地文件并上传到服务器
        file_handler.close()
        file['ok'] = True

    ftp.set_debuglevel(0)  # 关闭调试
    ftp.quit()  # 退出ftp服务器


def process_gy_upload(settle_date):
    files = []
    local_path = os.path.join(config.NONTAX_THIRD_ROOT, settle_date)
    remoate_path = posixpath.join(config.NONTAX_FTP_GY_REMOATE_PATH, settle_date)
    filename = 'a_online.wy.txt'
    if not os.path.isdir(local_path):
        raise Exception('目录不存在%s' % (local_path))

    files.append({
        'remote_filename': posixpath.join(remoate_path, filename),
        'local_filename': os.path.join(local_path, filename)
    })

    ftp_upload(config.NONTAX_FTP_GY_HOST, config.NONTAX_FTP_GY_USER, config.NONTAX_FTP_GY_PASSWD, files)


def process_yj_upload(settle_date):
    files = []
    local_path = os.path.join(config.NONTAX_THIRD_ROOT, settle_date)
    remoate_path = posixpath.join(config.NONTAX_FTP_YJ_REMOATE_PATH, settle_date)
    filename = 'a_yuanjian_%s.wy.txt' % (settle_date)
    if not os.path.isdir(local_path):
        raise Exception('目录不存在%s' % (local_path))

    files.append({
        'remote_filename': posixpath.join(remoate_path, filename),
        'local_filename': os.path.join(local_path, filename)
    })

    ftp_upload(config.NONTAX_FTP_YJ_HOST, config.NONTAX_FTP_YJ_USER, config.NONTAX_FTP_YJ_PASSWD, files)


def main():
    global logger, remote_logger
    logger = utils.init_log()

    parser = argparse.ArgumentParser()
    parser.add_argument("host", default='http://127.0.0.1:8080')
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    if not utils.notification_scheduled(args):
        logger.error('notification_scheduled')
        return False

    settle_date = args.date
    try:
        process_gy_upload(settle_date)
        process_yj_upload(settle_date)
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
    else:
        utils.notification_executed(args, 0, '上传完成')


if __name__ == '__main__':
    main()
