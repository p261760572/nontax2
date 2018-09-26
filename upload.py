# coding:utf-8
import argparse
import ftplib
import os
import posixpath
from ftplib import FTP

import config
from app import db, utils
from app.models import SsUpload, SsUploadList, UploadStatus, UploadFileType


def cwd_tree(ftp, dir):
    print(dir)
    if dir != '/':
        try:
            ftp.cwd(dir)
        except ftplib.error_perm:
            cwd_tree(ftp, posixpath.dirname(dir))
            ftp.mkd(dir)
            ftp.cwd(dir)


def ftp_upload(host, user, passwd, files, port=21):
    """
    ftp批量上传文件
    :param host: 服务器
    :param user: 用户名
    :param passwd: 密码
    :param files: 文件list，每个文件是一个dict:return:
    """
    ftp = FTP()
    # ftp.set_debuglevel(2)  # 打开调试级别2，显示详细信息
    ftp.connect(host, port)  # 连接
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


def process_off_upload(settle_date, ss_upload_list):
    files = []
    # nontax_cd_list = os.listdir(config.NONTAX_FILE_ROOT)
    # for nontax_cd in nontax_cd_list:
    #     remoate_path = posixpath('/', nontax_cd, settle_date)
    #     local_path = os.path.join(config.NONTAX_FILE_ROOT, nontax_cd, settle_date)
    #     if not os.path.isdir(local_path):
    #         raise
    #     bank_cd_list = os.listdir(local_path)
    #     for bank_cd in bank_cd_list:
    #         if re.match('\w{8}\.txt', bank_cd):
    #             files.append({
    #                 'remote_filename': posixpath.join(remoate_path, bank_cd),
    #                 'local_filename': os.path.join(local_path, bank_cd)
    #             })


    for file in ss_upload_list:
        files.append({
            'remote_filename': posixpath.join(config.NONTAX_FTP_OFF_REMOATE_PATH,
                                              file.local_filename.replace('\\', '/')),
            'local_filename': os.path.join(config.NONTAX_FILE_ROOT, file.local_filename)
        })

    ftp_upload(config.NONTAX_FTP_OFF_HOST, config.NONTAX_FTP_OFF_USER, config.NONTAX_FTP_OFF_PASSWD, files)


def process_on_upload(settle_date, ss_upload_list):
    files = []
    # nontax_cd_list = os.listdir(config.NONTAX_FILE_ROOT)
    # for nontax_cd in nontax_cd_list:
    #     remoate_path = posixpath('/', nontax_cd, settle_date)
    #     local_path = os.path.join(config.NONTAX_FILE_ROOT, nontax_cd, settle_date)
    #     if not os.path.isdir(local_path):
    #         raise
    #     bank_cd_list = os.listdir(local_path)
    #     for bank_cd in bank_cd_list:
    #         if re.match('\w{8}\.wy\.txt', bank_cd):
    #             files.append({
    #                 'remote_filename': posixpath.join(remoate_path, bank_cd),
    #                 'local_filename': os.path.join(local_path, bank_cd)
    #             })


    for file in ss_upload_list:
        files.append({
            'remote_filename': posixpath.join(config.NONTAX_FTP_ON_REMOATE_PATH,
                                              file.local_filename.replace('\\', '/')),
            'local_filename': os.path.join(config.NONTAX_FILE_ROOT, file.local_filename)
        })

    ftp_upload(config.NONTAX_FTP_ON_HOST, config.NONTAX_FTP_ON_USER, config.NONTAX_FTP_ON_PASSWD, files)

def process_qr_upload(settle_date, ss_upload_list):
    files = []
    # nontax_cd_list = os.listdir(config.NONTAX_FILE_ROOT)
    # for nontax_cd in nontax_cd_list:
    #     remoate_path = posixpath('/', nontax_cd, settle_date)
    #     local_path = os.path.join(config.NONTAX_FILE_ROOT, nontax_cd, settle_date)
    #     if not os.path.isdir(local_path):
    #         raise
    #     bank_cd_list = os.listdir(local_path)
    #     for bank_cd in bank_cd_list:
    #         if re.match('\w{8}\.wy\.txt', bank_cd):
    #             files.append({
    #                 'remote_filename': posixpath.join(remoate_path, bank_cd),
    #                 'local_filename': os.path.join(local_path, bank_cd)
    #             })


    for file in ss_upload_list:
        files.append({
            'remote_filename': posixpath.join(config.NONTAX_FTP_QR_REMOATE_PATH,
                                              file.local_filename.replace('\\', '/')),
            'local_filename': os.path.join(config.NONTAX_FILE_ROOT, file.local_filename)
        })

    ftp_upload(config.NONTAX_FTP_QR_HOST, config.NONTAX_FTP_QR_USER, config.NONTAX_FTP_QR_PASSWD, files, config.NONTAX_FTP_QR_PORT)

def main():
    global logger, remote_logger
    logger = utils.init_log()
    logger.info('start')

    parser = argparse.ArgumentParser()
    parser.add_argument("host", default='http://127.0.0.1:8080')
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    parser.add_argument('-n', '--batch-no')
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    if not utils.notification_scheduled(args):
        logger.error('notification_scheduled')
        return False

    remote_logger.info('开始上传')

    session = None
    ss_upload = None
    try:
        settle_date = args.date
        batch_no = args.batch_no

        session = db.get_session()

        query = session.query(SsUpload).filter_by(settle_date=settle_date, batch_no=batch_no,
                                                  status=UploadStatus.ACQUIRED).with_for_update()

        logger.info(query)
        ss_upload = query.first()

        if ss_upload:
            # ss_upload.status = UploadStatus.ACQUIRED
            # session.commit()

            ss_upload.status = UploadStatus.EXECUTING
            session.commit()

            query_list = session.query(SsUploadList)
            ss_upload_list = query_list.filter_by(batch_no=ss_upload.batch_no, file_type=UploadFileType.OFF).all()
            process_off_upload(settle_date, ss_upload_list)

            ss_upload_list = query_list.filter_by(batch_no=ss_upload.batch_no, file_type=UploadFileType.ON).all()
            process_on_upload(settle_date, ss_upload_list)

            ss_upload_list = query_list.filter_by(batch_no=ss_upload.batch_no, file_type=UploadFileType.QR).all()
            process_qr_upload(settle_date, ss_upload_list)

            ss_upload.status = UploadStatus.COMPLETE
        session.commit()

    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.info('上传失败')
        if ss_upload:
            ss_upload.status = UploadStatus.ERROR
            session.commit()
    else:
        logger.info('上传完成')
        utils.notification_executed(args, 0, '上传完成')
        remote_logger.info('上传完成')


if __name__ == '__main__':
    # 初始化日志
    main()
