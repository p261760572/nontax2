# coding:utf-8
import os
import posixpath
from datetime import datetime
from ftplib import FTP, error_perm


def fs_ftp_download(host, port, user, passwd, remote_path, local_path, settle_date):
    print("%s %s" % (settle_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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
                        local_filename = os.path.join(local_path, remote_dir, settle_date, remote_filename)
                        if not os.path.exists(os.path.dirname(local_filename)):
                            os.makedirs(os.path.dirname(local_filename), mode=0o755)

                        file_handler = open(local_filename, "wb")  # 以写模式在本地打开文件
                        ftp.retrbinary("RETR %s" % (remote_filename), file_handler.write)  # 接收服务器上文件并写入本地文件
                        file_handler.close()
                except error_perm as e:
                    # 忽略cwd错误
                    pass

    ftp.set_debuglevel(0)  # 关闭调试
    ftp.quit()  # 退出ftp服务器

    print("%s %s" % (settle_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


if __name__ == '__main__':
    host = "145.80.30.174"
    port = 21
    user = "feishuiall"
    passwd = "feishuiall_123"
    remote_path = "/"
    local_path = "/home/fs_run/data/fsoff"

    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171101")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171102")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171103")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171104")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171105")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171106")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171107")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171108")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171109")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171110")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171111")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171112")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171113")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171114")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171115")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171116")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171117")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171118")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171119")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171120")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171121")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171122")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171123")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171124")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171125")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171126")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171127")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171128")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171129")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171130")

    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171201")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171202")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171203")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171204")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171205")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171206")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171207")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171208")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171209")
    # fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171210")

    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171211")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171212")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171213")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171214")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171215")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171216")
    fs_ftp_download(host, port, user, passwd, remote_path, local_path, "20171217")