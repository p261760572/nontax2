# coding=utf-8

# 数据库配置
SQLALCHEMY_DATABASE_URI = 'oracle://fs:oracle123@127.0.0.1:1521/oradb'
SQLALCHEMY_ECHO = False
SQLALCHEMY_TRACK_MODIFICATIONS = True

# 本地存储路径
DATA_ROOT = '/home/fs_run/data/fsdata'
NONTAX_FILE_ROOT = '/home/fs_run/data/fs'
NONTAX_THIRD_ROOT = '/home/fs_run/data/fsthird'

# 非税上传线下FTP
NONTAX_FTP_OFF_HOST = '127.0.0.1'
NONTAX_FTP_OFF_USER = 'test'
NONTAX_FTP_OFF_PASSWD = 'test'
NONTAX_FTP_OFF_REMOATE_PATH = '/off'

# 非税上传线上FTP
NONTAX_FTP_ON_HOST = '127.0.0.1'
NONTAX_FTP_ON_USER = 'test'
NONTAX_FTP_ON_PASSWD = 'test'
NONTAX_FTP_ON_REMOATE_PATH = '/on'

# 非税上传线上FTP
NONTAX_FTP_QR_HOST = '145.80.30.201'
NONTAX_FTP_QR_PORT = 17421
NONTAX_FTP_QR_USER = 'writer-feishui'
NONTAX_FTP_QR_PASSWD = 'feishui13061'
NONTAX_FTP_QR_REMOATE_PATH = '/'
# 上传高阳FTP
NONTAX_FTP_GY_HOST = '127.0.0.1'
NONTAX_FTP_GY_USER = 'test'
NONTAX_FTP_GY_PASSWD = 'test'
NONTAX_FTP_GY_REMOATE_PATH = '/gy'

# 上传远见FTP
NONTAX_FTP_YJ_HOST = '127.0.0.1'
NONTAX_FTP_YJ_USER = 'test'
NONTAX_FTP_YJ_PASSWD = 'test'
NONTAX_FTP_YJ_REMOATE_PATH = '/yj'
