# coding=utf-8

# 数据库配置
SQLALCHEMY_DATABASE_URI = 'oracle://fs:fs@192.168.25.64:1521/orcl'
SQLALCHEMY_ECHO = True
SQLALCHEMY_TRACK_MODIFICATIONS = True

# 本地存储路径
DATA_ROOT = '/mnt/hgfs/data/fsdata'
NONTAX_FILE_ROOT = '/mnt/hgfs/data/fs'
NONTAX_THIRD_ROOT = '/mnt/hgfs/data/fsthird'

# 非税上传线上FTP
NONTAX_FTP_OFF_HOST = '127.0.0.1'
NONTAX_FTP_OFF_USER = 'test'
NONTAX_FTP_OFF_PASSWD = 'test'
NONTAX_FTP_OFF_REMOATE_PATH = '/'

# 非税上传线下FTP
NONTAX_FTP_ON_HOST = '127.0.0.1'
NONTAX_FTP_ON_USER = 'test'
NONTAX_FTP_ON_PASSWD = 'test'
NONTAX_FTP_ON_REMOATE_PATH = '/'

# 非税上传线下FTP
NONTAX_FTP_QR_HOST = '127.0.0.1'
NONTAX_FTP_QR_USER = 'test'
NONTAX_FTP_QR_PASSWD = 'test'
NONTAX_FTP_QR_REMOATE_PATH = '/'
# 上传高阳FTP
NONTAX_FTP_GY_HOST = '127.0.0.1'
NONTAX_FTP_GY_USER = 'test'
NONTAX_FTP_GY_PASSWD = 'test'
NONTAX_FTP_GY_REMOATE_PATH = '/'

# 上传远见FTP
NONTAX_FTP_YJ_HOST = '127.0.0.1'
NONTAX_FTP_YJ_USER = 'test'
NONTAX_FTP_YJ_PASSWD = 'test'
NONTAX_FTP_YJ_REMOATE_PATH = '/'

# 导入生产配置
try:
    global_variable = globals()
    prod = __import__('config_prod')
    for key in dir(prod):
        if key.isupper():
            global_variable[key] = getattr(prod, key)
except ImportError as e:
    pass
