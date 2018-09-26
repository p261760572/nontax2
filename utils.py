# coding=utf-8
import calendar
import logging
import os
import posixpath
import re
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from io import StringIO
from logging.handlers import RotatingFileHandler

import requests

import config


def makedict(cursor):
    cols = [d[0].lower() for d in cursor.description]

    def createrow(*args):
        values = []
        for v in args:
            # 解决日期类型的序列化问题
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, Decimal):
                v = float(v)
            values.append(v)
        return dict(zip(cols, values))

    return createrow


def settle_date_delta(settle_date, delta_days):
    settle_datetime = datetime.strptime(settle_date, "%Y%m%d")
    settle_datetime += timedelta(days=delta_days)
    return settle_datetime.strftime("%Y%m%d")


def replace_path(settle_date):
    settle_yyyy = settle_date[0:4]
    settle_yy = settle_date[2:4]
    settle_mm = settle_date[4:6]
    settle_dd = settle_date[6:8]

    def replace(m):
        text = m.group()
        text = text[1:-1]
        return text.replace('YYYY', settle_yyyy).replace('YY', settle_yy).replace('MM', settle_mm).replace('DD',
                                                                                                           settle_dd)

    return replace


def translate_path(path, sys_date):
    path = re.sub('\{\w+\}', replace_path(sys_date), path or '')
    # if is_local:
    #     # path = path.replace('/', os.sep)
    #     pass
    return path


def _translate(settle_date, row):
    if row.get('remote_path'):
        remote_path = row['remote_path']
        remote_path = translate_path(remote_path, settle_date)
        row['remote_path'] = remote_path

    if row.get('local_path'):
        local_path = row['local_path']
        local_path = translate_path(local_path, settle_date)
        local_path = os.path.join(config.DATA_ROOT, local_path)
        row['local_path'] = local_path

    if row.get('file_list'):
        for file in row['file_list']:
            if file.get('remote_filename'):
                remote_filename = file['remote_filename']
                remote_filename = translate_path(remote_filename, settle_date)
                remote_filename = posixpath.join(remote_path, remote_filename)
                file['remote_filename'] = remote_filename

            if file.get('local_filename'):
                local_filename = file['local_filename']
                local_filename = translate_path(local_filename, settle_date)
                local_filename = os.path.join(local_path, local_filename)
                file['local_filename'] = local_filename


def translate(settle_date, rows):
    """
    转换路径
    """
    if isinstance(rows, list):
        for row in rows:
            _translate(settle_date, row)
    else:
        _translate(settle_date, rows)


def crontab_match(settle_date, crontab):
    day, month, weekday = crontab.split()
    if day == "*":
        day = [i for i in range(1, 32)]
    else:
        day = [int(s) for s in day.split(",")]

    if month == "*":
        month = [i for i in range(1, 13)]
    else:
        month = [int(s) for s in month.split(",")]

    if weekday == "*":
        weekday = [i for i in range(1, 8)]
    else:
        weekday = [int(s) for s in weekday.split(",")]

    now_date = datetime.strptime(settle_date, '%Y%m%d')
    _, last_day = calendar.monthrange(now_date.year, now_date.month)

    if ((now_date.day == last_day and 31 in day) or now_date.day in day) \
            and now_date.month in month and now_date.weekday() + 1 in weekday:
        return True

    # if now_date.day in day and now_date.month in month and now_date.weekday() + 1 in weekday:
    #     return True

    return False


# SQL解析
_TK_ILLEGAL = 0
_TK_OTHER = 1
_TK_STRING = 2
_TK_BIND_VAR = 3


def _get_token(sql, sql_len, start):
    i = start

    if sql[i] == "'":
        i += 1
        while i < sql_len:
            if sql[i] != "'":
                i += 1
            elif sql[i:i + 2] == "''":
                i += 2
            else:
                i += 1  # if sql[i] == "'"
                return i - start, _TK_STRING
        return i - start, _TK_ILLEGAL
    elif sql[i] == ":":
        i += 1
        while i < sql_len and (sql[i].isalnum() or sql[i] == "_"):
            i += 1
        token_len = i - start
        if token_len > 1:
            return token_len, _TK_BIND_VAR
        return token_len, _TK_ILLEGAL
    else:
        while i < sql_len and sql[i] not in ("'", ":"):
            i += 1
        return i - start, _TK_OTHER


def to_string(obj):
    if obj is None:
        return ''
    elif isinstance(obj, Decimal):
        return str(float(obj))

    return str(obj)


def parse_sql_bind(sql):
    """
    解析SQL的绑定变量名称
    :param sql: 
    :return: 
    """
    bind = []
    sql_len = len(sql)
    i = 0
    while i < sql_len:
        token_len, token_type = _get_token(sql, sql_len, i)
        if token_type == _TK_ILLEGAL:
            raise SyntaxError("解析SQL出错:%s" % sql)

        token = sql[i:i + token_len]
        if token_type == _TK_BIND_VAR:
            name = token[1:]
            bind.append(name)
        i = i + token_len

    return bind


def generate_print_sql(sql, bind):
    sqlbuf = StringIO()
    count = 0
    sql_len = len(sql)
    i = 0
    while i < sql_len:
        token_len, token_type = _get_token(sql, sql_len, i)
        if token_type == _TK_ILLEGAL:
            raise Exception("解析SQL出错:%s" % sql)

        token = sql[i:i + token_len]
        if token_type == _TK_BIND_VAR:
            if count < len(bind):
                name = token[1:]
                sqlbuf.write("'")
                sqlbuf.write(to_string(bind[count]))
                sqlbuf.write("'")
            else:
                sqlbuf.write("null")
            count += 1
        else:
            sqlbuf.write(token)

        i = i + token_len

    return sqlbuf.getvalue()


def notification_scheduled(args):
    r = requests.post(args.host + '/notification/scheduled', json={
        'entry_id': args.entry_id,
        'pid': os.getpid()
    })
    if r.status_code == requests.codes.ok:
        rjson = r.json()
        if rjson.get('errcode') == 0:
            return True

    notification_executed(args, -1, args.date + ',' + '通知调度中心失败')
    return False


def notification_executed(args, errcode, errmsg):
    if errmsg.startswith("ORA-20999: "):
        errmsg = errmsg.split("\n", 1)[0][11:]
    elif errmsg.startswith("ORA-"):
        errmsg = errmsg.split("\n", 1)[0]

    r = requests.post(args.host + '/notification/executed', json={
        'entry_id': args.entry_id,
        'pid': os.getpid(),
        'errcode': errcode,
        'errmsg': args.date + ',' + errmsg
    })

    if r.status_code == requests.codes.ok:
        rjson = r.json()
        if rjson.get('errcode') == 0:
            return True

    return False


def notification_logging(args, msg, priority, name, tag=None):
    if msg.startswith("ORA-20999: "):
        msg = msg.split("\n", 1)[0][11:]

    if priority == 'WARNING' or property == 'ERROR' or priority == 'CRITICAL':
        msg = args.date + ',' + msg

    r = requests.post(args.host + '/notification/logging', json={
        'entry_id': args.entry_id,
        'pid': os.getpid(),
        'message': msg,
        'priority': priority,
        'facility': name,
        'tag': tag
    })

    if r.status_code == requests.codes.ok:
        rjson = r.json()
        if rjson.get('errcode') == 0:
            return True

    return False


def init_log():
    bin_path = os.path.dirname(sys.argv[0])  # os.getcwd()
    app_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    log_file = os.path.join(bin_path, "%s.log" % app_name)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    file_handler = RotatingFileHandler(log_file, maxBytes=1 * 1024 * 1024, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(process)d %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
    ))
    logger.addHandler(file_handler)

    return logger


class RemoteHandler(logging.Handler):
    def __init__(self, args, tag=None):
        logging.Handler.__init__(self)
        self.args = args
        self.tag = tag

    def emit(self, record):
        self.format(record)
        msg = record.getMessage()
        notification_logging(self.args, msg, record.levelname, record.name, self.tag)


def init_remote_log(args, tag=None):
    logger = logging.getLogger('remote')
    logger.setLevel(logging.DEBUG)
    remote_handler = RemoteHandler(args, tag)
    remote_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(process)d %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
    ))
    logger.addHandler(remote_handler)

    return logger
