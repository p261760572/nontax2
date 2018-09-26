# coding:utf-8
import argparse
import os
import posixpath
import re
from time import sleep

import cx_Oracle
import datetime
from sqlalchemy import func, tuple_
from xlrd import open_workbook

import config
import db
import utils
from models import SsImport, SsFileDesc

# os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.ZHS16GBK"
os.environ["NLS_DATE_FORMAT"] = "YYYY/MM/DD HH24:MI:SS"


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


def decode_bytes(data):
    if isinstance(data, bytes):
        data = bytearray(data)

    try:
        s = data.decode("gbk")
    except UnicodeDecodeError as e:
        # for i in range(e.start, e.end):
        data[e.start] = 0x20
        return decode_bytes(data)

    return s


def import_up_fixed(session, filename, sql, params, file_desc):
    # 查找列配置
    def get_column(column_name):
        for c in file_desc:
            if c.column_name == column_name:
                return c

        return None

    connection = session.connection()
    dbapi_conn = connection.connection.connection

    # 查找交易类型列
    c_transtype = get_column("transtype")

    # 查找清算标志列
    c_cupsflag = get_column("cupsflag")

    # 查找商户名列
    c_mchtname = get_column("mchtname")

    columns_part1 = [(c.column_name, c.position, c.position + c.column_length) for c in file_desc if
                     c.column_name is not None and c.position < c_mchtname.position]

    columns_part2 = [(c.column_name, c.position - c_mchtname.position - c_mchtname.column_length,
                      c.position + c.column_length - c_mchtname.position - c_mchtname.column_length) for c in file_desc
                     if c.column_name is not None and c.position > c_mchtname.position]

    with open(filename, "rb") as f:
        # first_line = f.readline()  # 跳过
        while True:
            lines = f.readlines(16 * 1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                # 跳过查询交易
                if line[c_transtype.position:c_transtype.position + c_transtype.column_length] == b'S00':
                    continue

                if line[c_cupsflag.position:c_cupsflag.position + c_cupsflag.column_length] == b'0':
                    continue

                # 商户名称特殊处理
                line_part1 = line[0:c_mchtname.position]
                mchtname = line[c_mchtname.position:c_mchtname.position + c_mchtname.column_length]
                line_part2 = line[c_mchtname.position + c_mchtname.column_length:]

                line_part1 = decode_bytes(line_part1)
                mchtname = decode_bytes(mchtname)
                line_part2 = decode_bytes(line_part2)

                parameter = [line_part1[start_pos:end_pos].rstrip() for column_name, start_pos, end_pos in
                             columns_part1]
                parameter.append(mchtname.rstrip())
                parameter.extend(
                    [line_part2[start_pos:end_pos].rstrip() for column_name, start_pos, end_pos in columns_part2])

                parameters.append(parameter)

            cur = dbapi_conn.cursor()
            cur.executemany(sql, parameters)
            dbapi_conn.commit()
            cur.close()
            # session.execute(sql, parameters)
            # session.commit()
            logger.info("commit %d条记录" % (len(parameters)))


def import_cp_split(session, filename, sql, params, file_desc):
    params = params or ','
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser()
    parser.add_argument('sep')
    parser.add_argument('-b', '--skip-bof', type=int, default=0)
    parser.add_argument('-e', '--skip-eof', type=int, default=0)
    parser.add_argument('-m', '--maxsplit', type=int, default=-1)
    args = parser.parse_args(params.split())

    sep = args.sep
    skip_bof = args.skip_bof
    skip_eof = args.skip_eof
    maxsplit = args.maxsplit

    # sep = '|'
    # maxsplit = -1

    format_check = False
    columns = [(c.column_name, c.position) for c in file_desc if c.column_name is not None]

    cache_lines = []
    with open(filename, 'rb') as f:
        # first_line = f.readline()  # 跳过
        line = True
        while line and skip_bof > 0:
            line = f.readline()  # 跳过
            skip_bof -= 1

        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break

            cache_lines.extend(lines)

            if skip_bof > 0:
                if len(cache_lines) >= skip_bof:
                    skip_bof = 0
                    cache_lines = cache_lines[skip_bof:]
                continue

            # lines = cache_lines
            if skip_eof > 0:
                if len(cache_lines) <= skip_eof:
                    continue
                lines = cache_lines[:-skip_eof]
                cache_lines = cache_lines[-skip_eof:]
            else:
                lines = cache_lines
                cache_lines = []

            # ChinaPay核心平台_商户结算对账接口文档V1.03
            if len(file_desc) > 0:
                file_group = file_desc[0].file_group
                if file_group == "CP20100401" and len(lines) > 0:
                    line = lines[0]
                    line = decode_bytes(line)
                    cols = line.split(sep, maxsplit)
                    if len(cols) == 13:
                        sql = "insert into fs_on_trans_log (version, trans_time, mchtid, ordid, trans_type, amount, trans_st, mchtdate, gateid, curyid, cpdate, seqid, priv, chkvalue) values ('V1.03', :trans_time, :mchtid, :ordid, :trans_type, :amount, :trans_st, :mchtdate, :gateid, :curyid, :cpdate, :seqid, :priv, :chkvalue)"
                        columns = [('trans_time', 0), ('mchtid', 1), ('ordid', 2), ('trans_type', 3), ('amount', 4),
                                   ('trans_st', 5), ('mchtdate', 6), ('gateid', 7), ('curyid', 8), ('cpdate', 9),
                                   ('seqid', 10), ('priv', 11), ('chkvalue', 12)]

            # logger.info(json.dumps(columns, ensure_ascii=False))
            parameters = []
            for line in lines:
                if line == b'0\n' or line == b'TransAmt=0|RefundAmt=0\n':
                    continue

                line = decode_bytes(line)
                cols = [c.strip(' \t\r\n\'') for c in line.split(sep, maxsplit)]
                # logger.info(len(cols))
                try:
                    parameters.append({column_name: cols[position] for column_name, position in columns})
                except IndexError as e:
                    logger.error(line)
                    raise Exception('文件格式错误')

            if len(parameters) > 0:
                session.execute(sql, parameters)

        session.commit()


def import_split(session, filename, sql, params, file_desc):
    connection = session.connection()
    dbapi_conn = connection.connection

    params = params or ','
    parser = argparse.ArgumentParser()
    parser.add_argument('sep')
    parser.add_argument('-b', '--skip-bof', type=int, default=0)
    parser.add_argument('-m', '--maxsplit', type=int, default=-1)
    args = parser.parse_args(params.split())

    skip_bof = args.skip_bof
    sep = args.sep
    maxsplit = args.maxsplit

    # sep = '|'
    # maxsplit = -1

    format_check = False
    columns = [(c.column_name, c.position) for c in file_desc if c.column_name is not None]

    with open(filename, 'rb') as f:
        # first_line = f.readline()  # 跳过
        line = True
        while line and skip_bof > 0:
            line = f.readline()  # 跳过
            skip_bof -= 1

        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                line = decode_bytes(line)
                cols = [c.strip(' \t\r\n\'') for c in line.split(sep, maxsplit)]
                # logger.info(cols)
                parameters.append([cols[position] for column_name, position in columns])

            cur = dbapi_conn.cursor()
            cur.executemany(sql, parameters)
            dbapi_conn.commit()
            cur.close()
            # session.execute(sql, parameters)
            # session.commit()


def import_cpdz_split(session, filename, sql, params, file_desc, fscode, bankcode):
    params = params or ','
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser()
    parser.add_argument('sep')
    parser.add_argument('-b', '--skip-bof', type=int, default=0)
    parser.add_argument('-e', '--skip-eof', type=int, default=0)
    parser.add_argument('-m', '--maxsplit', type=int, default=-1)
    args = parser.parse_args(params.split())

    sep = args.sep
    skip_bof = args.skip_bof
    skip_eof = args.skip_eof
    maxsplit = args.maxsplit

    # sep = '|'
    # maxsplit = -1

    format_check = False
    columns = [(c.column_name, c.position) for c in file_desc if c.column_name is not None]

    cache_lines = []
    with open(filename, 'rb') as f:
        # first_line = f.readline()  # 跳过
        line = True
        while line and skip_bof > 0:
            line = f.readline()  # 跳过
            skip_bof -= 1

        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break

            cache_lines.extend(lines)

            if skip_bof > 0:
                if len(cache_lines) >= skip_bof:
                    skip_bof = 0
                    cache_lines = cache_lines[skip_bof:]
                continue

            # lines = cache_lines
            if skip_eof > 0:
                if len(cache_lines) <= skip_eof:
                    continue
                lines = cache_lines[:-skip_eof]
                cache_lines = cache_lines[-skip_eof:]
            else:
                lines = cache_lines
                cache_lines = []

            # logger.info(json.dumps(columns, ensure_ascii=False))
            parameters = []
            for line in lines:
                line = decode_bytes(line)
                cols = [c.strip(' \t\r\n\'') for c in line.split(sep, maxsplit)]
                # logger.info(len(cols))
                try:
                    item = {column_name: cols[position] if position >= 0 else None for column_name, position in columns}
                    item['fscode'] = fscode
                    item['bankcode'] = bankcode
                    parameters.append(item)
                except IndexError as e:
                    logger.error(line)
                    raise Exception('文件格式错误')

            if len(parameters) > 0:
                session.execute(sql, parameters)

        session.commit()


def import_excel(session, filename, sql, params, file_desc):
    params = params or ''
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--sheet', type=int, default=0)
    parser.add_argument('-c', '--skip-col', type=int, default=0)
    parser.add_argument('-b', '--skip-bof', type=int, default=0)
    parser.add_argument('-e', '--skip-eof', type=int, default=0)
    args = parser.parse_args(params.split())

    sheet_index = args.sheet
    start_row = 0 + args.skip_bof
    start_col = 0 + args.skip_col
    tail_len = 0 + args.skip_eof

    columns = [(c.column_name, c.position) for c in file_desc if c.column_name is not None]

    # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    wb = open_workbook(filename)
    # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    ws = wb.sheet_by_index(sheet_index)

    max_row = ws.nrows
    max_column = ws.ncols

    for i in range(start_row, max_row - tail_len):
        parameters = []
        # 1000次做提交
        parameters.append({column_name: ws.cell(i, position).value for column_name, position in columns})

        # logger.info(parameters)
        session.execute(sql, parameters)
        session.commit()


def import_post(session, settle_date):
    logger.info(settle_date)

    def download(deadline):
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


def process_import(settle_date, file_group):
    session = None
    try:
        # now_datetime = datetime.now()
        # settle_datetime = datetime.strptime(settle_date, "%Y%m%d")

        session = db.get_session()
        subquery = session.query(SsImport.file_group, func.max(SsImport.start_date)).filter_by(
            file_group=file_group).filter(SsImport.start_date <= settle_date).group_by(SsImport.file_group)
        query = session.query(SsImport).filter(
            tuple_(SsImport.file_group, SsImport.start_date).in_(subquery)).filter(SsImport.status == "1")
        ss_import_list = query.all()

        for ss_import in ss_import_list:
            # real_date = utils.settle_date_delta(settle_date, ss_import.delta_days)

            # 清空表
            sql = "truncate table %s" % ss_import.import_table_name
            logger.info(sql)
            session.execute(sql)

            if ss_import.process_mode == 'simple':
                for file in ss_import.file_list:
                    if file.status != "1":
                        continue

                    local_filename = translate_local_path(ss_import.local_path, file.local_filename, settle_date)
                    logger.info(local_filename)
                    remote_logger.info(local_filename)

                    # 放弃非必需文件
                    if file.required != "1" and os.path.isfile(local_filename) == False:
                        logger.warning("文件不存在" + local_filename)
                        continue

                    if os.path.getsize(local_filename) == 0:
                        logger.warning("文件大小为0," + local_filename)
                        continue

                    file_desc = session.query(SsFileDesc).filter(SsFileDesc.file_group == file.import_format).order_by(
                        SsFileDesc.position).all()

                    columns = ",".join([c.column_name for c in file_desc if c.column_name is not None])
                    values = ",".join([":" + c.column_name for c in file_desc if c.column_name is not None])
                    sql = "insert into %s (%s) values (%s)" % (ss_import.import_table_name, columns, values)
                    logger.info(sql)

                    if file.import_type == "up_fixed":
                        import_up_fixed(session, local_filename, sql, file.import_params, file_desc)
                    # elif file.import_type == "cp_split":
                    #     import_cp_split(session, local_filename, sql, file.import_params, file_desc)
                    elif file.import_type == "excel":
                        import_excel(session, local_filename, sql, file.import_params, file_desc)
                    elif file.import_type == "split":
                        import_split(session, local_filename, sql, file.import_params, file_desc)
                    else:
                        raise ValueError('process_mode:%s, import_type:%s' % (ss_import.process_mode, file.import_type))
            elif ss_import.process_mode == 'cp':
                local_path = translate_local_path(ss_import.local_path, "", settle_date)
                logger.info(local_path)
                for file in ss_import.file_list:
                    file_desc = session.query(SsFileDesc).filter(SsFileDesc.file_group == file.import_format).order_by(
                        SsFileDesc.position).all()

                    columns = ",".join([c.column_name for c in file_desc if c.column_name is not None])
                    values = ",".join([":" + c.column_name for c in file_desc if c.column_name is not None])
                    sql = "insert into %s (%s) values (%s)" % (ss_import.import_table_name, columns, values)
                    logger.info(sql)

                    if file.import_type == 'cp_split':
                        names = os.listdir(local_path)
                        for name in names:
                            if re.match(file.pattern, name):
                                local_filename = os.path.join(local_path, name)
                                logger.info(local_filename)
                                remote_logger.info(local_filename)
                                import_cp_split(session, local_filename, sql, file.import_params, file_desc)
                    elif file.import_type == 'cpdz_split':
                        names = os.listdir(local_path)
                        for name in names:
                            if re.match(file.pattern, name):
                                local_filename = os.path.join(local_path, name)
                                logger.info(local_filename)
                                remote_logger.info(local_filename)
                                import_cpdz_split(session, local_filename, sql, file.import_params,
                                                  file_desc, name[0:8], '08' + name[18:26])
                    else:
                        raise ValueError('process_mode:%s, import_type:%s' % (ss_import.process_mode, file.import_type))
            elif ss_import.process_mode == 'post':
                import_post(session, settle_date)
            else:
                raise ValueError("process_mode:%s" % (ss_import.process_mode))

            logger.info("import_backup %s %s" % (settle_date, ss_import.import_table_name))

            connection = session.connection()
            dbapi_conn = connection.connection
            cur = None
            try:
                cur = dbapi_conn.cursor()
                cur.callproc("import_backup", [settle_date, ss_import.import_table_name.lower()])
            finally:
                if cur is not None:
                    cur.close()
    finally:
        if session is not None:
            session.close()


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
    parser.add_argument("-n", "--batch-no")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    logger.info("开始导入文件")
    remote_logger.info("开始导入文件")

    if not utils.notification_scheduled(args):
        logger.info("调用通知失败")
        remote_logger.error("调用通知失败")
        return False

    try:
        settle_date = args.date
        for file_group in args.file_groups.split(","):
            real_date = settle_date
            if file_group in ("ZJHF2"):
                real_date = next_workday(settle_date)
            logger.info("导入文件组%s" % (file_group))
            remote_logger.info("导入文件组%s" % (file_group))
            process_import(real_date, file_group)
            logger.info("导入文件组%s结束" % (file_group))
            remote_logger.info("导入文件组%s结束" % (file_group))

        session = None
        try:
            session = db.get_session()
            connection = session.connection()
            dbapi_conn = connection.connection
            cur = None
            try:
                cur = dbapi_conn.cursor()
                cur.callproc("import_end", [settle_date, args.file_groups])
            finally:
                if cur is not None:
                    cur.close()
        finally:
            if session is not None:
                session.close()
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.error("导入文件处理失败")
    else:
        utils.notification_executed(args, 0, "导入完成")
        remote_logger.info("导入文件完成")


if __name__ == "__main__":
    # 初始化日志
    logger = utils.init_log()
    remote_logger = None

    logger.info("start")

    main()
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # process_import("20171201", "UP")
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # process_import("20171201", "UP99")
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # process_import("20171201", "UPDZ")
    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("end")
