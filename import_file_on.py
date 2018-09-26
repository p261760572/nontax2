# coding:utf-8
import argparse
import os
import re
from datetime import datetime

from sqlalchemy import func, tuple_
from xlrd import open_workbook

import db
import utils
from models import SsImport, SsFileDesc

# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
os.environ['NLS_DATE_FORMAT'] = 'YYYY/MM/DD HH24:MI:SS'


def decode_bytes(data):
    if isinstance(data, bytes):
        data = bytearray(data)

    try:
        s = data.decode('gbk')
    except UnicodeDecodeError as e:
        # for i in range(e.start, e.end):
        data[e.start] = 0x20
        return decode_bytes(data)

    return s


def import_split(session, filename, sql, params, file_desc):
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
    columns = [(c.get('position'), c.get('column_name')) for c in file_desc if c.get('column_name') is not None]

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
                logger.info(cols)
                parameters.append({c[1]: cols[c[0]] for c in columns})
            session.execute(sql, parameters)

        session.commit()


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
    columns = [(c.get('position'), c.get('column_name')) for c in file_desc if c.get('column_name') is not None]

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
                file_group = file_desc[0].get("file_group")
                if file_group == "CP20100401" and len(lines) > 0:
                    line = lines[0]
                    line = decode_bytes(line)
                    cols = line.split(sep, maxsplit)
                    if len(cols) == 13:
                        sql = "insert into fs_on_trans_log (version, trans_time, mchtid, ordid, trans_type, amount, trans_st, mchtdate, gateid, curyid, cpdate, seqid, priv, chkvalue) values ('V1.03', :trans_time, :mchtid, :ordid, :trans_type, :amount, :trans_st, :mchtdate, :gateid, :curyid, :cpdate, :seqid, :priv, :chkvalue)"
                        columns = [(0, 'trans_time'), (1, 'mchtid'), (2, 'ordid'), (3, 'trans_type'), (4, 'amount'),
                                   (5, 'trans_st'), (6, 'mchtdate'), (7, 'gateid'), (8, 'curyid'), (9, 'cpdate'),
                                   (10, 'seqid'), (11, 'priv'), (12, 'chkvalue')]

            # logger.info(json.dumps(columns, ensure_ascii=False))
            parameters = []
            for line in lines:
                if line == b'0\n' or line == b'TransAmt=0|RefundAmt=0\n':
                    continue

                line = decode_bytes(line)
                cols = [c.strip(' \t\r\n\'') for c in line.split(sep, maxsplit)]
                # logger.info(len(cols))
                try:
                    parameters.append({c[1]: cols[c[0]] for c in columns})
                except IndexError as e:
                    logger.error(line)
                    raise Exception('文件格式错误')

            if len(parameters) > 0:
                session.execute(sql, parameters)

        session.commit()

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
    columns = [(c.get('position'), c.get('column_name')) for c in file_desc if c.get('column_name') is not None]

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
                    item = {c[1]: cols[c[0]] if c[0] >= 0 else None for c in columns}
                    item['fscode'] = fscode
                    item['bankcode'] = bankcode
                    parameters.append(item)
                except IndexError as e:
                    logger.error(line)
                    raise Exception('文件格式错误')

            if len(parameters) > 0:
                session.execute(sql, parameters)

        session.commit()



def import_up_fixed(session, filename, sql, params, file_desc):
    # connection = session.connection()
    # dbapi_conn = connection.connection
    # cur = dbapi_conn.cursor()
    # # cur.callproc('test', [None])
    # cur.close()


    # params = params or ''
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-b', '--skip-bof', type=int, default=0)
    # parser.add_argument('-e', '--skip-eof', type=int, default=0)
    # args = parser.parse_args(params.split())
    #
    # skip_bof = args.skip_bof
    # skip_eof = args.skip_eof

    # columns = [list(map(int, c.split(':'))) for c in columns.split(',')]
    columns = [(c.get('position'), c.get('position') + c.get('column_length'), c.get('column_name')) for c in file_desc
               if
               c.get('column_name') is not None]

    format_check = False
    max_position = max([c.get('position') + c.get('column_length') for c in file_desc])

    with open(filename, 'rb') as f:
        # first_line = f.readline()  # 跳过
        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                parameters.append({c[2]: decode_bytes(line[c[0]:c[1]]).rstrip() for c in columns})
                # parameters.append({c[2]: line[c[0]:c[1]].rstrip() for c in columns})

            session.execute(sql, parameters)
            session.commit()


def import_fixed(session, filename, sql, params, file_desc):
    # connection = session.connection()
    # dbapi_conn = connection.connection
    # cur = dbapi_conn.cursor()
    # # cur.callproc('test', [None])
    # cur.close()


    # params = params or ''
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-b', '--skip-bof', type=int, default=0)
    # parser.add_argument('-e', '--skip-eof', type=int, default=0)
    # args = parser.parse_args(params.split())
    #
    # skip_bof = args.skip_bof
    # skip_eof = args.skip_eof

    # columns = [list(map(int, c.split(':'))) for c in columns.split(',')]
    columns = [(c.get('position'), c.get('position') + c.get('column_length'), c.get('column_name')) for c in
               file_desc
               if
               c.get('column_name') is not None]

    format_check = False
    max_position = max([c.get('position') + c.get('column_length') for c in file_desc])

    with open(filename, 'rb') as f:
        # first_line = f.readline()  # 跳过
        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                parameters.append({c[2]: decode_bytes(line[c[0]:c[1]]).rstrip() for c in columns})
                # parameters.append({c[2]: line[c[0]:c[1]].rstrip() for c in columns})

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

    columns = [(c.get('position'), c.get('column_name')) for c in file_desc if c.get('column_name') is not None]

    # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    wb = open_workbook(filename)
    # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    ws = wb.sheet_by_index(sheet_index)

    max_row = ws.nrows
    max_column = ws.ncols


    for i in range(start_row, max_row - tail_len):
        parameters = []
        # 1000次做提交
        parameters.append({c[1]: ws.cell(i, c[0]).value for c in columns})

        # logger.info(parameters)
        session.execute(sql, parameters)
        session.commit()


def process_import(settle_date):
    session = None
    try:
        now_datetime = datetime.now()
        settle_datetime = datetime.strptime(settle_date, '%Y%m%d')

        session = db.get_session()
        subquery = session.query(SsImport.file_group, func.max(SsImport.start_date)).filter(
            SsImport.start_date <= settle_date).group_by(SsImport.file_group)
        query = session.query(SsImport).filter(tuple_(SsImport.file_group, SsImport.start_date).in_(subquery)).filter(
            SsImport.status == '1')
        rows = query.all()
        # 转dict
        rows = [row.to_dict() for row in rows]

        # 转换路径
        for row in rows:
            real_date = utils.settle_date_delta(settle_date, row['delta_days'])
            utils.translate(real_date, row)

            process_mode = row['process_mode']
            local_path = row['local_path']
            import_table_name = row['import_table_name']
            file_list = row['file_list']

            if not import_table_name:
                continue

            # import_type = row['import_type']
            # import_params = row['import_params']

            # 过滤
            file_list = [file for file in file_list if file.get('status') == '1']

            # 清空表
            sql = 'truncate table %s' % import_table_name
            logger.info(sql)
            session.execute(sql)

            # if process_mode == 'cp':
            #     file_list = [{'import_filename': os.path.join(local_path, name), 'required': '1'} for name in
            #                  os.listdir(local_path)]

            if process_mode == 'simple':
                for file in file_list:
                    local_filename = file['local_filename']
                    import_type = file['import_type']
                    import_format = file['import_format']
                    import_params = file['import_params']
                    required = file['required']
                    file_desc = session.query(SsFileDesc).filter(SsFileDesc.file_group == import_format).order_by(
                        SsFileDesc.position).all()

                    file_desc = [row.to_dict() for row in file_desc]

                    # 放弃非必需文件
                    if required != '1' and os.path.isfile(local_filename) == False:
                        logger.warning('文件不存在' + local_filename)
                        continue

                    logger.info(local_filename)
                    remote_logger.info(local_filename)

                    columns = ','.join([c.get('column_name') for c in file_desc if c.get('column_name') is not None])
                    values = ','.join(
                        [':' + c.get('column_name') for c in file_desc if c.get('column_name') is not None])
                    sql = 'insert into %s (%s) values (%s)' % (import_table_name, columns, values)
                    logger.info(sql)

                    if import_type == 'split':
                        import_split(session, local_filename, sql, import_params, file_desc)
                    elif import_type == 'fixed':
                        import_fixed(session, local_filename, sql, import_params, file_desc)
                    elif import_type == 'up_fixed':
                        import_up_fixed(session, local_filename, sql, import_params, file_desc)
                    elif import_type == 'cp_split':
                        import_cp_split(session, local_filename, sql, import_params, file_desc)
                    elif import_type == 'excel':
                        import_excel(session, local_filename, sql, import_params, file_desc)
                    else:
                        raise ValueError('错误的导入类型 %s' % import_type)

                    logger.info('import_backup %s %s' % (settle_date, import_table_name))

                    connection = session.connection()
                    dbapi_conn = connection.connection
                    cur = None
                    try:
                        cur = dbapi_conn.cursor()
                        cur.callproc('import_backup', [settle_date, import_table_name.lower()])
                    finally:
                        if cur is not None:
                            cur.close()
            elif process_mode == 'cp':
                for file in file_list:
                    pattern = file['pattern']
                    import_type = file['import_type']
                    import_format = file['import_format']
                    import_params = file['import_params']
                    required = file['required']
                    file_desc = session.query(SsFileDesc).filter(SsFileDesc.file_group == import_format).order_by(
                        SsFileDesc.position).all()

                    file_desc = [row.to_dict() for row in file_desc]

                    columns = ','.join([c.get('column_name') for c in file_desc if c.get('column_name') is not None])
                    values = ','.join(
                        [':' + c.get('column_name') for c in file_desc if c.get('column_name') is not None])
                    sql = 'insert into %s (%s) values (%s)' % (import_table_name, columns, values)
                    logger.info(sql)

                    if import_type == 'cp_split':
                        names = os.listdir(local_path)
                        for name in names:
                            if re.match(pattern, name):
                                logger.info(os.path.join(local_path, name))
                                remote_logger.info(os.path.join(local_path, name))
                                import_cp_split(session, os.path.join(local_path, name), sql, import_params, file_desc)
                    elif import_type == 'cpdz_split':
                        names = os.listdir(local_path)
                        for name in names:
                            if re.match(pattern, name):
                                logger.info(os.path.join(local_path, name))
                                remote_logger.info(os.path.join(local_path, name))
                                import_cpdz_split(session, os.path.join(local_path, name), sql, import_params, file_desc, name[0:8], '08' + name[18:26])
                    else:
                        raise ValueError('import_type:%s' % (import_type))

                    logger.info('import_backup %s %s' % (settle_date, import_table_name))
                    connection = session.connection()
                    dbapi_conn = connection.connection
                    cur = None
                    try:
                        cur = dbapi_conn.cursor()
                        cur.callproc('import_backup', [settle_date, import_table_name.lower()])
                    finally:
                        if cur is not None:
                            cur.close()
            else:
                raise ValueError('process_mode:%s' % (process_mode))
    finally:
        if session is not None:
            session.close()


def main():
    global remote_logger
    parser = argparse.ArgumentParser()
    parser.add_argument('host', default='http://127.0.0.1:8080')
    parser.add_argument('entry_id')
    parser.add_argument('base_dir')
    parser.add_argument('date')
    parser.add_argument('-n', '--batch-no')
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    remote_logger.info('开始导入文件')

    if not utils.notification_scheduled(args):
        remote_logger.error('调用通知失败')
        return False

    try:
        settle_date = args.date
        process_import(settle_date)
    except Exception as e:
        logger.error(str(e), exc_info=1)
        utils.notification_executed(args, -1, str(e))
        remote_logger.error('导入文件处理失败')
    else:
        utils.notification_executed(args, 0, '导入完成')
        remote_logger.info('导入文件完成')


if __name__ == '__main__':
    # 初始化日志
    logger = utils.init_log()
    remote_logger = None

    logger.info('start')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    main()
    # process_import('20170818')
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info('end')
