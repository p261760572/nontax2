# coding:utf-8
import argparse
import logging
import os
from datetime import datetime

import db
import utils
from models import SsFile

# os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.ZHS16GBK"
os.environ["NLS_DATE_FORMAT"] = "YYYY/MM/DD HH24:MI:SS"


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
    params = params or ''
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser()
    parser.add_argument("sep", default=',')
    parser.add_argument("-b", "--skip-bof", type=int, default=0)
    parser.add_argument("-e", "--skip-eof", type=int, default=0)
    parser.add_argument("-m", "--maxsplit", type=int, default=-1)
    args = parser.parse_args(params.split())

    skip_bof = args.skip_bof
    skip_eof = args.skip_eof
    sep = args.sep
    maxsplit = args.maxsplit

    format_check = False
    columns_index = [c.get("position") for c in file_desc if c.get("column_name") is not None]

    with open(filename, "rb") as f:
        # first_line = f.readline()  # 跳过
        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                line = decode_bytes(line)
                cols = [c.strip(" \t\r\n\"") for c in line.split(sep, maxsplit)]
                parameters.append([cols[i] for i in columns_index])
            session.execute(sql, parameters)

        session.commit()


def import_cp_split(session, filename, sql, params, file_desc):
    params = params or ''
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser()
    parser.add_argument("sep", default='|')
    parser.add_argument("-b", "--skip-bof", type=int, default=0)
    parser.add_argument("-e", "--skip-eof", type=int, default=0)
    parser.add_argument("-m", "--maxsplit", type=int, default=-1)
    args = parser.parse_args(params.split())

    skip_bof = args.skip_bof
    skip_eof = args.skip_eof
    sep = args.sep
    maxsplit = args.maxsplit

    format_check = False
    columns_index = [c.get("position") for c in file_desc if c.get("column_name") is not None]

    with open(filename, "rb") as f:
        first_line = f.readline()  # 跳过
        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                line = decode_bytes(line)
                cols = [c.strip() for c in line.split(sep, maxsplit)]
                parameters.append([cols[i] for i in columns_index])
            session.execute(sql, parameters)

        session.commit()


def import_fixed(session, filename, sql, params, file_desc):
    # connection = session.connection()
    # dbapi_conn = connection.connection
    # cur = dbapi_conn.cursor()
    # # cur.callproc("test", [None])
    # cur.close()


    params = params or ''
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--skip-bof", type=int, default=0)
    parser.add_argument("-e", "--skip-eof", type=int, default=0)
    args = parser.parse_args(params.split())

    skip_bof = args.skip_bof
    skip_eof = args.skip_eof

    # columns = [list(map(int, c.split(":"))) for c in columns.split(",")]
    columns = [(c.get("position"), c.get("position") + c.get("column_length"), c.get("column_name")) for c in file_desc
               if
               c.get("column_name") is not None]

    format_check = False
    max_position = max([c.get("position") + c.get("column_length") for c in file_desc])

    with open(filename, "rb") as f:
        # first_line = f.readline()  # 跳过
        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            parameters = []
            for line in lines:
                parameters.append({c[2]: decode_bytes(line[c[0]:c[1]]).rstrip() for c in columns})

            session.execute(sql, parameters)
            session.commit()


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

        for row in rows:
            process_mode = row['process_mode']
            local_path = row['local_path']
            import_type = row["import_type"]
            import_table_name = row["import_table_name"]
            import_params = row["import_params"]
            file_list = row["file_list"]
            file_desc = row["file_desc"]

            if not import_table_name:
                continue

            # 清空表
            sql = "truncate table %s" % import_table_name
            logging.info(sql)
            session.execute(sql)

            if process_mode == 'all':
                file_list = [{'import_filename': os.path.join(local_path, name), 'required': '1'} for name in
                             os.listdir(local_path)]

            for file in file_list:
                local_filename = file['import_filename']
                required = file['required']

                # 放弃非必需文件
                if required != "1" and os.path.isfile(local_filename) == False:
                    logging.warning("文件不存在" + local_filename)
                    continue

                logging.info(local_filename)

                columns = ",".join([c.get("column_name") for c in file_desc if c.get("column_name") is not None])
                values = ",".join(
                    [":" + c.get("column_name") for c in file_desc if c.get("column_name") is not None])
                sql = "insert into %s (%s) values (%s)" % (import_table_name, columns, values)
                print(sql)
                logging.info(sql)
                if import_type == 'split':
                    import_split(session, local_filename, sql, import_params, file_desc)
                elif import_type == 'fixed':
                    import_fixed(session, local_filename, sql, import_params, file_desc)
                elif import_type == 'cp_split':
                    import_cp_split(session, local_filename, sql, import_params, file_desc)
                else:
                    raise ValueError("错误的导入类型 %s" % import_type)

                logging.info("new_settle_backup %s %s" % (settle_date, import_table_name))
                # cur.callproc("new_settle_backup", [settle_date, import_table_name.lower()])

    except Exception as e:
        utils.notification_executed(args, -1, str(e))
        raise
    else:
        utils.notification_executed(args, 0, '导入完成')
    finally:
        if session is not None:
            session.close()


if __name__ == '__main__':
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
