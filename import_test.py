# coding:utf-8
import os

# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
import db

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


def import_fs_file(session, settle_date, fscode, bankcode, filename):
    def import_lines(lines):
        parameters = []
        for line in lines:
            line = decode_bytes(line)
            cols = [c.strip(' \t\r\n\'') for c in line.split("|")]

            parameter = [settle_date, fscode, bankcode]
            parameter.extend(cols)
            parameters.append(parameter)

        cur = dbapi_conn.cursor()
        cur.executemany(
            "insert into fs_off (settledate, fscode, bankcode, mchtid, mchtname, cardno, transtime, termssn, termid, refnbr, amount) values (:settledate, :fscode, :bankcode, :mchtid, :mchtname, :cardno, :transtime, :termssn, :termid, :refnbr, :amount)",
            parameters)
        dbapi_conn.commit()
        cur.close()

    connection = session.connection()
    dbapi_conn = connection.connection

    with open(filename, 'rb') as f:
        # 跳过"#"开头的行
        line = True
        while line:
            line = f.readline()  # 跳过
            if line and line[0:1] != b'#':
                break

        if line:
            import_lines([line])

        while True:
            lines = f.readlines(1024 * 1024)
            if len(lines) == 0:
                break
            import_lines(lines)


def import_fs(session, settle_date):
    local_path = '/home/fs_run/data/fsoff'
    dirnames = [name for name in os.listdir(local_path)
                if os.path.isdir(os.path.join(local_path, name))]

    for fscode in dirnames:
        path = os.path.join(local_path, fscode, settle_date)
        if os.path.isdir(path):
            names = [name for name in os.listdir(path)
                     if os.path.isfile(os.path.join(path, name))]
            for name in names:
                bankcode = os.path.splitext(name)[0]
                if len(bankcode) == 8:
                    import_fs_file(session, settle_date, fscode, bankcode, os.path.join(path, name))


if __name__ == '__main__':
    session = db.get_session()
    try:
        # import_fs(session, "20171201")
        import_fs(session, "20171202")
        import_fs(session, "20171203")
        import_fs(session, "20171204")
    finally:
        session.close()
