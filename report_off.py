# coding=utf-8
import argparse
import os
import re

import config
from app import db, utils
from app.models import SsUpload, SsUploadList, UploadStatus, UploadFileType, ProcessMode, JobMessage, JobTrigger


def format_amount(amount):
    if amount < 0:
        return "%12.11d" % (amount)
    return "%12.12d" % (amount)


def format_transtime(transtime):
    return "%4.4s-%2.2s-%2.2s %2.2s:%2.2s:%2.2s" % (
        transtime[0:4], transtime[4:6], transtime[6:8], transtime[8:10], transtime[10:12],
        transtime[12:14])


def format_mchtname(mchtname):
    return (b"%-40.40s" % (mchtname.encode("GBK"))).decode("GBK")


def txt_report(settle_date, cur, report_id, report_params, report_filename, head_sql, body_sql, tail_sql):
    report_filename = re.sub('\{\w+\}', utils.replace_path(settle_date), report_filename)

    report_filename = report_filename.replace("/", os.sep)
    report_filename = os.path.join(config.REPORT_ROOT, report_filename)

    pathname = os.path.dirname(report_filename)
    if not os.path.exists(pathname):
        os.makedirs(pathname, mode=0o755)

    with open(report_filename, "w") as fw:

        if head_sql:
            bind = utils.parse_sql_bind(head_sql)
            cur.execute(head_sql, [settle_date for b in bind])
            cur.rowfactory = None
            row = cur.fetchone()
            if row is not None:
                fw.write(report_params.join([utils.to_string(c) for c in row]))
                fw.write("\n")

        count = 0
        if body_sql:
            bind = utils.parse_sql_bind(body_sql)
            cur.execute(body_sql, [settle_date for b in bind])
            cur.rowfactory = None
            row = cur.fetchone()
            while row is not None:
                count += 1

                fw.write(report_params.join([utils.to_string(c) for c in row]))
                fw.write("\n")

                row = cur.fetchone()

        if tail_sql:
            bind = utils.parse_sql_bind(tail_sql)
            cur.execute(tail_sql, [settle_date for b in bind])
            cur.rowfactory = None
            row = cur.fetchone()
            if row is not None:
                fw.write(report_params.join([utils.to_string(c) for c in row]))
                fw.write("\n")


def process_off_report(session, settle_date, batch_no, ss_upload):
    trans_chnl = '1'

    if batch_no:
        query1 = session.execute(
            """select fscode, bankcode from fs_dz_result_his where trans_chnl = :trans_chnl and settledate = :settledate and mchtid in (select mchtid from fs_dz_zdsh_lb where batch_no = :batch_no) group by fscode, bankcode order by fscode, bankcode""",
            {
                'settledate': settle_date, 'trans_chnl': trans_chnl, 'batch_no': batch_no
            })
    else:
        query1 = session.execute(
            """select fscode, bankcode from fs_dz_result_his where trans_chnl = :trans_chnl and settledate = :settledate group by fscode, bankcode order by fscode, bankcode""",
            {
                'settledate': settle_date, 'trans_chnl': trans_chnl
            })

    for row1 in query1:
        if row1[0] is None:
            raise Exception("非税地区码为空")
        if row1[1] is None:
            raise Exception("银行代码为空")

        local_filename = os.path.join(row1[0], settle_date, row1[1][2:] + '.txt')
        filename = os.path.join(config.NONTAX_FILE_ROOT, local_filename)
        # 线上
        # filename = os.path.join(config.NONTAX_FILE_ROOT, row1[0], settle_date, row1[1] + '.wy.txt')
        pathname = os.path.dirname(filename)
        if not os.path.exists(pathname):
            os.makedirs(pathname, mode=0o755)

        with open(filename, "w") as fw:
            query2 = session.execute(
                """select mchtid, sum(amount), count(1) from fs_dz_result_his where settledate = :settledate and trans_chnl = :trans_chnl and fscode = :fscode and bankcode = :bankcode group by mchtid order by mchtid """,
                {
                    'settledate': settle_date, 'trans_chnl': trans_chnl,
                    'fscode': row1[0], 'bankcode': row1[1]
                })
            fw.write(
                "\r\n".join(["#%8.8s|%s|%d" % (settle_date, format_amount(row2[1]), row2[2]) for row2 in query2]))
            query3 = session.execute(
                """select mchtid, mchtname, cardno, transtime, termssn, termid, refnbr, amount from fs_dz_result_his where settledate = :settledate and trans_chnl = :trans_chnl and fscode = :fscode and bankcode = :bankcode order by mchtid """,
                {'settledate': settle_date, 'trans_chnl': trans_chnl, 'fscode': row1[0], 'bankcode': row1[1]})
            # for row3 in query3:
            #     print(list(row3))
            fw.write("\r\n")
            # fw.write("\n".join(
            #     ["%15.15s|%-40.40s|%-19.19s|%10.10s|%6.6s|%8.8s|%12.12s|%12.12s" % (row3[0],row3[1],row3[2],row3[3],row3[4],row3[5],row3[6],row3[7]) for row3 in query3]))
            fw.write("\r\n".join(["%15.15s|%s|%-19.19s|%10.10s|%6.6s|%8.8s|%12.12s|%12.12s" % (
            row3[0], format_mchtname(row3[1]), row3[2], row3[3], row3[4], row3[5], row3[6], format_amount(int(row3[7]))) for row3 in query3]))

            fw.write("\r\n")


        ss_upload_file = SsUploadList(ss_upload.batch_no, local_filename, UploadFileType.OFF)
        session.add(ss_upload_file)


def process_on_report(session, settle_date, batch_no, ss_upload):
    trans_chnl = '1'

    if batch_no:
        query1 = session.execute(
            """select fscode, bankcode from fs_dz_result_tmp where trans_chnl <> :trans_chnl and settledate = :settledate and mchtid in (select mchtid from fs_dz_zdsh_lb where batch_no = :batch_no) group by fscode, bankcode order by fscode, bankcode""",
            {
                'settledate': settle_date, 'trans_chnl': trans_chnl, 'batch_no': batch_no
            })
    else:
        query1 = session.execute(
            """select fscode, bankcode from fs_dz_result_tmp where trans_chnl <> :trans_chnl and settledate = :settledate group by fscode, bankcode order by fscode, bankcode""",
            {
                'settledate': settle_date, 'trans_chnl': trans_chnl
            })

    for row1 in query1:
        local_filename = os.path.join(row1[0], settle_date, row1[1][2:] + '.wy.txt')
        filename = os.path.join(config.NONTAX_FILE_ROOT, local_filename)
        # 线上
        # filename = os.path.join(config.NONTAX_FILE_ROOT, row1[0], settle_date, row1[1] + '.wy.txt')
        pathname = os.path.dirname(filename)
        if not os.path.exists(pathname):
            os.makedirs(pathname, mode=0o755)

        with open(filename, "w") as fw:
            query2 = session.execute(
                """select sum(amount), count(1) from fs_dz_result_tmp where settledate = :settledate and trans_chnl <> :trans_chnl and fscode = :fscode and bankcode = :bankcode""",
                {
                    'settledate': settle_date, 'trans_chnl': trans_chnl,
                    'fscode': row1[0], 'bankcode': row1[1]
                })
            fw.write("\n".join(
                ["#%8.8s|%s|%12.12d" % (settle_date, format_amount(row2[0]), row2[1]) for row2 in query2]))
            query3 = session.execute(
                """select mchtid, mchtname, refnbr, cardno, transtime, amount, trans_type, fs_ins_id from fs_dz_result_tmp where settledate = :settledate and trans_chnl <> :trans_chnl and fscode = :fscode and bankcode = :bankcode order by mchtid """,
                {
                    'settledate': settle_date, 'trans_chnl': trans_chnl,
                    'fscode': row1[0],
                    'bankcode': row1[1]
                })
            # for row3 in query3:
            #     print(list(row3))
            fw.write("\n")
            # fw.write("\n".join(
            #     ["%15.15s|%-40.40s|%-19.19s|%10.10s|%6.6s|%8.8s|%12.12s|%12.12s" % (row3[0],row3[1],row3[2],row3[3],row3[4],row3[5],row3[6],row3[7]) for row3 in query3]))
            fw.write("\n".join(
                ["%s|%s|%s|%s|%s|%s|%s|%s" % (
                    row3[0], row3[1], row3[2], row3[3], format_transtime(row3[4]), format_amount(int(row3[5])), row3[6],
                    row3[7])
                 for row3 in query3]))

        ss_upload_file = SsUploadList(ss_upload.batch_no, local_filename, UploadFileType.ON)
        session.add(ss_upload_file)


def process_gy_report(session, settle_date):
    trans_chnl0 = '0'
    trans_chnl3 = '3'

    local_filename = os.path.join(settle_date, 'a_online.wy.txt')
    filename = os.path.join(config.NONTAX_THIRD_ROOT, local_filename)
    # 线上
    # filename = os.path.join(config.NONTAX_FILE_ROOT, row1[0], settle_date, row1[1] + '.wy.txt')
    pathname = os.path.dirname(filename)
    if not os.path.exists(pathname):
        os.makedirs(pathname, mode=0o755)

    with open(filename, "w") as fw:
        query2 = session.execute(
            """select sum(amount), count(1) from (select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a where a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (a.mchtid, a.cardno) in (select mer_id, jrn_no from fs_gaoyang) union select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a, fs_mcht_info_tmp b where a.mchtid = b.s_mchtid and a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (b.mchtid, a.cardno) in (select mer_id, jrn_no from fs_gaoyang))""",
            {
                'settledate': settle_date, 'trans_chnl0': trans_chnl0, 'trans_chnl3': trans_chnl3
            })
        fw.write("\n".join(
            ["#%8.8s|%s|%12.12d" % (settle_date, format_amount(row2[0]), row2[1]) for row2 in query2]))
        query3 = session.execute(
            """select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a where a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (a.mchtid, a.cardno) in (select mer_id, jrn_no from fs_gaoyang) union select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a, fs_mcht_info_tmp b where a.mchtid = b.s_mchtid and a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (b.mchtid, a.cardno) in (select mer_id, jrn_no from fs_gaoyang)""",
            {
                'settledate': settle_date, 'trans_chnl0': trans_chnl0, 'trans_chnl3': trans_chnl3
            })
        # for row3 in query3:
        #     print(list(row3))
        fw.write("\n")
        # fw.write("\n".join(
        #     ["%15.15s|%-40.40s|%-19.19s|%10.10s|%6.6s|%8.8s|%12.12s|%12.12s" % (row3[0],row3[1],row3[2],row3[3],row3[4],row3[5],row3[6],row3[7]) for row3 in query3]))
        fw.write("\n".join(
            ["%s|%s|%s|%s|%s|%s|%s|%s" % (
                row3[0], row3[1], row3[2], row3[3], format_transtime(row3[4]), format_amount(int(row3[5])), row3[6],
                row3[7])
             for row3 in query3]))


def process_yj_report(session, settle_date):
    trans_chnl0 = '0'
    trans_chnl3 = '3'

    local_filename = os.path.join(settle_date, 'a_yuanjian_%s.wy.txt' % (settle_date))
    filename = os.path.join(config.NONTAX_THIRD_ROOT, local_filename)
    # 线上
    # filename = os.path.join(config.NONTAX_FILE_ROOT, row1[0], settle_date, row1[1] + '.wy.txt')
    pathname = os.path.dirname(filename)
    if not os.path.exists(pathname):
        os.makedirs(pathname, mode=0o755)

    with open(filename, "w") as fw:
        query2 = session.execute(
            """select nvl(sum(amount),0), count(1) from (select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a where a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (a.mchtid, a.cardno) in (select shh, dingdan from fs_yuanjian) union select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a, fs_mcht_info_tmp b where a.mchtid = b.s_mchtid and a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (b.mchtid, a.cardno) in (select shh, dingdan from fs_yuanjian))""",
            {
                'settledate': settle_date, 'trans_chnl0': trans_chnl0, 'trans_chnl3': trans_chnl3
            })
        fw.write("\n".join(
            ["#%8.8s|%s|%12.12d" % (settle_date, format_amount(row2[0]), row2[1]) for row2 in query2]))
        query3 = session.execute(
            """select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a where a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (a.mchtid, a.cardno) in (select shh, dingdan from fs_yuanjian) union select a.mchtid, a.mchtname, a.refnbr, a.cardno, a.transtime, a.amount, a.trans_type, a.fs_ins_id from fs_dz_result_tmp a, fs_mcht_info_tmp b where a.mchtid = b.s_mchtid and a.settledate = :settledate and a.trans_chnl in (:trans_chnl0, :trans_chnl3) and (b.mchtid, a.cardno) in (select shh, dingdan from fs_yuanjian)""",
            {
                'settledate': settle_date, 'trans_chnl0': trans_chnl0, 'trans_chnl3': trans_chnl3
            })
        # for row3 in query3:
        #     print(list(row3))
        fw.write("\n")
        # fw.write("\n".join(
        #     ["%15.15s|%-40.40s|%-19.19s|%10.10s|%6.6s|%8.8s|%12.12s|%12.12s" % (row3[0],row3[1],row3[2],row3[3],row3[4],row3[5],row3[6],row3[7]) for row3 in query3]))
        fw.write("\n".join(
            ["%s|%s|%s|%s|%s|%s|%s|%s" % (
                row3[0], row3[1], row3[2], row3[3], format_transtime(row3[4]), format_amount(int(row3[5])), row3[6],
                row3[7])
             for row3 in query3]))


def main():
    global logger, remote_logger
    logger = utils.init_log()
    logger.info('start')

    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("entry_id")
    parser.add_argument("base_dir")
    parser.add_argument("date")
    parser.add_argument("-n", "--batch-no")
    args = parser.parse_args()

    remote_logger = utils.init_remote_log(args)

    logger.info("开始生成对账文件")
    remote_logger.info("开始生成对账文件")

    if not utils.notification_scheduled(args):
        logger.info("调用通知失败")
        remote_logger.error("调用通知失败")
        return False

    settle_date = args.date
    batch_no = args.batch_no

    session = None
    try:
        # 非税对账文件
        session = db.get_session()
        ss_upload = SsUpload(settle_date, ProcessMode.OFF)
        session.add(ss_upload)
        session.flush()
        process_off_report(session, settle_date, batch_no, ss_upload)
        # process_on_report(session, settle_date, batch_no, ss_upload)
        session.commit()

        #if ss_upload:
        #     #自动触发
        #    ss_upload.status = UploadStatus.ACQUIRED
        #    job_message = JobMessage('UPLOAD', ' '.join([ss_upload.settle_date, '-n', ss_upload.batch_no]))
        #    session.add(job_message)
        #
        #    job_trigger = session.query(JobTrigger).filter_by(trigger_name='fs.upload').first()
        #    if job_trigger.trigger_state == 'COMPLETE':
        #        job_trigger.trigger_state = 'WAITING'
        #
        #    job_trigger.simple_trigger.repeat_count += 1
        #    session.commit()

        # if not batch_no:
        #     # 第三方对账文件
        #     process_gy_report(session, settle_date)
        #     process_yj_report(session, settle_date)
    except Exception as e:
        session.rollback()
        logger.error(str(e), exc_info=1)
        logger.error("生成对账文件失败")
        utils.notification_executed(args, -1, str(e))
        remote_logger.error(str(e))
        remote_logger.error("生成对账文件失败")
    else:
        logger.info("生成对账文件完成")
        utils.notification_executed(args, 0, '生成对账文件完成')
        remote_logger.info("生成对账文件完成")
    finally:
        if session is not None:
            session.close()

    logger.info('end')

if __name__ == '__main__':
    main()
