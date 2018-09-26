# coding=utf-8
import os
import re
import smtplib
import zipfile
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import groupby

import cx_Oracle
import logging

import config
import utils


def send_mail(receivers, subject, filename=None):
    message = MIMEMultipart()
    message['From'] = Header(config.SMTP_USER, 'utf-8')
    message['To'] = Header(",".join(receivers), 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')

    # 正文
    message.attach(MIMEText(subject, 'plain', 'utf-8'))

    # 附件
    if filename:
        att1 = MIMEText(open(filename, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # att1["Content-Disposition"] = 'attachment; filename="%s"' % (os.path.basename(filename).encode("gbk"))
        att1.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', os.path.basename(filename)))
        message.attach(att1)

    server = smtplib.SMTP(config.SMTP_HOST, 25)
    server.login(config.SMTP_USER, config.SMTP_PASSWD)
    server.sendmail(config.SMTP_FROM_ADDR, receivers, message.as_string())
    server.quit()


def main(settle_date):
    conn = None
    cur = None
    try:
        conn = cx_Oracle.Connection(config.DB_USER, config.DB_PASSWD, config.DB)
        cur = conn.cursor()
        cur.execute(
            "select a.receiver, b.attachment_filename, b.crontab from new_send_mail a, new_mail_file b where a.attachment = b.attachment  and b.status='1' order by a.receiver")
        cur.rowfactory = utils.makedict(cur)
        rows = cur.fetchall()
        temp = [(k, list(g)) for k, g in groupby(rows, key=lambda x: x["receiver"])]

        for receiver, attachment_files in temp:
            logging.info(receiver)

            zipname = os.path.join(config.REPORT_ROOT, "%s.zip" % settle_date)

            # 删除
            try:
                os.remove(zipname)
            except FileNotFoundError as e:
                pass

            has_attachment = False
            zf = zipfile.ZipFile(zipname, 'x', zipfile.ZIP_DEFLATED)
            for attachment in attachment_files:
                filename = attachment["attachment_filename"]
                crontab = attachment["crontab"]

                # 检测
                if utils.crontab_match(settle_date, crontab):
                    filename = re.sub('\{\w+\}', utils.replace_path(settle_date), filename)
                    filename = filename.replace("/", os.sep)
                    filename = os.path.join(config.REPORT_ROOT, filename)
                    zf.write(filename, os.path.basename(filename))
                    has_attachment = True
            zf.close()

            if has_attachment:
                send_mail([receiver], "%s报表" % settle_date, zipname)

            # 删除
            try:
                os.remove(zipname)
            except FileNotFoundError as e:
                pass

    finally:
        if cur is not None:
            cur.close()

        if conn is not None:
            conn.close()
