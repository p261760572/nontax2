# coding=utf-8
import enum
import json
from datetime import datetime
from uuid import UUID

from sqlalchemy import Column, Integer, String, ForeignKeyConstraint, DateTime, text, Enum, Sequence
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import relationship

Base = declarative_base()


class OutputMixin(object):
    RELATIONSHIPS_TO_DICT = False

    def __iter__(self):
        return self.to_dict().iteritems()

    def to_dict(self, rel=None, backref=None):
        if rel is None:
            rel = self.RELATIONSHIPS_TO_DICT
        res = {column.key: getattr(self, attr) for attr, column in self.__mapper__.c.items()}
        if rel:
            for attr, relation in self.__mapper__.relationships.items():
                # Avoid recursive loop between to tables.
                if backref == relation.table:
                    continue
                value = getattr(self, attr)
                if value is None:
                    res[relation.key] = None
                elif isinstance(value.__class__, DeclarativeMeta):
                    res[relation.key] = value.to_dict(backref=self.__table__)
                else:
                    res[relation.key] = [i.to_dict(backref=self.__table__) for i in value]
        return res

    def to_json(self, rel=None):
        def extended_encoder(x):
            if isinstance(x, datetime):
                return x.isoformat()
            if isinstance(x, UUID):
                return str(x)

        if rel is None:
            rel = self.RELATIONSHIPS_TO_DICT
        return json.dumps(self.to_dict(rel), default=extended_encoder)


# class SsFile(Base, OutputMixin):
#     __tablename__ = 'ss_file'
#     RELATIONSHIPS_TO_DICT = True
#
#     filename = Column(String(40), primary_key=True)
#     start_date = Column(String(8), primary_key=True)
#     process_mode = Column(String(20))
#     server_protocol = Column(String(200))
#     server_host = Column(String(200))
#     server_user = Column(String(200))
#     server_passwd = Column(String(200))
#     remote_path = Column(String(200))
#     local_path = Column(String(200))
#     extract_command = Column(String(200))
#     import_type = Column(String(200))
#     import_table_name = Column(String(200))
#     import_params = Column(String(4000))
#     delta_days = Column(Integer)
#     status = Column(String(200))
#
#     file_list = relationship('SsFileList')
#     file_desc = relationship('SsFileDesc', order_by='SsFileDesc.position')
#
#
# class SsFileList(Base, OutputMixin):
#     __tablename__ = 'ss_file_list'
#     __table_args__ = (
#         ForeignKeyConstraint(
#             ['filename', 'start_date'],
#             ['ss_file.filename', 'ss_file.start_date']
#         ),
#     )
#     filename = Column(String(40), primary_key=True)
#     start_date = Column(String(8), primary_key=True)
#     remote_filename = Column(String(200), primary_key=True)
#     local_filename = Column(String(200))
#     import_filename = Column(String(200))
#     required = Column(String(1))
#
#     file = relationship('SsFile', uselist=False, back_populates='file_list')
#
#



class SsDownload(Base, OutputMixin):
    __tablename__ = 'ss_download'
    RELATIONSHIPS_TO_DICT = True

    file_group = Column(String(40), primary_key=True)
    start_date = Column(String(8), primary_key=True)
    server_protocol = Column(String(200))
    server_host = Column(String(200))
    server_port = Column(Integer)
    server_user = Column(String(200))
    server_passwd = Column(String(200))
    remote_path = Column(String(200))
    local_path = Column(String(200))
    status = Column(String(1))
    process_mode = Column(String(20))
    delta_days = Column(Integer)

    file_list = relationship('SsDownloadList')


class SsDownloadList(Base, OutputMixin):
    __tablename__ = 'ss_download_list'
    __table_args__ = (
        ForeignKeyConstraint(
            ['file_group', 'start_date'],
            ['ss_download.file_group', 'ss_download.start_date']
        ),
    )
    file_group = Column(String(40), primary_key=True)
    start_date = Column(String(8), primary_key=True)
    remote_filename = Column(String(200), primary_key=True)
    local_filename = Column(String(200))
    required = Column(String(1))
    status = Column(String(1))

    ss_download = relationship('SsDownload', uselist=False, back_populates='file_list')


class SsExtract(Base, OutputMixin):
    __tablename__ = 'ss_extract'

    file_group = Column(String(40), primary_key=True)
    start_date = Column(String(8), primary_key=True)
    local_path = Column(String(200))
    extract_command = Column(String(200))
    status = Column(String(1))
    pattern = Column(String(20))


class SsImport(Base, OutputMixin):
    __tablename__ = 'ss_import'
    RELATIONSHIPS_TO_DICT = True

    file_group = Column(String(40), primary_key=True)
    start_date = Column(String(8), primary_key=True)
    local_path = Column(String(200))
    status = Column(String(1))
    process_mode = Column(String(20))
    import_table_name = Column(String(200))
    delta_days = Column(Integer)

    file_list = relationship('SsImportList')


class SsImportList(Base, OutputMixin):
    __tablename__ = 'ss_import_list'
    __table_args__ = (
        ForeignKeyConstraint(
            ['file_group', 'start_date'],
            ['ss_import.file_group', 'ss_import.start_date']
        ),
    )

    file_group = Column(String(40), primary_key=True)
    start_date = Column(String(8), primary_key=True)
    local_filename = Column(String(200), primary_key=True)
    required = Column(String(1))
    status = Column(String(1))
    import_type = Column(String(200))
    import_params = Column(String(4000))
    import_format = Column(String(40))
    pattern = Column(String(200))

    ss_import = relationship('SsImport', uselist=False, back_populates='file_list')


class SsFileDesc(Base, OutputMixin):
    __tablename__ = 'ss_file_desc'
    file_group = Column(String(40), primary_key=True)
    start_date = Column(String(8), primary_key=True)
    position = Column(Integer, primary_key=True)
    column_name = Column(String(30))
    column_length = Column(Integer)
    column_desc = Column(String(40))


class UploadStatus(enum.Enum):
    WAITING = "WAITING"
    ACQUIRED = "ACQUIRED"
    EXECUTING = "EXECUTING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class ProcessMode(enum.Enum):
    ON = "ON"
    OFF = "OFF"
    QR = "QR"

class SsUpload(Base, OutputMixin):
    __tablename__ = 'ss_upload'
    settle_date = Column(String(8))
    status = Column(Enum(UploadStatus), default=UploadStatus.WAITING)
    batch_no = Column(String(20), Sequence('seq_batch_no'))
    rec_id = Column(Integer, Sequence('seq_rec_id'), primary_key=True)
    oper_in = Column(String(1), default='0')
    proc_st = Column(String(1), default='0')
    created_by = Column(String(32), default='sys')
    created_time = Column(String(7), default=text('sysdate'))
    last_modified_by = Column(String(32), default='sys')
    last_modified_time = Column(DateTime, default=text('sysdate'))
    last_checked_by = Column(String(32))
    last_checked_time = Column(DateTime)
    checked_reason = Column(String(200))
    process_mode = Column(Enum(ProcessMode))

    def __init__(self, settle_date, process_mode):
        self.settle_date = settle_date
        self.process_mode = process_mode


class UploadFileStatus(enum.Enum):
    WAITING = "WAITING"
    ACQUIRED = "ACQUIRED"
    EXECUTING = "UPLOADING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class UploadFileType(enum.Enum):
    ON = "ON"
    OFF = "OFF"
    QR = "QR"


class SsUploadList(Base, OutputMixin):
    __tablename__ = 'ss_upload_list'
    local_filename = Column(String(200))
    status = Column(Enum(UploadFileStatus), default=UploadFileStatus.WAITING)
    batch_no = Column(String(20))
    rec_id = Column(Integer, Sequence('seq_rec_id'), primary_key=True)
    created_time = Column(String(7), default=text('sysdate'))
    last_modified_time = Column(String(7), default=text('sysdate'))
    file_type = Column(Enum(UploadFileType))

    def __init__(self, batch_no, local_filename, file_type):
        self.batch_no = batch_no
        self.local_filename = local_filename
        self.file_type = file_type


class MessageState(enum.Enum):
    INIT = "INIT"
    PROCESSING = "PROCESSING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class JobMessage(Base):
    __tablename__ = 'job_message'
    message_id = Column(Integer, Sequence('seq_message_id'), primary_key=True)
    queue_id = Column(String(10))
    message_state = Column(Enum(MessageState), default=MessageState.INIT)
    message_body = Column(String(200))

    def __init__(self, queue_id, message_body):
        self.queue_id = queue_id
        self.message_body = message_body



class JobTrigger(Base):
    __tablename__ = 'job_trigger'
    trigger_name = Column(String(200), primary_key=True)
    trigger_group = Column(String(200))
    job_name = Column(String(20))
    description = Column(String(250))
    next_fire_time = Column(Integer)
    prev_fire_time = Column(Integer)
    priority = Column(Integer)
    trigger_state = Column(String(16))
    trigger_type = Column(String(8))
    start_time = Column(Integer)
    end_time = Column(Integer)
    misfire_instr = Column(Integer)
    trigger_handler = Column(String(20))

    # back_populates
    simple_trigger = relationship('JobSimpleTrigger', uselist=False, back_populates='trigger')


class JobSimpleTrigger(Base):
    __tablename__ = 'job_simple_trigger'
    __table_args__ = (
        ForeignKeyConstraint(
            ['trigger_name'],
            ['job_trigger.trigger_name']
        ),
    )
    trigger_name = Column(String(200), primary_key=True)
    repeat_count = Column(Integer)
    repeat_interval = Column(Integer)
    times_triggered = Column(Integer)

    trigger = relationship('JobTrigger', uselist=False, back_populates='simple_trigger')
