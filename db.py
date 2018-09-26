# coding=utf-8
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import config

engine = None


def get_session():
    global engine
    if engine is None:
        engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=config.SQLALCHEMY_ECHO)

    return Session(engine)
