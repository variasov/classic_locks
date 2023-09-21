import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from classic.locks import EXCLUSIVE, SHARED, PGAdvisoryLocker

from .common import *

DB_URL = 'postgresql+psycopg2://postgres:postgres@localhost:5432/test_db'


@pytest.fixture(scope='session')
def engine():
    return create_engine(DB_URL)


@pytest.fixture(scope='function')
def session(engine):
    session = sessionmaker(bind=engine)()

    if session.in_transaction():
        session.begin_nested()
    else:
        session.begin()

    yield session

    session.rollback()


@pytest.fixture(scope='function')
def session_2(engine):
    session = sessionmaker(bind=engine)()

    if session.in_transaction():
        session.begin_nested()
    else:
        session.begin()

    yield session

    session.rollback()


@pytest.fixture(scope='function')
def locker_1(session):
    return PGAdvisoryLocker(session=session)


@pytest.fixture(scope='function')
def obj(some_csl, locker_1):
    return some_csl(locker_1=locker_1)


@pytest.fixture(scope='function')
def excl_lock(session_2, resource_1_locker):
    return PGAdvisoryLocker(session=session_2).acquire(
        resource=resource_1_locker,
        lock_type=EXCLUSIVE,
    )


@pytest.fixture(scope='function')
def excl_lock_2(session_2, resource_2_locker):
    return PGAdvisoryLocker(session=session_2).acquire(
        resource=resource_2_locker,
        lock_type=EXCLUSIVE,
    )


@pytest.fixture(scope='function')
def shared_lock(session_2, resource_1_locker):
    return PGAdvisoryLocker(session=session_2).acquire(
        resource=resource_1_locker,
        lock_type=SHARED,
    )
