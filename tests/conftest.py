import os

import pytest

try:
    import psycopg2

    HAVE_PSYCOPG2 = True
except ImportError:
    HAVE_PSYCOPG2 = False

try:
    import psycopg

    HAVE_PSYCOPG3 = True
except ImportError:
    HAVE_PSYCOPG3 = False

try:
    import pymssql

    HAVE_PYMSSQL = True
except ImportError:
    HAVE_PYMSSQL = False

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    HAVE_SQLALCHEMY = True
except ImportError:
    HAVE_SQLALCHEMY = False
    create_engine, Session = None, None

from classic.locks import (
    AcquirePsycopg2PGAdvisoryLock,
    AcquirePsycopg3PGAdvisoryLock,
    AcquirePyMSSQLAdvisoryLock,
    AcquireSQLAlchemyMSAdvisoryLock,
    AcquireSQLAlchemyPGAdvisoryLock,
)

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'postgres')
PG_DATABASE = os.getenv('PG_DATABASE', 'test_db')

MSSQL_HOST = os.getenv('MSSQL_HOST', 'localhost')
MSSQL_PORT = os.getenv('MSSQL_PORT', '1433')
MSSQL_USER = os.getenv('MSSQL_USER', 'sa')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD', 'Password123!')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE', 'test_db')

PG_DSN = (
    f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)
MSSQL_DSN = (
    f"mssql+pymssql://{MSSQL_USER}:{MSSQL_PASSWORD}@{MSSQL_HOST}:{MSSQL_PORT}"
    f"/{MSSQL_DATABASE}"
)
PG_CONNINFO = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} user={PG_USER} "
    f"password={PG_PASSWORD}"
)


def skip_if_no_pg2():
    """Пропускает тест если нет подключения к PostgreSQL через psycopg2"""
    if not HAVE_PSYCOPG2:
        pytest.skip("psycopg2 is not installed")
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.close()
    except:
        pytest.skip("PostgreSQL is not available for psycopg2")


def skip_if_no_pg3():
    """Пропускает тест если нет подключения к PostgreSQL через psycopg3"""
    if not HAVE_PSYCOPG3:
        pytest.skip("psycopg3 is not installed")
    try:
        conn = psycopg.connect(PG_CONNINFO)
        conn.close()
    except:
        pytest.skip("PostgreSQL is not available for psycopg3")


def skip_if_no_mssql():
    """Пропускает тест если нет подключения к MS SQL через pymssql"""
    if not HAVE_PYMSSQL:
        pytest.skip("pymssql is not installed")
    try:
        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=MSSQL_PORT,
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_DATABASE
        )
        conn.close()
    except:
        pytest.skip("MS SQL Server is not available")


def skip_if_no_sqlalchemy():
    """Пропускает тест если нет SQLAlchemy"""
    if not HAVE_SQLALCHEMY:
        pytest.skip("SQLAlchemy is not installed")


@pytest.fixture
def psycopg2_conn1():
    """Фикстура для подключения к PostgreSQL через psycopg2"""
    skip_if_no_pg2()
    conn = psycopg2.connect(PG_DSN)
    yield conn
    conn.close()


@pytest.fixture
def psycopg2_conn2():
    """Фикстура для подключения к PostgreSQL через psycopg2"""
    skip_if_no_pg2()
    conn = psycopg2.connect(PG_DSN)
    yield conn
    conn.close()


@pytest.fixture
def psycopg3_conn1():
    """Фикстура для подключения к PostgreSQL через psycopg3"""
    skip_if_no_pg3()
    conn = psycopg.connect(PG_CONNINFO)
    yield conn
    conn.close()


@pytest.fixture
def psycopg3_conn2():
    """Фикстура для подключения к PostgreSQL через psycopg3"""
    skip_if_no_pg3()
    conn = psycopg.connect(PG_CONNINFO)
    yield conn
    conn.close()


@pytest.fixture
def mssql_conn1():
    """Фикстура для подключения к MS SQL через pymssql"""
    skip_if_no_mssql()
    conn = pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE
    )
    yield conn
    conn.close()


@pytest.fixture
def mssql_conn2():
    """Фикстура для подключения к MS SQL через pymssql"""
    skip_if_no_mssql()
    conn = pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE
    )
    yield conn
    conn.close()


@pytest.fixture
def pg_sqlalchemy_session1():
    """Фикстура для подключения к PostgreSQL через SQLAlchemy"""
    skip_if_no_pg2()
    skip_if_no_sqlalchemy()
    engine = create_engine(PG_DSN)
    session = Session(engine)
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def pg_sqlalchemy_session2():
    """Фикстура для подключения к PostgreSQL через SQLAlchemy"""
    skip_if_no_pg2()
    skip_if_no_sqlalchemy()
    engine = create_engine(PG_DSN)
    session = Session(engine)
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def mssql_sqlalchemy_session1():
    """Фикстура для подключения к MS SQL через SQLAlchemy"""
    skip_if_no_mssql()
    skip_if_no_sqlalchemy()
    engine = create_engine(MSSQL_DSN)
    session = Session(engine)
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def mssql_sqlalchemy_session2():
    """Фикстура для подключения к MS SQL через SQLAlchemy"""
    skip_if_no_mssql()
    skip_if_no_sqlalchemy()
    engine = create_engine(MSSQL_DSN)
    session = Session(engine)
    yield session
    session.close()
    engine.dispose()


implementations = (
    pytest.param(
        (
            AcquirePsycopg2PGAdvisoryLock(),
            "psycopg2_conn1",
            "psycopg2_conn2",
        ),
        marks=[pytest.mark.postgres, pytest.mark.psycopg2],
        id="psycopg2-pg-lock"
    ) if HAVE_PSYCOPG2 else None,
    pytest.param(
        (
            AcquirePsycopg3PGAdvisoryLock(),
            "psycopg3_conn1",
            "psycopg3_conn2",
        ),
        marks=[pytest.mark.postgres, pytest.mark.psycopg3],
        id="psycopg3-pg-lock"
    ) if HAVE_PSYCOPG3 else None,
    pytest.param(
        (
            AcquireSQLAlchemyPGAdvisoryLock(),
            "pg_sqlalchemy_session1",
            "pg_sqlalchemy_session2",
        ),
        marks=[pytest.mark.postgres, pytest.mark.sqlalchemy],
        id="sqlalchemy-pg-lock"
    ) if HAVE_SQLALCHEMY else None,
    pytest.param(
        (
            AcquirePyMSSQLAdvisoryLock(),
            "mssql_conn1",
            "mssql_conn2",
        ),
        marks=[pytest.mark.mssql, pytest.mark.pymssql],
        id="pymssql-lock"
    ) if HAVE_PYMSSQL else None,
    pytest.param(
        (
            AcquireSQLAlchemyMSAdvisoryLock(),
            "mssql_sqlalchemy_session1",
            "mssql_sqlalchemy_session2",
        ),
        marks=[pytest.mark.mssql, pytest.mark.sqlalchemy],
        id="sqlalchemy-mssql-lock"
    ) if HAVE_SQLALCHEMY and HAVE_PYMSSQL else None,
)
implementations = tuple(_ for _ in implementations if _ is not None)


@pytest.fixture(params=implementations)
def locker_fixture(request):
    """
    Параметризованная фикстура, возвращающая кортеж из:
    - объекта блокировки
    - имени фикстуры с подключением к БД (1й коннект)
    - имени фикстуры с подключением к БД (2й коннект)
    """
    return request.param
