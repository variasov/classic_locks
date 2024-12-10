import hashlib
import time

try:
    from sqlalchemy import func, select
    from sqlalchemy.orm import Session
    HAVE_SQLALCHEMY = True
except ImportError:
    HAVE_SQLALCHEMY = False
    Session = None

from .. import errors
from ..lock import (
    EXCLUSIVE,
    SHARED,
    TRANSACTION,
    AcquireLock,
    Lock,
    LockType,
    ScopeType,
)

TRY_QUERY = 'TRY'
WAIT_QUERY = 'WAIT'

LOCK_FN_QUERY_TYPE_MAP = {
    'pg_advisory_lock': WAIT_QUERY,
    'pg_advisory_lock_shared': WAIT_QUERY,
    'pg_advisory_xact_lock': WAIT_QUERY,
    'pg_advisory_xact_lock_shared': WAIT_QUERY,
    'pg_try_advisory_lock': TRY_QUERY,
    'pg_try_advisory_lock_shared': TRY_QUERY,
    'pg_try_advisory_xact_lock': TRY_QUERY,
    'pg_try_advisory_xact_lock_shared': TRY_QUERY,
}

LOCK_FUNC_UNLOCK_FUNC_MAP = {
    'pg_advisory_lock': 'pg_advisory_unlock',
    'pg_advisory_lock_shared': 'pg_advisory_unlock_shared',
    'pg_advisory_xact_lock': None,
    'pg_advisory_xact_lock_shared': None,
    'pg_try_advisory_lock': 'pg_advisory_unlock',
    'pg_try_advisory_lock_shared': 'pg_advisory_unlock_shared',
    'pg_try_advisory_xact_lock': None,
    'pg_try_advisory_xact_lock_shared': None,
}


def get_resource_id(resource: str) -> int:
    # postgre принимает в качестве id ресурса bigint (int8)
    return int.from_bytes(
        hashlib.blake2b(resource.encode('utf-8')).digest()[:8],
        'little',
        signed=True
    )

def get_lock_fn(
    block: bool,
    lock_type: LockType,
    scope: ScopeType,
    default_block: bool,
    default_lock_type: LockType,
    default_scope: ScopeType,
) -> str:
    # Используем значения по умолчанию, если параметры не указаны
    block = block if block is not None else default_block
    lock_type = lock_type if lock_type is not None else default_lock_type
    scope = scope if scope is not None else default_scope
    
    # Формируем базовое имя функции
    fn_name = 'pg_'
    
    # Добавляем 'try_' если блокировка неблокирующая
    if not block:
        fn_name += 'try_'
    
    # Добавляем 'advisory_'
    fn_name += 'advisory_'
    
    # Добавляем 'xact_' если область действия - транзакция
    if scope == TRANSACTION:
        fn_name += 'xact_'
    
    # Добавляем 'lock'
    fn_name += 'lock'
    
    # Добавляем '_shared' если тип блокировки - shared
    if lock_type == SHARED:
        fn_name += '_shared'
    
    return fn_name


class AcquirePsycopg2PGAdvisoryLock(AcquireLock):
    def __init__(
        self,
        delay: int = 0.5,
        block: bool = True,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ):
        try:
            import psycopg2  # noqa
        except ImportError:
            raise ImportError(
                "psycopg2 is required for AcquirePsycopg2PGAdvisoryLock. "
                "Install it with: pip install classic-locks[postgres-psycopg2]"
            )
        self.delay = delay
        self.block = block
        self.lock_type = lock_type
        self.scope = scope

    def __call__(
        self,
        connection: object,
        resource: str,
        block: bool = True,
        timeout: int = None,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ) -> Lock:

        lock_fn = get_lock_fn(
            block, lock_type, scope, self.block, self.lock_type, self.scope
        )

        return Psycopg2PGAdvisoryLock(
            connection=connection,
            resource=resource,
            lock_fn=lock_fn,
            timeout=timeout,
            delay=self.delay,
        )


class Psycopg2PGAdvisoryLock(Lock):
    def __init__(
        self,
        connection,
        resource: str,
        lock_fn: str,
        timeout: int | None = None,
        delay: int = 0.5,
    ):
        self.connection = connection
        self.resource = resource
        self.resource_id = get_resource_id(resource)
        self.lock_fn = lock_fn
        self.unlock_fn = LOCK_FUNC_UNLOCK_FUNC_MAP[lock_fn]
        self.query_type = LOCK_FN_QUERY_TYPE_MAP[lock_fn]
        self.timeout = timeout
        self.delay = delay

    def __enter__(self):
        start_time = time.monotonic()

        while True:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    'SELECT {}(%s)'.format(self.lock_fn), (self.resource_id, )
                )
                is_access = cursor.fetchone()[0]

            if self.query_type == WAIT_QUERY or is_access:
                break

            if not self.timeout or time.monotonic() - start_time > self.timeout:
                raise errors.ResourceIsLocked(resource=self.resource)

            time.sleep(self.delay)

    def __exit__(self, *exc):
        if self.unlock_fn:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    'SELECT {}(%s)'.format(self.unlock_fn), (self.resource_id, )
                )


class AcquireSQLAlchemyPGAdvisoryLock(AcquireLock):
    def __init__(
        self,
        delay: int = 0.5,
        block: bool = True,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ):
        if not HAVE_SQLALCHEMY:
            raise ImportError(
                "SQLAlchemy is required for AcquireSQLAlchemyPGAdvisoryLock. "
                "Install it with: pip install classic-locks[postgres-sqlalchemy]"
            )
        self.delay = delay
        self.block = block
        self.lock_type = lock_type
        self.scope = scope

    def __call__(
        self,
        session: Session,
        resource: str,
        block: bool = True,
        timeout: int = None,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ) -> Lock:
        lock_fn = get_lock_fn(
            block, lock_type, scope, self.block, self.lock_type, self.scope
        )

        return SQLAlchemyPGAdvisoryLock(
            session=session,
            resource=resource,
            lock_fn=lock_fn,
            timeout=timeout,
            delay=self.delay,
        )


class SQLAlchemyPGAdvisoryLock(Lock):
    def __init__(
        self,
        session: Session,
        resource: str,
        lock_fn: str,
        timeout: int | None = None,
        delay: int = 0.5,
    ):
        self.session = session
        self.resource = resource
        self.resource_id = get_resource_id(resource)
        self.lock_fn = lock_fn
        self.unlock_fn = LOCK_FUNC_UNLOCK_FUNC_MAP[lock_fn]
        self.query_type = LOCK_FN_QUERY_TYPE_MAP[lock_fn]
        self.timeout = timeout
        self.delay = delay

    def __enter__(self):
        start_time = time.monotonic()

        while True:
            result = self.session.execute(
                select(getattr(func, self.lock_fn)(self.resource_id))
            )
            is_access = result.scalar()

            if self.query_type == WAIT_QUERY or is_access:
                break

            if not self.timeout or time.monotonic() - start_time > self.timeout:
                raise errors.ResourceIsLocked(resource=self.resource)

            time.sleep(self.delay)

    def __exit__(self, *exc):
        if self.unlock_fn:
            self.session.execute(
                select(getattr(func, self.unlock_fn)(self.resource_id))
            )


class AcquirePsycopg3PGAdvisoryLock(AcquireLock):
    def __init__(
        self,
        delay: int = 0.5,
        block: bool = True,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ):
        try:
            import psycopg  # noqa
        except ImportError:
            raise ImportError(
                "psycopg3 is required for AcquirePsycopg3PGAdvisoryLock. "
                "Install it with: pip install classic-locks[postgres-psycopg3]"
            )
        self.delay = delay
        self.block = block
        self.lock_type = lock_type
        self.scope = scope

    def __call__(
        self,
        connection: object,
        resource: str,
        block: bool = True,
        timeout: int = None,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ) -> Lock:
        lock_fn = get_lock_fn(
            block, lock_type, scope, self.block, self.lock_type, self.scope
        )

        return Psycopg3PGAdvisoryLock(
            connection=connection,
            resource=resource,
            lock_fn=lock_fn,
            timeout=timeout,
            delay=self.delay,
        )


class Psycopg3PGAdvisoryLock(Lock):
    def __init__(
        self,
        connection,
        resource: str,
        lock_fn: str,
        timeout: int | None = None,
        delay: int = 0.5,
    ):
        self.connection = connection
        self.resource = resource
        self.resource_id = get_resource_id(resource)
        self.lock_fn = lock_fn
        self.unlock_fn = LOCK_FUNC_UNLOCK_FUNC_MAP[lock_fn]
        self.query_type = LOCK_FN_QUERY_TYPE_MAP[lock_fn]
        self.timeout = timeout
        self.delay = delay

    def __enter__(self):
        start_time = time.monotonic()

        while True:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT {self.lock_fn}(%s)',
                    [self.resource_id]
                )
                is_access = cursor.fetchone()[0]

            if self.query_type == WAIT_QUERY or is_access:
                break

            if not self.timeout or time.monotonic() - start_time > self.timeout:
                raise errors.ResourceIsLocked(resource=self.resource)

            time.sleep(self.delay)

    def __exit__(self, *exc):
        if self.unlock_fn:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT {self.unlock_fn}(%s)',
                    [self.resource_id]
                )
