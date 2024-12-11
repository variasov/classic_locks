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


def get_resource_id(resource: str) -> int:
    """
    Преобразует строковый ресурс в числовой идентификатор для PostgreSQL.
    
    Args:
        resource: Строковое имя ресурса
        
    Returns:
        int: 64-битный signed integer, полученный из хеша blake2b строки
    """
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
    """
    'pg_advisory_lock' - блокирующая обычная блокировка
    'pg_advisory_lock_shared' - блокирующая shared блокировка
    'pg_advisory_xact_lock' - блокирующая обычная блокировка в рамках транзакции
    'pg_advisory_xact_lock_shared' - блокирующая shared блокировка в рамках
        транзакции
    'pg_try_advisory_lock' - неблокирующая обычная блокировка
    'pg_try_advisory_lock_shared' - неблокирующая shared блокировка
    'pg_try_advisory_xact_lock' - неблокирующая обычная блокировка в
        рамках транзакции
    'pg_try_advisory_xact_lock_shared' - неблокирующая shared блокировка в
        рамках транзакции
    """
    # Используем значения по умолчанию, если параметры не указаны
    block = block if block is not None else default_block
    lock_type = lock_type if lock_type is not None else default_lock_type
    scope = scope if scope is not None else default_scope

    # Формируем базовое имя функции
    fn_name = 'pg_'

    # Добавляем 'try_' если блокировка неблокирующая
    if not block:
        fn_name += 'try_'

    fn_name += 'advisory_'

    # Добавляем 'xact_' если область действия - транзакция
    if scope == TRANSACTION:
        fn_name += 'xact_'

    fn_name += 'lock'

    # Добавляем '_shared' если тип блокировки - shared
    if lock_type == SHARED:
        fn_name += '_shared'

    return fn_name


def get_unlock_fn(lock_fn: str) -> str | None:
    """
    Возвращает имя функции для освобождения блокировки.
    
    Для транзакционных блокировок (содержащих 'xact') возвращает None,
    так как они освобождаются автоматически при завершении транзакции.
    
    Для остальных задаем соответствие:
    - pg_advisory_lock -> pg_advisory_unlock
    - pg_advisory_lock_shared -> pg_advisory_unlock_shared
    - pg_try_advisory_lock -> pg_advisory_unlock
    - pg_try_advisory_lock_shared -> pg_advisory_unlock_shared
    """
    if 'xact' in lock_fn:
        return None

    if lock_fn == 'pg_advisory_lock':
        return 'pg_advisory_unlock'
    if lock_fn == 'pg_advisory_lock_shared':
        return 'pg_advisory_unlock_shared'
    if lock_fn == 'pg_try_advisory_lock':
        return 'pg_advisory_unlock'
    if lock_fn == 'pg_try_advisory_lock_shared':
        return 'pg_advisory_unlock_shared'


class AcquirePsycopg2PGAdvisoryLock(AcquireLock):
    """
    Фабрика для создания блокировок PostgreSQL через psycopg2.
    
    Args:
        delay: Задержка между попытками получения блокировки в секундах
        block: Блокирующий режим по умолчанию
        lock_type: Тип блокировки по умолчанию (EXCLUSIVE или SHARED)
        scope: Область действия блокировки по умолчанию (TRANSACTION или SESSION)
        
    Raises:
        ImportError: Если psycopg2 не установлен
    """
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
            block=block,
            timeout=timeout,
            delay=self.delay,
        )


class Psycopg2PGAdvisoryLock(Lock):
    """
    Реализация блокировки PostgreSQL через psycopg2.
    
    Args:
        connection: Соединение psycopg2
        resource: Имя блокируемого ресурса
        lock_fn: Имя функции блокировки в PostgreSQL
        block: Блокирующий режим
        timeout: Таймаут в секундах (None для бесконечного ожидания)
        delay: Задержка между попытками в секундах
        
    Raises:
        ResourceIsLocked: Если ресурс заблокирован и timeout истек
    """
    def __init__(
        self,
        connection,
        resource: str,
        lock_fn: str,
        block: bool,
        timeout: int | None = None,
        delay: int = 0.5,
    ):
        self.connection = connection
        self.resource = resource
        self.resource_id = get_resource_id(resource)
        self.lock_fn = lock_fn
        self.unlock_fn = get_unlock_fn(lock_fn)
        self.block = block
        self.timeout = timeout
        self.delay = delay

    def __enter__(self):
        start_time = time.monotonic()

        while True:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    'SELECT {}(%s)'.format(self.lock_fn), (self.resource_id,)
                )
                is_access = cursor.fetchone()[0]

            if self.block or is_access:
                break

            if not self.timeout or time.monotonic() - start_time > self.timeout:
                raise errors.ResourceIsLocked(resource=self.resource)

            time.sleep(self.delay)

    def __exit__(self, *exc):
        if self.unlock_fn:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    'SELECT {}(%s)'.format(self.unlock_fn), (self.resource_id,)
                )


class AcquireSQLAlchemyPGAdvisoryLock(AcquireLock):
    """
    Фабрика для создания блокировок PostgreSQL через SQLAlchemy.
    
    Args:
        delay: Задержка между попытками получения блокировки в секундах
        block: Блокирующий режим по умолчанию
        lock_type: Тип блокировки по умолчанию (EXCLUSIVE или SHARED)
        scope: Область действия блокировки по умолчанию (TRANSACTION или SESSION)
        
    Raises:
        ImportError: Если SQLAlchemy не установлен
    """
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
            block=block,
            timeout=timeout,
            delay=self.delay,
        )


class SQLAlchemyPGAdvisoryLock(Lock):
    """
    Реализация блокировки PostgreSQL через SQLAlchemy.
    
    Args:
        session: Сессия SQLAlchemy
        resource: Имя блокируемого ресурса
        lock_fn: Имя функции блокировки в PostgreSQL
        block: Блокирующий режим
        timeout: Таймаут в секундах (None для бесконечного ожидания)
        delay: Задержка между попытками в секундах
        
    Raises:
        ResourceIsLocked: Если ресурс заблокирован и timeout истек
    """
    def __init__(
        self,
        session: Session,
        resource: str,
        lock_fn: str,
        block: bool,
        timeout: int | None = None,
        delay: int = 0.5,
    ):
        self.session = session
        self.resource = resource
        self.resource_id = get_resource_id(resource)
        self.lock_fn = lock_fn
        self.unlock_fn = get_unlock_fn(lock_fn)
        self.block = block
        self.timeout = timeout
        self.delay = delay

    def __enter__(self):
        start_time = time.monotonic()

        while True:
            result = self.session.execute(
                select(getattr(func, self.lock_fn)(self.resource_id))
            )
            is_access = result.scalar()

            if self.block or is_access:
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
    """
    Фабрика для создания блокировок PostgreSQL через psycopg3.
    
    Args:
        delay: Задержка между попытками получения блокировки в секундах
        block: Блокирующий режим по умолчанию
        lock_type: Тип блокировки по умолчанию (EXCLUSIVE или SHARED)
        scope: Область действия блокировки по умолчанию (TRANSACTION или SESSION)
        
    Raises:
        ImportError: Если psycopg3 не установлен
    """
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
            block=block,
            timeout=timeout,
            delay=self.delay,
        )


class Psycopg3PGAdvisoryLock(Lock):
    """
    Реализация блокировки PostgreSQL через psycopg3.
    
    Args:
        connection: Соединение psycopg3
        resource: Имя блокируемого ресурса
        lock_fn: Имя функции блокировки в PostgreSQL
        block: Блокирующий режим
        timeout: Таймаут в секундах (None для бесконечного ожидания)
        delay: Задержка между попытками в секундах
        
    Raises:
        ResourceIsLocked: Если ресурс заблокирован и timeout истек
    """
    def __init__(
        self,
        connection,
        resource: str,
        lock_fn: str,
        block: bool,
        timeout: int | None = None,
        delay: int = 0.5,
    ):
        self.connection = connection
        self.resource = resource
        self.resource_id = get_resource_id(resource)
        self.lock_fn = lock_fn
        self.unlock_fn = get_unlock_fn(lock_fn)
        self.block = block
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

            if self.block or is_access:
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
