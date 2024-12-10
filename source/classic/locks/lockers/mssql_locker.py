from typing import Literal

try:
    from sqlalchemy import text
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

OWNER_TYPE_TRANSACTION = 'Transaction'
OWNER_TYPE_SESSION = 'Session'

OwnerType = Literal['Transaction', 'Session']
LockModeType = Literal[
    'Shared',
    'Update',
    'Exclusive',
    'Intent',
    'Schema',
    'Bulk Update',
    'Key-range',
]

LOCK_TYPE_FUNC_MAP = {
    EXCLUSIVE: 'Exclusive',
    SHARED: 'Shared',
}


def handle_timeout(timeout: int | None, block: bool) -> int:
    """
    Преобразует timeout в миллисекунды для MS SQL Server.
    
    Args:
        timeout: время ожидания в секундах
        block: если False, возвращает 0 (неблокирующий вызов)
    
    Returns:
        timeout в миллисекун��ах:
        - 0: неблокирующий вызов
        - -1: бесконечное ожидание
        - >0: ожидание в миллисекундах
    """
    if not block:
        return 0
        
    if timeout is None:
        return -1  # бесконечное ожидание
        
    return timeout * 1000 if timeout > 0 else -1

def get_lock_mode(
    lock_type: LockType,
    default_lock_type: LockType,
    custom_mode: LockModeType | None = None,
) -> LockModeType:
    if custom_mode:
        return custom_mode

    lock_type = lock_type if lock_type is not None else default_lock_type

    if lock_type == EXCLUSIVE:
        return 'Exclusive'
    return 'Shared'

class AcquirePyMSSQLAdvisoryLock(AcquireLock):
    def __init__(
        self,
        delay: int = 0.5,
        block: bool = True,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
        lock_mode: LockModeType | None = None,
    ):
        try:
            import pymssql  # noqa
        except ImportError:
            raise ImportError(
                "pymssql is required for AcquirePyMSSQLAdvisoryLock. "
                "Install it with: pip install classic-locks[mssql-pymssql]"
            )
        self.delay = delay
        self.block = block
        self.lock_type = lock_type
        self.scope = scope
        self.lock_mode = lock_mode

    def __call__(
        self,
        connection: object,
        resource: str,
        block: bool = True,
        timeout: int = None,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ) -> Lock:
        lock_mode = get_lock_mode(
            lock_type=lock_type,
            default_lock_type=self.lock_type,
            custom_mode=self.lock_mode,
        )

        return PyMSSQLAdvisoryLock(
            connection=connection,
            resource=resource,
            lock_mode=lock_mode,
            lock_owner=(
                OWNER_TYPE_TRANSACTION
                if scope == TRANSACTION
                else OWNER_TYPE_SESSION
            ),
            timeout=handle_timeout(timeout, block),
        )


class PyMSSQLAdvisoryLock(Lock):
    def __init__(
        self,
        connection,
        resource: str,
        lock_mode: LockModeType,
        timeout: int | None = None,
        delay: int = 0.5,
        lock_owner: OwnerType = TRANSACTION,
        database_principal: str = 'public',
    ):
        self.connection = connection
        self.resource = resource
        self.lock_mode = lock_mode
        self.timeout = timeout
        self.delay = delay
        self.lock_owner = lock_owner
        self.database_principal = database_principal

    def __enter__(self):
        with self.connection.cursor() as cursor:
            query = """
                DECLARE @result int;
                EXEC @result = sp_getapplock
                    @DbPrincipal = %s,
                    @Resource = %s,
                    @LockMode = %s,
                    @LockOwner = %s,
                    @LockTimeout = %d;
                SELECT @result;
            """
            cursor.execute(
                query, (
                    self.database_principal,
                    self.resource,
                    self.lock_mode,
                    self.lock_owner,
                    self.timeout,
                )
            )
            is_access = cursor.fetchone()[0]

            if is_access < 0:
                raise errors.ResourceIsLocked(resource=self.resource)

    def __exit__(self, *exc):
        with self.connection.cursor() as cursor:
            query = """
                EXEC sp_releaseapplock
                    @DbPrincipal = %s,
                    @Resource = %s,
                    @LockOwner = %s;
            """
            cursor.execute(
                query, (
                    self.database_principal,
                    self.resource,
                    self.lock_owner,
                )
            )


class AcquireSQLAlchemyMSAdvisoryLock(AcquireLock):
    def __init__(
        self,
        delay: int = 0.5,
        block: bool = True,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
        lock_mode: LockModeType | None = None,
    ):
        if not HAVE_SQLALCHEMY:
            raise ImportError(
                "SQLAlchemy is required for AcquireSQLAlchemyMSAdvisoryLock. "
                "Install it with: pip install classic-locks[mssql-sqlalchemy]"
            )
        self.delay = delay
        self.block = block
        self.lock_type = lock_type
        self.scope = scope
        self.lock_mode = lock_mode

    def __call__(
        self,
        session: Session,
        resource: str,
        block: bool = True,
        timeout: int = None,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ) -> Lock:
        lock_mode = get_lock_mode(
            lock_type=lock_type,
            default_lock_type=self.lock_type,
            custom_mode=self.lock_mode,
        )

        return SQLAlchemyMSAdvisoryLock(
            session=session,
            resource=resource,
            lock_mode=lock_mode,
            lock_owner=(
                OWNER_TYPE_TRANSACTION
                if scope == TRANSACTION
                else OWNER_TYPE_SESSION
            ),
            timeout=handle_timeout(timeout, block),
        )


class SQLAlchemyMSAdvisoryLock(Lock):
    def __init__(
        self,
        session: Session,
        resource: str,
        lock_mode: LockModeType,
        timeout: int | None = None,
        delay: int = 0.5,
        lock_owner: OwnerType = OWNER_TYPE_TRANSACTION,
        database_principal: str = 'public',
    ):
        self.session = session
        self.resource = resource
        self.lock_mode = lock_mode
        self.timeout = timeout
        self.delay = delay
        self.lock_owner = lock_owner
        self.database_principal = database_principal

    def __enter__(self):
        query = text("""
            DECLARE @result int;
            EXEC @result = sp_getapplock
                @DbPrincipal = :principal,
                @Resource = :resource,
                @LockMode = :mode,
                @LockOwner = :owner,
                @LockTimeout = :timeout;
            SELECT @result;
        """)

        result = self.session.execute(
            query,
            {
                'principal': self.database_principal,
                'resource': self.resource,
                'mode': self.lock_mode,
                'owner': self.lock_owner,
                'timeout': self.timeout,
            }
        )
        is_access = result.scalar()

        if is_access < 0:
            raise errors.ResourceIsLocked(resource=self.resource)

    def __exit__(self, *exc):
        query = text("""
            EXEC sp_releaseapplock
                @DbPrincipal = :principal,
                @Resource = :resource,
                @LockOwner = :owner;
        """)

        self.session.execute(
            query,
            {
                'principal': self.database_principal,
                'resource': self.resource,
                'owner': self.lock_owner,
            }
        )
