from typing import Literal, Optional

from sqlalchemy.orm import Session

from . import errors
from .locker import EXCLUSIVE, SHARED, Lock, Locker, LockType

TRANSACTION = 'Transaction'
SESSION = 'Session'

OwnerType = Literal['Transaction', 'Session']
LockModeType = Literal['Shared',
                       'Update',
                       'Exclusive',
                       'Intent',
                       'Schema',
                       'Bulk Update',
                       'Key-range',
                       ]

LOCK_TYPE_FUNC_MAP = {
    EXCLUSIVE: 'Exclusive ',
    SHARED: 'Shared',
}


class MSAdvisoryLocker(Locker):
    def __init__(self, session: Session, lock_mode: LockModeType = None):
        self.session = session
        self.lock_mode = lock_mode

    def __handle_lock_mode(self, lock_type) -> str:
        return self.lock_mode or LOCK_TYPE_FUNC_MAP[lock_type]

    @staticmethod
    def __handle_timeout(timeout) -> int:
        if timeout:
            return timeout * 1000 if timeout > 0 else -1

        return 0

    def acquire(
        self,
        resource: str,
        lock_type: LockType = EXCLUSIVE,
        timeout: int = None,
    ) -> Lock:
        return MSAdvisoryLock(
            session=self.session,
            resource=resource,
            lock_type=lock_type,
            lock_mode=self.__handle_lock_mode(lock_type),
            timeout=self.__handle_timeout(timeout),
        )


class MSAdvisoryLock(Lock):
    def __init__(
        self,
        session: Session,
        resource: str,
        lock_type: LockType,
        lock_mode: LockModeType,
        timeout: Optional[int] = None,
        delay: int = 0.5,
        lock_owner: OwnerType = TRANSACTION,
        database_principal: str = 'public',
    ):
        self.session = session
        self.resource = resource
        self.lock_type = lock_type
        self.lock_mode = lock_mode
        self.timeout = timeout
        self.delay = delay
        self.lock_owner = lock_owner
        self.database_principal = database_principal

        self.cursor = None

    def __enter__(self):
        conn = self.session.connection().connection
        self.cursor = conn.cursor()

        query = "DECLARE @result int;" \
                " EXEC @result = sp_getapplock" \
                " @DbPrincipal = %s," \
                " @Resource = %s," \
                " @LockMode = %s," \
                " @LockOwner = %s," \
                " @LockTimeout = %d;" \
                " SELECT @result;"
        self.cursor.execute(
            query, (
                self.database_principal,
                self.resource,
                self.lock_mode,
                self.lock_owner,
                self.timeout,
            )
        )

        is_access = self.cursor.fetchone()[0]

        if is_access < 0:
            raise errors.ResourceIsLocked(resource=self.resource)

    def __exit__(self, *exc):
        query = "EXEC sp_releaseapplock" \
                " @DbPrincipal = %s," \
                " @Resource = %s," \
                " @LockOwner = %s;"
        self.cursor.execute(
            query, (
                self.database_principal,
                self.resource,
                self.lock_owner,
            )
        )
        self.cursor.close()
