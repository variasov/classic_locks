import hashlib
import time
from typing import Optional

from sqlalchemy.orm import Session

from . import errors
from .locker import EXCLUSIVE, SHARED, Lock, Locker, LockType

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

LOCK_TYPE_FUNC_MAP = {
    EXCLUSIVE: 'pg_try_advisory_xact_lock',
    SHARED: 'pg_try_advisory_xact_lock_shared',
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


class PGAdvisoryLocker(Locker):
    def __init__(
        self,
        session: Session,
        delay: int = 0.5,
        lock_fn: str = None,
        unlock_fn: str = None
    ):
        self.session = session
        self.delay = delay
        self.lock_fn = lock_fn
        self.unlock_fn = unlock_fn

    def __get_lock_func(self, lock_type: LockType) -> str:
        return self.lock_fn or LOCK_TYPE_FUNC_MAP[lock_type]

    def __get_unlock_func(self, lock_fn: str) -> str:
        return self.unlock_fn or LOCK_FUNC_UNLOCK_FUNC_MAP[lock_fn]

    @staticmethod
    def __get_resource_id(resource: str) -> int:
        # postgre принимает в качестве id ресурса bigint (int8)
        return int.from_bytes(
            hashlib.blake2b(resource.encode('utf-8')).digest()[:8],
            'little',
            signed=True
        )

    def acquire(
        self,
        resource: str,
        lock_type: LockType = EXCLUSIVE,
        timeout: int = None,
    ) -> Lock:
        lock_fn = self.__get_lock_func(lock_type)
        query_type = LOCK_FN_QUERY_TYPE_MAP[lock_fn]

        return PGAdvisoryLock(
            session=self.session,
            resource=resource,
            lock_type=lock_type,
            resource_id=self.__get_resource_id(resource),
            lock_fn=lock_fn,
            unlock_fn=self.__get_unlock_func(lock_fn),
            query_type=query_type,
            timeout=timeout,
            delay=self.delay,
        )


class PGAdvisoryLock(Lock):
    def __init__(
        self,
        session: Session,
        resource: str,
        lock_type: LockType,
        resource_id: int,
        lock_fn: str,
        unlock_fn: str,
        query_type: str,
        timeout: Optional[int] = None,
        delay: int = 0.5,
    ):
        self.session = session
        self.resource = resource
        self.lock_type = lock_type
        self.resource_id = resource_id
        self.lock_fn = lock_fn
        self.unlock_fn = unlock_fn
        self.query_type = query_type
        self.timeout = timeout
        self.delay = delay

        self.cursor = None

    def __enter__(self):
        start_time = time.monotonic()
        conn = self.session.connection().connection
        self.cursor = conn.cursor()

        while True:
            self.cursor.execute(
                'SELECT {}(%s)'.format(self.lock_fn), (self.resource_id, )
            )
            is_access = self.cursor.fetchone()[0]

            if self.query_type == WAIT_QUERY or is_access:
                break

            if not self.timeout or time.monotonic() - start_time > self.timeout:
                raise errors.ResourceIsLocked(resource=self.resource)

            time.sleep(self.delay)

    def __exit__(self, *exc):
        if self.unlock_fn:
            self.cursor.execute(
                'SELECT {}(%s)'.format(self.unlock_fn), (self.resource_id, )
            )
        self.cursor.close()
