from abc import ABC, abstractmethod
from typing import Literal, Optional

EXCLUSIVE = 'EXCLUSIVE'
SHARED = 'SHARED'

LockType = Literal['EXCLUSIVE', 'SHARED']


class Locker(ABC):
    @abstractmethod
    def acquire(
        self,
        resource: str,
        lock_type: LockType = EXCLUSIVE,
        timeout: Optional[int] = None,
    ) -> 'Lock':
        ...


class Lock(ABC):
    resource: str
    lock_type: LockType
    timeout: int

    @abstractmethod
    def __enter__(self):
        ...

    @abstractmethod
    def __exit__(self, *exc):
        ...
