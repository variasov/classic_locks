from abc import ABC, abstractmethod
from typing import Literal

EXCLUSIVE = 'EXCLUSIVE'
SHARED = 'SHARED'

TRANSACTION = 'TRANSACTION'
SESSION = 'SESSION'

LockType = Literal['EXCLUSIVE', 'SHARED']
ScopeType = Literal['TRANSACTION', 'SESSION']


class AcquireLock(ABC):
    @abstractmethod
    def __call__(
        self,
        connection: object,
        resource: str,
        block: bool = True,
        timeout: int = None,
        lock_type: LockType = EXCLUSIVE,
        scope: ScopeType = TRANSACTION,
    ) -> 'Lock':
        ...


class Lock(ABC):
    resource: str
    timeout: int

    @abstractmethod
    def __enter__(self):
        ...

    @abstractmethod
    def __exit__(self, *exc):
        ...
