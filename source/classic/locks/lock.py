from abc import ABC, abstractmethod
from typing import Literal

EXCLUSIVE = 'EXCLUSIVE'
SHARED = 'SHARED'

TRANSACTION = 'TRANSACTION'
SESSION = 'SESSION'

LockType = Literal['EXCLUSIVE', 'SHARED']
ScopeType = Literal['TRANSACTION', 'SESSION']


class AcquireLock(ABC):
    """
    Абстрактный базовый класс для фабрик блокировок.
    
    Определяет интерфейс для создания объектов блокировок.
    """
    
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
        """
        Создает объект блокировки.
        
        Args:
            connection: Соединение с БД
            resource: Имя блокируемого ресурса
            block: Блокирующий режим
            timeout: Таймаут в секундах
            lock_type: Тип блокировки (EXCLUSIVE или SHARED)
            scope: Область действия (TRANSACTION или SESSION)
            
        Returns:
            Lock: Объект блокировки
        """
        ...


class Lock(ABC):
    """
    Абстрактный базовый класс для блокировок.
    
    Определяет интерфейс для работы с блокировками через контекстный менеджер.
    """
    
    resource: str
    timeout: int

    @abstractmethod
    def __enter__(self):
        """
        Получает блокировку.
        
        Raises:
            ResourceIsLocked: Если ресурс заблокирован и timeout истек
        """
        ...

    @abstractmethod
    def __exit__(self, *exc):
        """
        Освобождает блокировку.
        """
        ...
