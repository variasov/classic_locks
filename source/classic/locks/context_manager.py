from functools import wraps
from typing import Any

from classic.components.extra_annotations import add_extra_annotation
from classic.components.types import Function

from .lock import EXCLUSIVE, TRANSACTION, AcquireLock, LockType, ScopeType


def locking(
    resource: str,
    block: bool = True,
    timeout: int = None,
    lock_type: LockType = EXCLUSIVE,
    scope: ScopeType = TRANSACTION,
    attr: str = 'locker',
):
    """
    Декоратор для автоматической блокировки ресурса при вызове метода.
    
    Args:
        resource: Шаблон имени ресурса (может содержать {параметры})
        block: Блокирующий режим
        timeout: Таймаут в секундах
        lock_type: Тип блокировки (EXCLUSIVE или SHARED)
        scope: Область действия (TRANSACTION или SESSION)
        attr: Имя атрибута объекта, содержащего фабрику блокировок
        
    Returns:
        Function: Декорированная функция
        
    Example:
        @locking('user:{user_id}')
        def update_user(self, user_id: int):
            ...
    """
    def decorate(function: Function) -> Function:
        @wraps(function)
        def wrapper(obj: object, *args: Any, **kwargs: Any) -> Any:
            locker = getattr(obj, attr)
            with locker(
                resource.format(**kwargs),
                block,
                timeout,
                lock_type,
                scope,
            ):
                return function(obj, *args, **kwargs)

        return add_extra_annotation(wrapper, attr, AcquireLock)

    return decorate
