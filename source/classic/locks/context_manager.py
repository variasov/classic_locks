from functools import wraps
from typing import Any

from classic.components.extra_annotations import add_extra_annotation
from classic.components.types import Function, Object

from .locker import EXCLUSIVE, Locker, LockType


def locking(
    resource: str,
    lock_type: LockType = EXCLUSIVE,
    timeout: int = None,
    attr: str = 'locker',
):
    def decorate(function: Function) -> Function:
        @wraps(function)
        def wrapper(obj: Object, *args: Any, **kwargs: Any) -> Any:
            locker = getattr(obj, attr)
            with locker.acquire(resource.format(**kwargs), lock_type, timeout):
                return function(obj, *args, **kwargs)

        return add_extra_annotation(wrapper, attr, Locker)

    return decorate
