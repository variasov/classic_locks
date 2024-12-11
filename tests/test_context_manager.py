from unittest.mock import MagicMock, Mock

import pytest
from classic.components import component

from classic.locks import (
    EXCLUSIVE,
    SESSION,
    SHARED,
    TRANSACTION,
    AcquireLock,
    ResourceIsLocked,
)
from classic.locks.context_manager import locking


@pytest.fixture
def mock_locker():
    """Фикстура, создающая мок для AcquireLock"""
    mock = Mock(spec=AcquireLock)
    context_manager = MagicMock()
    mock.return_value = context_manager
    return mock


def takes_connection(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        with obj.connect() as conn:
            return func(conn, *args, **kwargs)

    return wrapper


@pytest.fixture
def test_class(mock_locker):
    """Фикстура, создающая тестовый класс с декорированным методом"""

    @component
    class TestClass:
        locker: AcquireLock
        connect: Callable[[], Connection]

        @takes_connection('connect', 'connection')
        @locking('test-{id}')
        def method(self, connection: Connection,  id: int) -> str:
            return f'result-{id}'

        @locking(
            'shared-{id}',
            lock_type=SHARED,
            scope=SESSION,
            block=False,
            timeout=10
        )
        def shared_method(self, id: int) -> str:
            return f'shared-{id}'

        @locking('custom-{id}', attr='custom_locker')
        def custom_attr_method(self, id: int) -> str:
            return f'custom-{id}'

    return TestClass(locker=mock_locker, custom_locker=mock_locker)


def test_method_calls_locker_with_formatted_resource(test_class):
    """Проверяет, что ресурс форматируется с использованием параметров метода"""
    test_class.method(id=123)

    test_class.locker.assert_called_once_with(
        'test-123',
        True,  # block
        None,  # timeout
        EXCLUSIVE,  # lock_type
        TRANSACTION,  # scope
    )


def test_method_returns_original_result(test_class):
    """Проверяет, что декоратор возвращает результат оригинального метода"""
    result = test_class.method(id=123)
    assert result == 'result-123'


def test_method_with_custom_parameters(test_class):
    """Проверяет, что параметры блокировки передаются корректно"""
    test_class.shared_method(id=456)

    test_class.locker.assert_called_once_with(
        'shared-456',
        False,  # block
        10,  # timeout
        SHARED,  # lock_type
        SESSION,  # scope
    )


def test_method_with_custom_attr(test_class, mock_locker):
    """Проверяет работу с кастомным атрибутом для локера"""
    test_class.custom_attr_method(id=789)

    mock_locker.assert_called_once_with(
        'custom-789',
        True,  # block
        None,  # timeout
        EXCLUSIVE,  # lock_type
        TRANSACTION,  # scope
    )


def test_raises_if_locker_not_found(test_class):
    """Проверяет, что возникает ошибка если локер не найден"""
    delattr(test_class, 'locker')

    with pytest.raises(AttributeError):
        test_class.method(id=123)


def test_propagates_lock_error(test_class):
    """Проверяет, что ошибки блокировки пробрасываются наверх"""
    test_class.locker.return_value.__enter__.side_effect = (
        ResourceIsLocked('test-123')
    )

    with pytest.raises(ResourceIsLocked):
        test_class.method(id=123)
