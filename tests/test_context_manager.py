from typing import Any, Callable
from unittest.mock import MagicMock, Mock

import pytest
from classic.components import component
from classic.db_utils import takes_connection

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


@pytest.fixture
def mock_connect():
    connect = MagicMock()
    connect.return_value.__enter__.return_value = 'test_connection'
    connect.return_value.__exit__.return_value = None
    return connect


@component
class SomeClass:
    locker: AcquireLock
    connect: Callable[[], Any]

    @takes_connection
    @locking('test-{id}')
    def some_method(self, connection, id: int) -> str:
        return f'result-{id}'

    @takes_connection
    @locking(
        'shared-{id}',
        lock_type=SHARED,
        scope=SESSION,
        block=False,
        timeout=10
    )
    def shared_method(self, connection, id: int) -> str:
        return f'shared-{id}'

    @takes_connection(connection_param='custom_conn')
    @locking(
        'custom-{id}',
        connection_param='custom_conn',
        attr='custom_locker',
    )
    def custom_attr_method(self, custom_conn, id: int) -> str:
        return f'custom-{id}'


@pytest.fixture
def test_class_obj(mock_locker, mock_connect):
    return SomeClass(
        connect=mock_connect,
        locker=mock_locker,
        custom_locker=mock_locker,
    )


def test_method_calls_locker_with_formatted_resource(test_class_obj):
    """Проверяет, что ресурс форматируется с использованием параметров метода"""
    test_class_obj.some_method(id=123)

    test_class_obj.locker.assert_called_once_with(
        'test_connection',
        'test-123',
        True,  # block
        None,  # timeout
        EXCLUSIVE,  # lock_type
        TRANSACTION,  # scope
    )


def test_method_returns_original_result(test_class_obj):
    """Проверяет, что декоратор возвращает результат оригинального метода"""
    result = test_class_obj.some_method(id=123)
    assert result == 'result-123'


def test_method_with_custom_parameters(test_class_obj):
    """Проверяет, что параметры блокировки передаются корректно"""
    test_class_obj.shared_method(id=456)

    test_class_obj.locker.assert_called_once_with(
        'test_connection',
        'shared-456',
        False,  # block
        10,  # timeout
        SHARED,  # lock_type
        SESSION,  # scope
    )


def test_method_with_custom_attr(test_class_obj, mock_locker):
    """Проверяет работу с кастомным атрибутом для локера"""
    test_class_obj.custom_attr_method(id=789)

    mock_locker.assert_called_once_with(
        'test_connection',
        'custom-789',
        True,  # block
        None,  # timeout
        EXCLUSIVE,  # lock_type
        TRANSACTION,  # scope
    )


def test_raises_if_locker_not_found(test_class_obj):
    """Проверяет, что возникает ошибка если локер не найден"""
    delattr(test_class_obj, 'locker')

    with pytest.raises(AttributeError):
        test_class_obj.some_method(id=123)


def test_propagates_lock_error(test_class_obj):
    """Проверяет, что ошибки блокировки пробрасываются наверх"""
    test_class_obj.locker.return_value.__enter__.side_effect = (
        ResourceIsLocked('test-123')
    )

    with pytest.raises(ResourceIsLocked):
        test_class_obj.some_method(id=123)
