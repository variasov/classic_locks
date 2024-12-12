import pytest

from classic.locks import (
    EXCLUSIVE,
    SESSION,
    SHARED,
    TRANSACTION,
    ResourceIsLocked,
)

RESOURCE = "test_resource"


def test_exclusive_lock_blocks_another_exclusive(locker_fixture, request):
    """
    Тест проверяет, что эксклюзивная блокировка блокирует другую эксклюзивную
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, lock_type=EXCLUSIVE, block=False):
        with pytest.raises(ResourceIsLocked):
            with locker(conn2, RESOURCE, lock_type=EXCLUSIVE, block=False):
                pass


def test_shared_lock_allows_another_shared(locker_fixture, request):
    """
    Тест проверяет, что разделяемая блокировка позволяет установить
    другую разделяемую
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, lock_type=SHARED, block=False):
        with locker(conn2, RESOURCE, lock_type=SHARED, block=False):
            pass


def test_shared_lock_blocks_exclusive(locker_fixture, request):
    """Тест проверяет, что разделяемая блокировка блокирует эксклюзивную"""
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, lock_type=SHARED, block=False):
        with pytest.raises(ResourceIsLocked):
            with locker(conn2, RESOURCE, lock_type=EXCLUSIVE, block=False):
                pass


def test_timeout_raises_resource_locked(locker_fixture, request):
    """
    Тест проверяет, что по истечении таймаута возникает исключение
    ResourceIsLocked
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, lock_type=EXCLUSIVE, block=False):
        with pytest.raises(ResourceIsLocked):
            with locker(conn2, RESOURCE, block=False, timeout=0.1):
                pass


def test_lock_released_after_context(locker_fixture, request):
    """Тест проверяет, что блокировка освобождается после выхода из контекста"""
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, block=False):
        pass

    conn1.commit()

    with locker(conn2, RESOURCE, block=False):
        pass


def test_transaction_lock_released_after_rollback(locker_fixture, request):
    """
    Тест проверяет, что блокировка уровня TRANSACTION освобождается после
    rollback
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, scope=TRANSACTION, block=False):
        pass

    conn1.rollback()

    with locker(conn2, RESOURCE, block=False):
        pass


def test_session_lock_persists_after_commit(locker_fixture, request):
    """
    Тест проверяет, что блокировка уровня SESSION сохраняется после commit
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, scope=SESSION, block=False):
        conn1.commit()
        
        with pytest.raises(ResourceIsLocked):
            with locker(conn2, RESOURCE, block=False):
                pass


def test_session_lock_persists_after_rollback(locker_fixture, request):
    """
    Тест проверяет, что блокировка уровня SESSION сохраняется после rollback
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, scope=SESSION, block=False):
        conn1.rollback()
        
        with pytest.raises(ResourceIsLocked):
            with locker(conn2, RESOURCE, block=False):
                pass


def test_session_lock_released_after_context(locker_fixture, request):
    """
    Тест проверяет, что блокировка уровня SESSION освобождается после выхода
    из контекста
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, scope=SESSION, block=False):
        pass

    with locker(conn2, RESOURCE, block=False):
        pass


def test_session_lock_released_after_connection_close(locker_fixture, request):
    """
    Тест проверяет, что блокировка уровня SESSION освобождается после закрытия
    соединения
    """
    locker, conn1_fixture, conn2_fixture = locker_fixture
    conn1 = request.getfixturevalue(conn1_fixture)
    conn2 = request.getfixturevalue(conn2_fixture)

    with locker(conn1, RESOURCE, scope=SESSION, block=False):
        pass

    conn1.close()

    with locker(conn2, RESOURCE, block=False):
        pass
