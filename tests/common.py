import pytest

from classic.locks import errors


def test__excl_lock_write_diff_resource(obj, excl_lock_2, resource_1):
    with excl_lock_2:
        obj.write(number=resource_1)


def test__excl_lock_error_write(obj, excl_lock, resource_1):
    with excl_lock, pytest.raises(errors.ResourceIsLocked):
        obj.write(number=resource_1)


def test__excl_lock_error_read(obj, excl_lock, resource_1):
    with excl_lock, pytest.raises(errors.ResourceIsLocked):
        obj.read(number=resource_1)


def test__shared_lock_error_write(obj, shared_lock, resource_1):
    with shared_lock, pytest.raises(errors.ResourceIsLocked):
        obj.write(number=resource_1)


def test__shared_lock_read(obj, shared_lock, resource_1):
    with shared_lock:
        obj.read(number=resource_1)
