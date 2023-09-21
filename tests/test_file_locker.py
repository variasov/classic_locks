import sys

import pytest

from classic.locks import EXCLUSIVE, SHARED, FileLocker

from .common import *

path = '/home/pavel/lock_files/'


@pytest.fixture
def locker_1():
    return FileLocker(path=path)


@pytest.fixture
def obj(some_csl, locker_1):
    return some_csl(locker_1=locker_1)


@pytest.fixture(scope='function')
def excl_lock(resource_1_locker):
    return FileLocker(path=path).acquire(resource_1_locker, EXCLUSIVE)


@pytest.fixture(scope='function')
def excl_lock_2(resource_2_locker):
    return FileLocker(path=path).acquire(resource_2_locker, EXCLUSIVE)


@pytest.fixture(scope='function')
def shared_lock(resource_1_locker):
    return FileLocker(path=path).acquire(resource_1_locker, SHARED)


@pytest.fixture(scope='function', autouse=True)
def check_os():
    if sys.platform != 'linux':
        pytest.skip('только для linux os')


@pytest.fixture(scope='function', autouse=True)
def check_os(locker_1):
    yield
    locker_1.run_cleaner()
