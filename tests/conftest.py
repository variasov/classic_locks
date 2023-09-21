import pytest

from classic.components import component
from classic.locks import SHARED, locking


@component
class SomeCls:
    @locking('res_{number}', SHARED, attr='locker_1')
    def read(self, number):
        return number

    def write(self, number):
        with self.locker_1.acquire(f'res_{number}'):
            return number


@pytest.fixture(scope='session')
def some_csl():
    return SomeCls


@pytest.fixture(scope='session')
def resource_1():
    return '1111'


@pytest.fixture(scope='session')
def resource_2():
    return '2222'


@pytest.fixture(scope='session')
def resource_1_locker(resource_1):
    return f'res_{resource_1}'


@pytest.fixture(scope='session')
def resource_2_locker(resource_2):
    return f'res_{resource_2}'
