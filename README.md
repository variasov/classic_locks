# Classic Locks

Этот пакет предоставляет функциональность рекомендательных блокировок.
Является частью проекта "Classic".

Пакет предоставляет абстрактные классы для реализации рекомендальных 
блокировок необходимых типов, а также реализации для баз данных 
PostgreSQL, MSSQL и файлов.

Основным компонентом пакета является декортор locking, 
который устанавливает рекомендательную блокировку на заданный ресурс.

Простой пример использования декоратора:

```python
from classic.components import component
from classic.locks import locking, PGAdvisoryLocker


@component
class SomeCls:
    @locking('resource_1')
    def action_1(self):
        ...
    
    # использование в виде контекстного менаджера
    def action_2(self):
        with self.locker.acquire('resource_2'):
            ...


locker = PGAdvisoryLocker(session=session)
some_cls = SomeCls(locker=locker)
some_cls.action_1()
```

Также декторатор может принимать следующие опциональные параметры:
- lock_type=EXCLUSIVE, тип блокировки. EXCLUSIVE или SHARED
- timeout=None, кол-во секунд ожидания блокировки. По умолчанию, если блокировка 
не доступна, то выбрасывается исключение без ожидания.
- attr='locker', имя атрибута, в который будет помещен инстанс Locker.

```python
from classic.components import component
from classic.locks import SHARED, locking, PGAdvisoryLocker


@component
class SomeCls:
    @locking('res_{number}', SHARED, 10, attr='locker_1')
    def action(self, number):
        ...

locker = PGAdvisoryLocker(session=session)
some_cls = SomeCls(locker_1=locker)
some_cls.action(number=1)
```
