# Classic Locks

Этот пакет предоставляет функциональность рекомендательных блокировок.
Является частью проекта "Classic".

Пакет предоставляет абстрактные классы для реализации рекомендательных 
блокировок необходимых типов, а также реализации для баз данных 
PostgreSQL и MSSQL.

## Установка

```bash
# Базовая установка
pip install classic-locks

# Для работы с PostgreSQL через psycopg2
pip install classic-locks[postgres-psycopg2]

# Для работы с PostgreSQL через psycopg3
pip install classic-locks[postgres-psycopg3]

# Для работы с PostgreSQL через SQLAlchemy
pip install classic-locks[postgres-sqlalchemy]

# Для работы с MSSQL через pymssql
pip install classic-locks[mssql-pymssql]

# Для работы с MSSQL через SQLAlchemy
pip install classic-locks[mssql-sqlalchemy]
```

## Использование

### Декоратор @locking

Основным компонентом пакета является декоратор `@locking`, который устанавливает 
рекомендательную блокировку на заданный ресурс.

Для автоматического управления соединениями требуется использовать декоратор 
`@takes_connection` из библиотеки [classic-db-utils](https://pypi.org/project/classic-db-utils/).

```python
from typing import Any, Callable

from classic.db_utils import takes_connection
from classic.components import component
from classic.locks import (
    locking,
    SHARED,
    SESSION,
    AcquirePsycopg2PGAdvisoryLock,
    AcquirePsycopg3PGAdvisoryLock,
    AcquireSQLAlchemyPGAdvisoryLock,
    AcquirePyMSSQLAdvisoryLock,
    AcquireSQLAlchemyMSAdvisoryLock,
)


@component
class SomeClass:
    locker: AcquirePsycopg2PGAdvisoryLock
    # метод для получения соединения, к примеру, фабрика или пул соединений
    connect: Callable[[], Any]

    @takes_connection
    @locking('resource-{id}')
    def exclusive_lock_method(self, connection, id: int):
        """Метод с эксклюзивной блокировкой уровня транзакции"""
        ...

    @takes_connection(connection_param='custom_conn')
    @locking(
        'shared-resource-{id}',
        connection_param='custom_conn',
        lock_type=SHARED,
        scope=SESSION,
        block=False,
        timeout=10
    )
    def shared_lock_method(self, custom_conn, id: int):
        """Метод с разделяемой блокировкой уровня сессии"""
        ...

    # Использование в виде контекстного менеджера
    def manual_lock_method(self, id: int):
        with self.locker(f'manual-resource-{id}'):
            ...


# PostgreSQL через psycopg2
locker = AcquirePsycopg2PGAdvisoryLock()
some_class = SomeClass(locker=locker)

# PostgreSQL через psycopg3
locker = AcquirePsycopg3PGAdvisoryLock()
some_class = SomeClass(locker=locker)

# PostgreSQL через SQLAlchemy
locker = AcquireSQLAlchemyPGAdvisoryLock()
some_class = SomeClass(locker=locker)

# MSSQL через pymssql
locker = AcquirePyMSSQLAdvisoryLock()
some_class = SomeClass(locker=locker)

# MSSQL через SQLAlchemy
locker = AcquireSQLAlchemyMSAdvisoryLock()
some_class = SomeClass(locker=locker)
```

### Параметры декоратора

- `resource: str` - шаблон имени ресурса. Может содержать параметры в формате Python format string
- `lock_type: LockType = EXCLUSIVE` - тип блокировки:
  - `EXCLUSIVE` - эксклюзивная блокировка
  - `SHARED` - разделяемая блокировка
- `scope: ScopeType = TRANSACTION` - область действия блокировки:
  - `TRANSACTION` - в рамках транзакции
  - `SESSION` - в рамках сессии
- `block: bool = True` - блокирующий режим:
  - `True` - ждать освобождения блокировки
  - `False` - немедленно вернуть ошибку если ресурс заблокирован
- `timeout: int = None` - таймаут ожидания в секундах
- `attr: str = 'locker'` - имя атрибута с инстансом локера

### Особенности реализаций

#### PostgreSQL
- Использует advisory locks
- Поддерживает эксклюзивные и разделяемые блокировки
- Поддерживает блокировки уровня транзакции и сессии
- Доступны реализации для psycopg2, psycopg3 и SQLAlchemy

#### MSSQL
- Использует sp_getapplock
- Поддерживает различные режимы блокировок (Shared, Update, Exclusive и др.)
- Поддерживает блокировки уровня транзакции и сессии
- Доступны реализации для pymssql и SQLAlchemy
