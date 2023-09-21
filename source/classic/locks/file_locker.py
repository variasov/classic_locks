import fcntl
import os
import threading
import time
from typing import Optional

from . import errors
from .locker import EXCLUSIVE, SHARED, Lock, Locker, LockType


class FileLocker(Locker):
    extension = '.lock'

    def __init__(
        self, path: str, timeout_delay: float = 0.5, cleaner_delay: float = 60
    ):
        assert os.path.isabs(path), 'Please, use absolute path!'
        self.path = path

        self.timeout_delay = timeout_delay
        self.cleaner_delay = cleaner_delay

    def acquire(
        self,
        resource: str,
        lock_type: LockType = EXCLUSIVE,
        timeout: int = None,
    ) -> Lock:
        return FileLock(
            file=self.path + resource + self.extension,
            lock_fn=fcntl.LOCK_SH if lock_type == SHARED else fcntl.LOCK_EX,
            lock_type=lock_type,
            resource=resource,
            timeout=timeout,
            timeout_delay=self.timeout_delay,
        )

    def __remove_files(self):
        files_to_remove = [
            os.path.join(self.path, f_name)
            for f_name in os.listdir(self.path)
            if f_name.endswith(self.extension)
        ]

        for file in files_to_remove:
            try:
                fd = open(file, 'w+')
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                os.remove(file)
            except OSError:
                pass

    def __loop(self):
        while True:
            self.__remove_files()
            time.sleep(self.cleaner_delay)

    def run_cleaner(self, background: bool = False):
        if background:
            threading.Thread(target=self.__loop).start()
        else:
            self.__remove_files()


class FileLock(Lock):
    def __init__(
        self,
        file: str,
        lock_fn: int,
        lock_type: LockType,
        resource: str,
        timeout: Optional[int] = None,
        timeout_delay=0.5,
    ):
        self.file = file
        self.lock_fn = lock_fn
        self.lock_type = lock_type
        self.resource = resource
        self.timeout = timeout
        self.timeout_delay = timeout_delay

        self.mode = 'w+'
        self.fd = None

    def __enter__(self):
        start_time = time.monotonic()
        self.fd = open(self.file, self.mode)

        while True:
            try:
                fcntl.flock(self.fd, self.lock_fn | fcntl.LOCK_NB)
                break
            except OSError:
                if not self.timeout or (time.monotonic() - start_time >
                                        self.timeout):
                    raise errors.ResourceIsLocked(resource=self.resource)

                time.sleep(self.timeout_delay)

    def __exit__(self, *exc):
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        self.fd.close()
