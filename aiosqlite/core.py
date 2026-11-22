# Copyright Amethyst Reese
# Licensed under the MIT license

"""
Core implementation of aiosqlite proxies
"""

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator, Generator, Iterable
from functools import partial
from pathlib import Path
from queue import Empty, Queue, SimpleQueue
from threading import Thread
from typing import Any, Callable, Literal, Optional, Union
from warnings import warn

from .context import contextmanager
from .cursor import Cursor

__all__ = ["connect", "Connection", "Cursor"]

AuthorizerCallback = Callable[[int, str, str, str, str], int]

LOG = logging.getLogger("aiosqlite")


IsolationLevel = Optional[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]


def set_result(fut: asyncio.Future, result: Any) -> None:
    """Set the result of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_result(result)


def set_exception(fut: asyncio.Future, e: BaseException) -> None:
    """Set the exception of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_exception(e)


_STOP_RUNNING_SENTINEL = object()

# Interval (in seconds) at which the worker thread re-checks whether the task
# holding the transaction lease has finished, while other operations are parked.
# This is only a backstop; leases are normally released immediately via a
# done-callback on the owning task.
_LEASE_CHECK_INTERVAL = 0.1

# Result of every function run on the worker thread:
# (result, error, in_transaction)
_TxResult = tuple[Any, Optional[BaseException], bool]

# Queue item: (future to fulfill, wrapped function, owning asyncio task or None)
_TxItem = tuple[Optional[asyncio.Future], Callable[[], _TxResult], Any]
_TxQueue = SimpleQueue[_TxItem]


def _release_lease() -> _TxResult:
    """
    No-op queued when a lease-holding task finishes.

    Reports ``in_transaction=False`` so the worker drops the finished task's
    lease, letting parked operations from other tasks proceed. Any transaction
    left open is deliberately kept; it remains usable (and committable) by
    whichever task runs next.
    """
    return None, None, False


def _connection_worker_thread(tx: _TxQueue) -> None:
    """
    Execute function calls on a separate thread.

    Enforces per-task transaction isolation on the shared connection: once a
    task's operation leaves the connection inside a transaction, that task owns
    the connection. Operations queued by other tasks are parked until the owner
    commits, rolls back, or finishes. This prevents one task's statements from
    silently joining (and being rolled back with) another task's transaction.

    :meta private:
    """
    owner: Any = None
    parked: list[_TxItem] = []

    def run(item: _TxItem) -> tuple[Any, bool]:
        future, function, _ = item
        LOG.debug("executing %s", function)
        result, error, in_transaction = function()
        if future is not None:
            if error is not None:
                LOG.debug("returning exception %s", error)
                future.get_loop().call_soon_threadsafe(set_exception, future, error)
            else:
                LOG.debug("operation %s completed", function)
                future.get_loop().call_soon_threadsafe(set_result, future, result)
        return result, in_transaction

    def update_lease(tag: Any, in_transaction: bool) -> None:
        nonlocal owner
        if tag is None:
            return
        if in_transaction:
            if owner is None:
                owner = tag
                arm_release_callback(tag)
        elif owner is tag:
            owner = None

    def arm_release_callback(task: "asyncio.Task[Any]") -> None:
        def release(done_task: "asyncio.Task[Any]") -> None:
            tx.put_nowait((None, _release_lease, done_task))

        try:
            task.get_loop().call_soon_threadsafe(task.add_done_callback, release)
        except Exception:
            # Owner's loop is gone; the periodic check below will release it.
            pass

    def drain_parked() -> None:
        remaining = []
        for pending in parked:
            ptag = pending[2]
            if ptag is not None and owner is not None and ptag is not owner:
                remaining.append(pending)
                continue
            _, in_transaction = run(pending)
            update_lease(ptag, in_transaction)
        parked[:] = remaining

    while True:
        # Continues running until all queue items are processed,
        # even after connection is closed (so we can finalize all
        # futures)

        if parked and owner is not None:
            try:
                item: Optional[_TxItem] = tx.get(timeout=_LEASE_CHECK_INTERVAL)
            except Empty:
                item = None
        else:
            item = tx.get()

        # A finished owner can no longer commit or roll back; drop its lease
        # so parked operations are not blocked forever.
        if owner is not None and owner.done():
            owner = None

        if item is None:
            if owner is None:
                drain_parked()
            continue

        tag = item[2]
        if tag is not None and owner is not None and tag is not owner:
            parked.append(item)
            continue

        result, in_transaction = run(item)
        update_lease(tag, in_transaction)

        if result is _STOP_RUNNING_SENTINEL:
            for parked_future, _, _ in parked:
                if parked_future is not None:
                    try:
                        parked_future.get_loop().call_soon_threadsafe(
                            set_exception,
                            parked_future,
                            ValueError("Connection closed"),
                        )
                    except Exception:
                        pass
            break

        # drop references to the queued closure and its result so neither the
        # connection nor query results are kept alive by this thread
        item = None
        result = None

        if owner is None:
            drain_parked()


class Connection:
    def __init__(
        self,
        connector: Callable[[], sqlite3.Connection],
        iter_chunk_size: int,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self._running = True
        self._connection: Optional[sqlite3.Connection] = None
        self._connector = connector
        self._tx: _TxQueue = SimpleQueue()
        self._iter_chunk_size = iter_chunk_size
        self._thread = Thread(target=_connection_worker_thread, args=(self._tx,))

        if loop is not None:
            warn(
                "aiosqlite.Connection no longer uses the `loop` parameter",
                DeprecationWarning,
            )

    def __del__(self):
        if self._connection is None:
            return

        warn(
            (
                f"{self!r} was deleted before being closed. "
                "Please use 'async with' or '.close()' to close the connection properly."
            ),
            ResourceWarning,
            stacklevel=1,
        )

        # Don't try to be creative here, the event loop may have already been closed.
        # Simply stop the worker thread, and let the underlying sqlite3 connection
        # be finalized by its own __del__.
        self.stop()

    def stop(self) -> Optional[asyncio.Future]:
        """Stop the background thread. Prefer `async with` or `await close()`"""
        self._running = False

        def close_and_stop():
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            return _STOP_RUNNING_SENTINEL

        try:
            future = asyncio.get_event_loop().create_future()
        except Exception:
            future = None

        self._tx.put_nowait((future, self._wrap(close_and_stop), None))
        return future

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._connection is None:
            raise ValueError("no active connection")

        return self._connection

    def _execute_insert(self, sql: str, parameters: Any) -> Optional[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        cursor.execute("SELECT last_insert_rowid()")
        return cursor.fetchone()

    def _execute_fetchall(self, sql: str, parameters: Any) -> Iterable[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchall()

    def _wrap(self, function: Callable[[], Any]) -> Callable[[], _TxResult]:
        """
        Wrap a function so the worker thread gets a uniform result tuple of
        (result, error, in_transaction), capturing exceptions and reporting
        whether the connection is inside a transaction afterwards.
        """

        def run() -> _TxResult:
            error: Optional[BaseException] = None
            try:
                result: Any = function()
            except BaseException as e:  # noqa B036
                result = None
                error = e

            try:
                conn = self._connection
                in_transaction = (
                    bool(conn.in_transaction) if conn is not None else False
                )
            except Exception:
                in_transaction = False

            return result, error, in_transaction

        return run

    async def _execute(
        self,
        fn,
        *args,
        _bypass_lease: bool = False,
        _task: Optional["asyncio.Task[Any]"] = None,
        **kwargs,
    ):
        """
        Queue a function with the given arguments for execution.

        Each call is tagged with the current asyncio task. While a task holds
        the transaction lease (it has an open transaction), calls from other
        tasks are parked until the lease is released, so concurrent
        transactions on this shared connection stay isolated from each other.
        """
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()

        if _bypass_lease:
            tag = None
        elif _task is not None:
            tag = _task
        else:
            tag = asyncio.current_task()
        self._tx.put_nowait((future, self._wrap(function), tag))

        return await future

    async def _connect(self) -> "Connection":
        """Connect to the actual sqlite database."""
        if self._connection is None:
            try:
                future = asyncio.get_event_loop().create_future()
                self._tx.put_nowait((future, self._wrap(self._connector), None))
                self._connection = await future
            except BaseException:
                self.stop()
                self._connection = None
                raise

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        self._thread.start()
        return self._connect().__await__()

    async def __aenter__(self) -> "Connection":
        return await self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @contextmanager
    async def cursor(self) -> Cursor:
        """Create an aiosqlite cursor wrapping a sqlite3 cursor object."""
        return Cursor(self, await self._execute(self._conn.cursor))

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._execute(self._conn.rollback)

    async def close(self) -> None:
        """Complete queued queries/cursors and close the connection."""

        if self._connection is None:
            return

        try:
            await self._execute(self._conn.close, _bypass_lease=True)
        except Exception:
            LOG.info("exception occurred while closing connection")
            raise
        finally:
            self._connection = None
            future = self.stop()
            if future:
                await future

    @contextmanager
    async def execute(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Cursor:
        """Helper to create a cursor and execute the given query."""
        if parameters is None:
            parameters = []
        cursor = await self._execute(self._conn.execute, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def execute_insert(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Optional[sqlite3.Row]:
        """Helper to insert and get the last_insert_rowid."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_insert, sql, parameters)

    @contextmanager
    async def execute_fetchall(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Iterable[sqlite3.Row]:
        """Helper to execute a query and return all the data."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_fetchall, sql, parameters)

    @contextmanager
    async def executemany(
        self, sql: str, parameters: Iterable[Iterable[Any]]
    ) -> Cursor:
        """Helper to create a cursor and execute the given multiquery."""
        cursor = await self._execute(self._conn.executemany, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(self, sql_script: str) -> Cursor:
        """Helper to create a cursor and execute a user script."""
        cursor = await self._execute(self._conn.executescript, sql_script)
        return Cursor(self, cursor)

    async def interrupt(self) -> None:
        """Interrupt pending queries."""
        return self._conn.interrupt()

    async def create_function(
        self, name: str, num_params: int, func: Callable, deterministic: bool = False
    ) -> None:
        """
        Create user-defined function that can be later used
        within SQL statements. Must be run within the same thread
        that query executions take place so instead of executing directly
        against the connection, we defer this to `run` function.

        If ``deterministic`` is true, the created function is marked as deterministic,
        which allows SQLite to perform additional optimizations. This flag is supported
        by SQLite 3.8.3 or higher, ``NotSupportedError`` will be raised if used with
        older versions.
        """
        await self._execute(
            self._conn.create_function,
            name,
            num_params,
            func,
            deterministic=deterministic,
        )

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def isolation_level(self) -> Optional[str]:
        return self._conn.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel) -> None:
        self._conn.isolation_level = value

    @property
    def row_factory(self) -> Optional[type]:
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, factory: Optional[type]) -> None:
        self._conn.row_factory = factory

    @property
    def text_factory(self) -> Callable[[bytes], Any]:
        return self._conn.text_factory

    @text_factory.setter
    def text_factory(self, factory: Callable[[bytes], Any]) -> None:
        self._conn.text_factory = factory

    @property
    def total_changes(self) -> int:
        return self._conn.total_changes

    async def enable_load_extension(self, value: bool) -> None:
        await self._execute(self._conn.enable_load_extension, value)  # type: ignore

    async def load_extension(self, path: str):
        await self._execute(self._conn.load_extension, path)  # type: ignore

    async def set_progress_handler(
        self, handler: Callable[[], Optional[int]], n: int
    ) -> None:
        await self._execute(self._conn.set_progress_handler, handler, n)

    async def set_trace_callback(self, handler: Callable) -> None:
        await self._execute(self._conn.set_trace_callback, handler)

    async def set_authorizer(
        self, authorizer_callback: Optional[AuthorizerCallback]
    ) -> None:
        """
        Set an authorizer callback to control database access.

        The authorizer callback is invoked for each SQL statement that is prepared,
        and controls whether specific operations are permitted.

        Example::

            import sqlite3

            def restrict_drops(action_code, arg1, arg2, db_name, trigger_name):
                # Deny all DROP operations
                if action_code == sqlite3.SQLITE_DROP_TABLE:
                    return sqlite3.SQLITE_DENY
                # Allow everything else
                return sqlite3.SQLITE_OK

            await conn.set_authorizer(restrict_drops)

        See ``sqlite3`` documentation for details:
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.set_authorizer

        :param authorizer_callback: An optional callable that receives five arguments:

            - ``action_code`` (int): The action to be authorized (e.g., ``SQLITE_READ``)
            - ``arg1`` (str): First argument, meaning depends on ``action_code``
            - ``arg2`` (str): Second argument, meaning depends on ``action_code``
            - ``db_name`` (str): Database name (e.g., ``"main"``, ``"temp"``)
            - ``trigger_name`` (str): Name of trigger or view that is doing the access,
              or ``None``

            The callback should return:

            - ``SQLITE_OK`` (0): Allow the operation
            - ``SQLITE_DENY`` (1): Deny the operation, raise ``sqlite3.DatabaseError``
            - ``SQLITE_IGNORE`` (2): Treat operation as no-op

            Pass ``None`` to remove the authorizer.
        """
        await self._execute(self._conn.set_authorizer, authorizer_callback)

    async def iterdump(self) -> AsyncIterator[str]:
        """
        Return an async iterator to dump the database in SQL text format.

        Example::

            async for line in db.iterdump():
                ...

        """
        dump_queue: Queue = Queue()
        caller_task = asyncio.current_task()

        def dumper():
            try:
                for line in self._conn.iterdump():
                    dump_queue.put_nowait(line)
                dump_queue.put_nowait(None)

            except Exception:
                LOG.exception("exception while dumping db")
                dump_queue.put_nowait(None)
                raise

        # the helper task inherits this task's identity so it is not blocked
        # by our own transaction lease
        fut = self._execute(dumper, _task=caller_task)
        task = asyncio.ensure_future(fut)

        while True:
            try:
                line: Optional[str] = dump_queue.get_nowait()
                if line is None:
                    break
                yield line

            except Empty:
                if task.done():
                    LOG.warning("iterdump completed unexpectedly")
                    break

                await asyncio.sleep(0.01)

        await task

    async def backup(
        self,
        target: Union["Connection", sqlite3.Connection],
        *,
        pages: int = 0,
        progress: Optional[Callable[[int, int, int], None]] = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        """
        Make a backup of the current database to the target database.

        Takes either a standard sqlite3 or aiosqlite Connection object as the target.
        """
        if isinstance(target, Connection):
            target = target._conn

        await self._execute(
            self._conn.backup,
            target,
            pages=pages,
            progress=progress,
            name=name,
            sleep=sleep,
        )


def connect(
    database: Union[str, Path],
    *,
    iter_chunk_size=64,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs: Any,
) -> Connection:
    """Create and return a connection proxy to the sqlite database."""

    if loop is not None:
        warn(
            "aiosqlite.connect() no longer uses the `loop` parameter",
            DeprecationWarning,
        )

    def connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode("utf-8")
        else:
            loc = str(database)

        return sqlite3.connect(loc, **kwargs)

    return Connection(connector, iter_chunk_size)
