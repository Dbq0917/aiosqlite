aiosqlite\: Sqlite for AsyncIO
==============================

.. image:: https://readthedocs.org/projects/aiosqlite/badge/?version=latest
   :target: https://aiosqlite.omnilib.dev/en/latest/?badge=latest
   :alt: Documentation Status
.. image:: https://img.shields.io/pypi/v/aiosqlite.svg
   :target: https://pypi.org/project/aiosqlite
   :alt: PyPI Release
.. image:: https://img.shields.io/badge/change-log-blue
   :target: https://github.com/omnilib/aiosqlite/blob/master/CHANGELOG.md
   :alt: Changelog
.. image:: https://img.shields.io/pypi/l/aiosqlite.svg
   :target: https://github.com/omnilib/aiosqlite/blob/master/LICENSE
   :alt: MIT Licensed

aiosqlite provides a friendly, async interface to sqlite databases.

It replicates the standard ``sqlite3`` module, but with async versions
of all the standard connection and cursor methods, plus context managers for
automatically closing connections and cursors:

.. code-block:: python

    async with aiosqlite.connect(...) as db:
        await db.execute("INSERT INTO some_table ...")
        await db.commit()

        async with db.execute("SELECT * FROM some_table") as cursor:
            async for row in cursor:
                ...

It can also be used in the traditional, procedural manner:

.. code-block:: python

    db = await aiosqlite.connect(...)
    cursor = await db.execute('SELECT * FROM some_table')
    row = await cursor.fetchone()
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()

aiosqlite also replicates most of the advanced features of ``sqlite3``:

.. code-block:: python

    async with aiosqlite.connect(...) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM some_table') as cursor:
            async for row in cursor:
                value = row['column']

        await db.execute('INSERT INTO foo some_table')
        assert db.total_changes > 0


Install
-------

aiosqlite is compatible with Python 3.8 and newer.
You can install it from PyPI:

.. code-block:: console

    $ pip install aiosqlite


Details
-------

aiosqlite allows interaction with SQLite databases on the main AsyncIO event
loop without blocking execution of other coroutines while waiting for queries
or data fetches.  It does this by using a single, shared thread per connection.
This thread executes all actions within a shared request queue to prevent
overlapping actions.

Connection objects are proxies to the real connections, contain the shared
execution thread, and provide context managers to handle automatically closing
connections.  Cursors are similarly proxies to the real cursors, and provide
async iterators to query results.


Read visibility in WAL mode
---------------------------

When the database runs in WAL mode (``PRAGMA journal_mode=WAL``), a read
connection that keeps a transaction open (for example with ``BEGIN``) pins the
database snapshot taken when that transaction started. Writes committed by
other connections stay invisible to it until the read transaction ends, so a
long-lived reader can serve stale data even though it keeps using the same open
connection.

Connections in autocommit mode (the default, one query per statement) acquire a
fresh snapshot for every statement and always see the latest committed data.
For a long-lived read connection that holds an explicit read transaction, call
``await db.refresh()`` between read cycles to end that transaction and release
the stale snapshot; the connection stays open and reusable, and the next query
observes all commits made elsewhere:

.. code-block:: python

    reader = await aiosqlite.connect("example.db")

    async def latest_rows():
        await reader.refresh()  # release any snapshot held by a prior read
        async with reader.execute("SELECT * FROM events") as cursor:
            return await cursor.fetchall()

``refresh()`` rolls back the active transaction if one exists (discarding
uncommitted changes on that connection) and is a no-op when no transaction is
open. It does not close or reopen the connection.


License
-------

aiosqlite is copyright `Amethyst Reese <https://noswap.com>`_, and licensed under the
MIT license.  I am providing code in this repository to you under an open source
license.  This is my personal repository; the license you receive to my code
is from me and not from my employer. See the `LICENSE`_ file for details.

.. _LICENSE: https://github.com/omnilib/aiosqlite/blob/master/LICENSE
