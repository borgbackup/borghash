BorgHash
=========

Memory-efficient hashtable implementations as a Python library,
implemented in Cython.

HashTable
---------

``HashTable`` is a rather low-level implementation, usually one rather wants to
use the ``HashTableNT`` wrapper. But read on to get the basics...

Keys and Values
~~~~~~~~~~~~~~~

The keys MUST be perfectly random ``bytes`` of arbitrary, but constant length,
like from a cryptographic hash (sha256, hmac-sha256, ...).
The implementation relies on this "perfectly random" property and does not
implement an own hash function, but just takes 32 bits from the given key.

The values are binary ``bytes`` of arbitrary, but constant length.

The length of the keys and values is defined when creating a ``HashTable``
instance (after that, the length must always match that defined length).

Implementation details
~~~~~~~~~~~~~~~~~~~~~~

To have little memory overhead overall, the hashtable only stores uint32_t
indexes into separate keys and values arrays (short: kv arrays).

A new key just gets appended to the keys array. The corresponding value gets
appended to the values array. After that, the key and value do not change their
index as long as they exist in the hashtable and the ht and kv arrays are in
memory. Even when kv pairs are deleted from ``HashTable``, the kv arrays never
shrink and the indexes of other kv pairs don't change.

This is because we want to have stable array indexes for the keys/values so the
indexes can be used outside of ``HashTable`` as memory-efficient references.

Memory allocated
~~~~~~~~~~~~~~~~

For a hashtable load factor of 0.1 - 0.5, a kv array grow factor of 1.3 and
N kv pairs, memory usage in bytes is approximately:

- Hashtable: from ``N * 4 / 0.5`` to ``N * 4 / 0.1``
- Keys/Values: from ``N * len(key+value) * 1.0`` to ``N * len(key+value) * 1.3``
- Overall: from ``N * (8 + len(key+value))`` to ``N * (40 + len(key+value) * 1.3)``

When the hashtable or the kv arrays are resized, there will be short memory
usage spikes. For the kv arrays, ``realloc()`` is used to avoid copying of
data and memory usage spikes, if possible.

HashTableNT
-----------

``HashTableNT`` is a convenience wrapper around ``HashTable``:

- accepts and returns ``namedtuple`` values
- implements persistence: can read (write) the hashtable from (to) a file.

Keys and Values
~~~~~~~~~~~~~~~

Keys: ``bytes``, see ``HashTable``.

Values: any fixed type of ``namedtuple`` that can be serialized to ``bytes``
by Python's ``struct`` module using a given format string.

When setting a value, it is automatically serialized. When a value is returned,
it will be a ``namedtuple`` of the given type.

Persistence
~~~~~~~~~~~

``HashTableNT`` has ``.write()`` and ``.read()`` methods to save/load its
content to/from a file, using an efficient binary format.

When a ``HashTableNT`` is saved to disk, only the non-deleted entries are
persisted and when it is loaded from disk, a new hashtable and new, dense
kv arrays are built - thus, kv indexes will be different!

API
---

HashTable / HashTableNT have an API similar to a dict:

- ``__setitem__`` / ``__getitem__`` / ``__delitem__`` / ``__contains__``
- ``get()``, ``pop()``, ``setdefault()``
- ``items()``, ``len()``
- ``read()``, ``write()``, ``size()``

Example code
------------

::

    # HashTableNT mapping 256bit key [bytes] --> Chunk value [namedtuple]
    Chunk = namedtuple("Chunk", ["refcount", "size"])
    ChunkFormat = namedtuple("ChunkFormat", ["refcount", "size"])
    chunk_format = ChunkFormat(refcount="I", size="I")

    # 256bit (32Byte) key, 2x 32bit (4Byte) values
    ht = HashTableNT(key_size=32, value_type=Chunk, value_format=chunk_format)

    key = b"x" * 32  # the key is usually from a cryptographic hash fn
    value = Chunk(refcount=1, size=42)
    ht[key] = value
    assert ht[key] == value

    for key, value in ht.items():
        assert isinstance(key, bytes)
        assert isinstance(value, Chunk)

    file = "dump.bin"  # giving an fd of a file opened in binary mode also works
    ht.write(file)
    ht = HashTableNT.read(file)

Building / Installing
---------------------
::

    python setup.py build_ext --inplace
    python -m build
    pip install dist/borghash*.tar.gz


Want a demo?
------------

Run ``borghash-demo`` after installing the ``borghash`` package.

It will show you the demo code, run it and print the results for your machine.

Results on an Apple MacBook Pro (M3 Pro CPU) are like:

::

    HashTableNT in-memory ops (count=50000): insert: 0.062s, lookup: 0.066s, pop: 0.061s.
    HashTableNT serialization (count=50000): write: 0.020s, read: 0.021s.


State of this project
---------------------

**API is still unstable and expected to change as development goes on.**

**As long as the API is unstable, there will be no data migration tools,
like e.g. for reading an existing serialized hashtable.**

There might be missing features or optimization potential, feedback welcome!

Borg?
-----

Please note that this code is currently **not** used by the stable release of
BorgBackup (aka "borg"), but might be used by borg master branch in the future.

License
-------

BSD license.
