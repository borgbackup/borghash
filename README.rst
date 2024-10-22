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
indexes into separate keys and values arrays.

A new key just gets appended to the keys array. The corresponding value gets
appended to the values array. After that, the key and value do not change their
index as long as they exist in the hashtable and the ht and k/v arrays are in
memory. Even when k/v pairs are deleted from HashTable, the k/v arrays never
shrink and the indexes of other k/v pairs don't change.

This is because we want to have stable array indexes for the keys/values so the
indexes can be used outside of ``HashTable`` as memory-efficient references.

Memory allocated
~~~~~~~~~~~~~~~~

For a hashtable load factor of 0.1 - 0.5, a kv array grow factor of 1.3 and N
key/value pairs, memory usage in bytes is approximately:

- Hashtable: from ``N * 4 / 0.5`` to ``N * 4 / 0.1``
- Keys: from ``N * len(key) * 1.0`` to ``N * len(key) * 1.3``
- Values: from ``N * len(value) * 1.0`` to ``N * len(value) * 1.3``

- Overall maximum: ``N * (40 + len(key + value) * 1.3)``
- Overall minimum: ``N * (8 + len(key + value))``

When the hashtable or the keys/values arrays are resized, there will be short
memory usage spikes. For the k/v arrays, ``realloc()`` is used for efficiency
and to avoid copying of data and memory usage spikes, if possible.

HashTableNT
-----------

A convenience wrapper around HashTable, providing namedtuple values and
persistence.

Keys and Values
~~~~~~~~~~~~~~~

Keys: ``bytes``, see ``HashTable``.

Values: any fixed type of ``namedtuple`` that can be serialized to bytes by
Python's ``struct`` module using a given format string. When setting a
value, it is automatically serialized. When values are returned, they will
be namedtuples of the given type.

Persistence
~~~~~~~~~~~

``.write()`` and ``.read()`` methods are provided to save/load the whole
``HashTableNT`` to/from a file, using ``msggpack`` as an efficient binary
format.

When a ``HashTableNT`` is saved to disk, only the non-deleted entries are
persisted and when it is loaded from disk, a new hashtable and new, dense
arrays are built for these keys/values. Thus, k/v indexes will be different!

API
---

HashTable / HashTableNT have an API similar to a dict:

- ``__setitem__`` / ``__getitem__`` / ``__delitem__`` / ``__contains__``
- ``get()``, ``pop()``, ``setdefault()``
- ``iteritems()``, ``len()``
- ``read()``, ``write()``, ``size()``

Want a demo?
------------

Run this to get instructions how to run the demo:

``python3 -m borghash``

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
