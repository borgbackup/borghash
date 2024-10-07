BorgHash
=========

A hashtable implementation as a Python library.

Implemented in Cython (most code) and a tiny bit of C++.

Keys
----

The hashtable keys MUST be perfectly random 256bit values,
like from a cryptographic hash (sha256, hmac-sha256, ...).

The implementation relies on this property and does not implement
an own hash function, but just takes bits from the given key.

Values
------

The hashtable values are binary bytes of arbitrary, but constant length.
The length is defined when creating a hashtable instance (after that, the
length of stored values must always match that defined length).

Users can implement their own wrappers to pack/unpack whatever they need
into these binary bytes, e.g. Python stdlib ``struct`` module.

BorgHash Operations
-------------------

- add / remove / lookup
- iteritems
- len()
- save / load

Scalability
-----------

- Memory is used very efficiently: (add formula)
- Serialization uses msgpack, which is an efficient binary format.

Want a demo?
------------

Run this to get instructions how to run the demo:

python3 -m borghash

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

