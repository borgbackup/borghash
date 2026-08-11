Changelog
=========

Version 0.2.0 (2026-08-11)
--------------------------

- ``HashTable.items()`` / ``HashTableNT.items()``: add optional ``prefix_bits`` /
  ``prefix`` arguments to iterate over only the items whose keys start with the
  given bit prefix. As the keys are random bytes, this partitions the items into
  ``2 ** prefix_bits`` roughly equally sized, disjoint sets, so a huge hash table
  can be processed in batches with a small memory footprint, #49.
- Require ``key_size >= 4`` to avoid out-of-bounds reads in ``_get_index``, #42.
- Add support for Python 3.15, remove 3.10.
- Cythonize with the latest Cython release.


Version 0.1.1 (2026-02-09)
--------------------------

- Cythonize with the latest Cython release.
- Use the SPDX license identifier, require a recent setuptools.
- Add support for Python 3.14, remove 3.9.
- Migrate tox configuration to pyproject.toml.
- Fix typos and grammar.

Version 0.1.0 2024-11-18
------------------------

- HashTableNT: handle ``byte_order`` separately.
- HashTableNT: provide separate formats in the ``value_format`` namedtuple.

Version 0.0.2 2024-11-10
------------------------

- Fixed "KV array is full" crash on 32-bit platforms (and maybe also some other
  integer-size related issues), #27.
- Added an ``.update()`` method to HashTableNT (like ``dict.update()``), #28.

Version 0.0.1 2024-10-31
------------------------

Initial release.
