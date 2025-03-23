ChangeLog
=========

Version 0.1.0 2024-11-18
------------------------

- HashTableNT: deal with byte_order separately
- HashTableNT: give separate formats in value_format namedtuple

Version 0.0.2 2024-11-10
------------------------

- Fixed "KV array is full" crash on 32bit platforms (and maybe also some other
  int-size related issues), #27.
- Added a .update method to HashTableNT (like dict.update), #28.

Version 0.0.1 2024-10-31
------------------------

Initial release.
