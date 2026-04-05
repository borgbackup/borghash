from collections import namedtuple
import os

import pytest

from borghash import HashTableNT
from .hashtable_test import H2

key_size = 32
value_type = namedtuple("vt", "v1 v2 v3")
value_format_t = namedtuple("vf", "v1 v2 v3")
value_format = value_format_t(v1="I", v2="I", v3="I")

key1, value1 = b"a" * 32, value_type(11, 12, 13)
key2, value2 = b"b" * 32, value_type(21, 22, 23)
key3, value3 = b"c" * 32, value_type(31, 32, 33)


def test_mmap_open_existing(tmp_path):
    path = str(tmp_path / "test.borghash")
    # Create and write a file
    ht = HashTableNT(key_size=key_size, value_type=value_type, value_format=value_format)
    ht[key1] = value1
    ht[key2] = value2
    ht.write(path)

    # Open in mmap mode
    ht_mmap = HashTableNT.open_mmap(path)
    assert len(ht_mmap) == 2
    assert ht_mmap[key1] == value1
    assert ht_mmap[key2] == value2


def test_mmap_persistence(tmp_path):
    path = str(tmp_path / "test_persistence.borghash")
    ht = HashTableNT(key_size=key_size, value_type=value_type, value_format=value_format)
    ht[key1] = value1
    ht.write(path)
    
    # Open mmap, modify, and close
    ht_mmap = HashTableNT.open_mmap(path)
    ht_mmap[key2] = value2
    del ht_mmap[key1]
    # Update header/metadata in the file
    ht_mmap.write_header()
    
    # Re-open normally to verify
    ht_read = HashTableNT.read(path)
    assert key1 not in ht_read
    assert ht_read[key2] == value2


def test_mmap_resize(tmp_path):
    path = str(tmp_path / "test_resize.borghash")
    # Small initial capacity to trigger resize early
    ht = HashTableNT(key_size=key_size, value_type=value_type, value_format=value_format, capacity=100)
    ht[key1] = value1
    ht.write(path)
    
    ht_mmap = HashTableNT.open_mmap(path)
    # Add many items to trigger KV and table resize
    for i in range(200):
        key = H2(i)
        ht_mmap[key] = value_type(i, i+1, i+2)
    
    ht_mmap.write_header()  # update used count in metadata
    
    assert len(ht_mmap) == 201
    assert ht_mmap[key1] == value1
    
    # Close and reopen to ensure resized file is valid
    ht_reopened = HashTableNT.open_mmap(path)
    assert len(ht_reopened) == 201
    assert ht_reopened[key1] == value1
    for i in range(200):
        key = H2(i)
        assert ht_reopened[key] == value_type(i, i+1, i+2)


def test_mmap_shrink_to_fit(tmp_path):
    path = str(tmp_path / "test_shrink.borghash")
    # Small initial_capacity so it grows
    ht = HashTableNT(key_size=key_size, value_type=value_type, value_format=value_format, capacity=100)
    for i in range(100):
        ht[H2(i)] = value_type(i, 0, 0)
    ht.write(path)
    
    ht_mmap = HashTableNT.open_mmap(path)
    # Add items to trigger growth of kv_capacity beyond kv_used
    for i in range(100, 200):
         ht_mmap[H2(i)] = value_type(i, 0, 0)
    
    # After 200 items, kv_capacity > 200 (due to kv_grow_factor)
    assert ht_mmap.inner.kv_capacity > 200
    initial_size = os.path.getsize(path)

    # shrink_to_fit should reduce file size to exactly kv_used
    ht_mmap.inner.shrink_to_fit()
    ht_mmap.write_header()
    
    shrunk_size = os.path.getsize(path)
    assert shrunk_size < initial_size
    assert len(ht_mmap) == 200


def test_mmap_new_file(tmp_path):
    # Testing using HashTableNT directly with a path to create a NEW mmapped file
    path = str(tmp_path / "new_mmap.borghash")
    ht = HashTableNT(key_size=key_size, value_type=value_type, value_format=value_format, path=path)
    ht[key1] = value1
    ht.write_header() # Initialize header for new file
    assert os.path.exists(path)
    
    # Check if it persists without explicit write()
    ht2 = HashTableNT.open_mmap(path)
    assert ht2[key1] == value1


def test_mmap_corrupt_magic(tmp_path):
    path = tmp_path / "corrupt.borghash"
    path.write_bytes(b"NOTBORG" + b"\x00" * 100)
    with pytest.raises(ValueError, match="magic BORGHASH not found"):
        HashTableNT.open_mmap(str(path))
