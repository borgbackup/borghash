import hashlib

import pytest

from borghash import HashTable

# 256bit keys, 32bit values
key1, value1 = b"a" * 32, b"A" * 4
key2, value2 = b"b" * 32, b"B" * 4


def H(x):
    # make some 32byte long thing that depends on x
    return bytes("%-0.32d" % x, "ascii")


def H2(x):
    # like H(x), but with pseudo-random distribution of the output value
    return hashlib.sha256(H(x)).digest()


@pytest.fixture
def ht():
    # 8 entries initially, 256bit keys, 4Byte (32bit) values
    return HashTable(size=8, value_size=4, max_load_factor=0.75, min_load_factor=0.3)


@pytest.fixture
def ht12(ht):
    ht.insert(key1, value1)
    ht.insert(key2, value2)
    return ht


def test_cyhash_insert_lookup(ht12):
    assert ht12.lookup(key1) == value1
    assert ht12.lookup(key2) == value2


def test_remove_lookup(ht12):
    ht12.remove(key1)
    with pytest.raises(KeyError):
        ht12.lookup(key1)

    ht12.remove(key2)
    with pytest.raises(KeyError):
        ht12.lookup(key2)


def test_items(ht12):
    items = set(ht12.items())
    assert (key1, value1) in items
    assert (key2, value2) in items


def test_len(ht12):
    assert len(ht12) == 2


def test_save_load(ht12, tmp_path):
    path = tmp_path / "hashtable.msgpack"
    with open(path, "wb") as fd:
        ht12.save(fd)
    with open(path, "rb") as fd:
        ht_loaded = HashTable.load(fd)
    assert ht_loaded.lookup(key1) == value1
    assert ht_loaded.lookup(key2) == value2


def test_stress(ht):
    # this also triggers some hashtable resizing
    keys = set()
    for i in range(10000):
        key = H2(i)
        value = key[:4]
        ht.insert(key, value)
        keys.add(key)
    found_keys = set()
    for key, value in ht.items():
        found_keys.add(key)
        assert value == key[:4]
    assert keys == found_keys
    for key in keys:
        assert ht.lookup(key) == key[:4]
    for key in keys:
        ht.remove(key)
    assert len(ht) == 0
