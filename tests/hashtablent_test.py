from collections import namedtuple

import pytest

from borghash import HashTableNT

from .hashtable_test import H2

key_size = 32  # 32 bytes = 256bits key
value_format = "<III"  # 3x little endian 32bit unsigned int
value_type = namedtuple("vt", "v1 v2 v3")

key1, value1 = b"a" * 32, value_type(11, 12, 13)
key2, value2 = b"b" * 32, value_type(21, 22, 23)
key3, value3 = b"c" * 32, value_type(31, 32, 33)


@pytest.fixture
def ntht():
    return HashTableNT(key_size, value_format, value_type)


@pytest.fixture
def ntht12(ntht):
    ntht[key1] = value1
    ntht[key2] = value2
    return ntht


def test_insert_lookup(ntht12):
    assert ntht12[key1] == value1
    assert ntht12[key2] == value2
    assert type(ntht12[key1]) == value_type
    assert type(ntht12[key2]) == value_type


def test_remove_lookup(ntht12):
    del ntht12[key1]
    with pytest.raises(KeyError):
        ntht12[key1]

    del ntht12[key2]
    with pytest.raises(KeyError):
        ntht12[key2]


def test_items(ntht12):
    items = set(ntht12.iteritems())
    assert (key1, value1) in items
    assert (key2, value2) in items


def test_len(ntht12):
    assert len(ntht12) == 2


def test_contains(ntht12):
    assert key1 in ntht12
    assert key2 in ntht12


def test_get(ntht12):
    assert ntht12.get(key1, value3) == value1
    assert ntht12.get(key3, value3) == value3
    assert key3 not in ntht12


def test_setdefault(ntht12):
    assert ntht12.setdefault(key1, value3) == value1
    assert ntht12.setdefault(key3, value3) == value3
    assert ntht12[key3] == value3


def test_pop(ntht12):
    assert ntht12.pop(key1) == value1
    assert key1 not in ntht12
    assert ntht12.pop(key2) == value2
    assert key2 not in ntht12
    with pytest.raises(KeyError):
        ntht12.pop(key3)
    assert ntht12.pop(key3, None) is None


def test_ntht_stress(ntht):
    # this also triggers some hashtable resizing
    keys = set()
    for i in range(10000):
        key = H2(i)
        v = key[0]
        value = value_type(v, v*2, v*3)
        ntht[key] = value
        keys.add(key)
    found_keys = set()
    for key, value in ntht.iteritems():
        found_keys.add(key)
        v = key[0]
        assert value == value_type(v, v*2, v*3)
    assert keys == found_keys
    for key in keys:
        v = key[0]
        assert ntht[key] == value_type(v, v*2, v*3)
    for key in keys:
        del ntht[key]
    assert len(ntht) == 0


def test_stats(ntht):
    assert isinstance(ntht.stats, dict)


def test_read_write(ntht12, tmp_path):
    path = tmp_path / "hashtablent.msgpack"
    with open(path, "wb") as fd:
        ntht12.write(fd)
    with open(path, "rb") as fd:
        ntht_loaded = HashTableNT.read(fd)
    assert ntht_loaded[key1] == value1
    assert ntht_loaded[key2] == value2


