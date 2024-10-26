import hashlib

import pytest

from borghash import HashTable

# 256bit keys, 32bit values
key1, value1 = b"a" * 32, b"A" * 4
key2, value2 = b"b" * 32, b"B" * 4
key3, value3 = b"c" * 32, b"C" * 4


def H(x):
    # make some 32byte long thing that depends on x
    return bytes("%-0.32d" % x, "ascii")


def H2(x):
    # like H(x), but with pseudo-random distribution of the output value
    return hashlib.sha256(H(x)).digest()


@pytest.fixture
def ht():
    # 8 entries initially, 256bit keys, 4Byte (32bit) values
    return HashTable(key_size=32, value_size=4)


@pytest.fixture
def ht12(ht):
    ht[key1] = value1
    ht[key2] = value2
    return ht


def test_init():
    ht = HashTable(key_size=32, value_size=4)
    assert len(ht) == 0
    items = [(key1, value1), (key2, value2)]
    ht = HashTable(items, key_size=32, value_size=4)
    assert ht[key1] == value1
    assert ht[key2] == value2



def test_insert_lookup(ht12):
    assert ht12[key1] == value1
    assert ht12[key2] == value2


def test_remove_lookup(ht12):
    del ht12[key1]
    with pytest.raises(KeyError):
        ht12[key1]

    del ht12[key2]
    with pytest.raises(KeyError):
        ht12[key2]


def test_items(ht12):
    items = set(ht12.items())
    assert (key1, value1) in items
    assert (key2, value2) in items


def test_len(ht12):
    assert len(ht12) == 2


def test_contains(ht12):
    assert key1 in ht12
    assert key2 in ht12


def test_get(ht12):
    assert ht12.get(key1, value3) == value1
    assert ht12.get(key3, value3) == value3
    assert key3 not in ht12


def test_setdefault(ht12):
    assert ht12.setdefault(key1, value3) == value1
    assert ht12.setdefault(key3, value3) == value3
    assert ht12[key3] == value3


def test_pop(ht12):
    assert ht12.pop(key1) == value1
    assert key1 not in ht12
    assert ht12.pop(key2) == value2
    assert key2 not in ht12
    with pytest.raises(KeyError):
        ht12.pop(key3)
    assert ht12.pop(key3, None) is None


def test_clear(ht12):
    ht12.clear()
    assert len(ht12) == 0
    assert len(list(ht12.items())) == 0
    with pytest.raises(KeyError):
        ht12[key1]
    with pytest.raises(KeyError):
        ht12[key2]


def test_ht_stress(ht):
    # this also triggers some hashtable resizing
    keys = set()
    for i in range(10000):
        key = H2(i)
        value = key[:4]
        ht[key] = value
        keys.add(key)
    found_keys = set()
    for key, value in ht.items():
        found_keys.add(key)
        assert value == key[:4]
    assert keys == found_keys
    for key in keys:
        assert ht[key] == key[:4]
    for key in keys:
        del ht[key]
    assert len(ht) == 0


def test_stats(ht):
    assert ht.stats["get"] == 0
    assert ht.stats["set"] == 0
    assert ht.stats["del"] == 0
    assert ht.stats["iter"] == 0
    assert ht.stats["lookup"] == 0
    assert ht.stats["linear"] == 0
    assert ht.stats["resize_table"] == 0
    assert ht.stats["resize_table"] == 0
    assert ht.stats["resize_kv"] == 0
    ht[key1] = value1
    assert ht.stats["set"] == 1
    assert ht.stats["lookup"] == 1
    ht[key1]
    assert ht.stats["get"] == 1
    assert ht.stats["lookup"] == 2
    del ht[key1]
    assert ht.stats["del"] == 1
    assert ht.stats["lookup"] == 3
    list(ht.items())
    assert ht.stats["iter"] == 1


def test_k_to_idx(ht12):
    idx1 = ht12.k_to_idx(key1)
    idx2 = ht12.k_to_idx(key2)
    with pytest.raises(KeyError):
        ht12.k_to_idx(key3)
    assert idx1 != idx2
    assert ht12.idx_to_k(idx1) == key1
    assert ht12.idx_to_k(idx2) == key2


def test_kv_to_idx(ht12):
    idx1 = ht12.kv_to_idx(key1, value1)
    idx2 = ht12.kv_to_idx(key2, value2)
    with pytest.raises(KeyError):
        ht12.kv_to_idx(key3, value3)
    with pytest.raises(KeyError):
        ht12.kv_to_idx(key1, value2)
    with pytest.raises(KeyError):
        ht12.kv_to_idx(key2, value1)
    assert idx1 != idx2
    assert ht12.idx_to_kv(idx1) == (key1, value1)
    assert ht12.idx_to_kv(idx2) == (key2, value2)
