import struct
import pytest

from borghash import HashTable

def H(x, y):
    """
    Create a 256bit key - x will determine the first 32bit, y will determine the last 32bit.
    As our HashTable computes the ht index from first 32bit, same x will give same ht index (a collision).
    """
    return struct.pack(">IIIIIIII", x, 0, 0, 0, 0, 0, 0, y)  # BE is easier to read.


@pytest.fixture
def ht():
    # 256bit keys, 32bit values
    return HashTable(key_size=32, value_size=4)


def test_ht_stress(ht):
    # this also triggers some hashtable resizing
    keys = set()
    for i in range(10000):
        key = H(i, i)
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
