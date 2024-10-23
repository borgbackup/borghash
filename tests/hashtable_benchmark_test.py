"""
Benchmark borghash.HashTable against CPython's dict.
"""

import pytest

from borghash import HashTable
from .hashtable_test import H2


@pytest.fixture(scope="module")
def keys():
    # use quite a lot of keys to reduce issues with timer resolution
    # and outside influences onto the measurement.
    return frozenset(H2(x) for x in range(1000000))


def bh():  # borghash
    return HashTable(key_size=32, value_size=4)  # 256bit keys, 4Byte (32bit) values


def pd():  # python dict
    return dict()


HT_CLASSES = [bh, pd]


def setup(ht_class, keys, fill=False):
    ht = ht_class()
    if fill:
        for key in keys:
            ht[key] = key[:4]
    return (ht, keys), {}


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_insert(benchmark, ht_class, keys):
    def func(ht, keys):
        for key in keys:
            ht[key] = key[:4]

    benchmark.pedantic(func, setup=lambda: setup(ht_class, keys, fill=False))


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_update(benchmark, ht_class, keys):
    def func(ht, keys):
        for key in keys:
            ht[key] = key[-4:]  # update value for an existing ht entry

    benchmark.pedantic(func, setup=lambda: setup(ht_class, keys, fill=True))


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_lookup(benchmark, ht_class, keys):
    def func(ht, keys):
        for key in keys:
            assert ht[key] == key[:4]

    benchmark.pedantic(func, setup=lambda: setup(ht_class, keys, fill=True))


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_delete(benchmark, ht_class, keys):
    def func(ht, keys):
        for key in keys:
            del ht[key]

    benchmark.pedantic(func, setup=lambda: setup(ht_class, keys, fill=True))
