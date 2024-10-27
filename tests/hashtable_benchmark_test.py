"""
Benchmark borghash.HashTable against CPython's dict.
"""

import pytest

from borghash import HashTable
from .hashtable_test import H2


@pytest.fixture(scope="module")
def items():
    # use quite a lot of items to reduce issues with timer resolution
    # and outside influences onto the measurement.
    items = []
    for x in range(1000000):
        key = H2(x)
        value_raw = key[-4:]
        items.append((key, value_raw))
    return frozenset(items)


def bh():  # borghash
    return HashTable(key_size=32, value_size=4)  # 256bit keys, 4Byte (32bit) values


def pd():  # python dict
    return dict()


HT_CLASSES = [bh, pd]


def setup(ht_class, items, fill=False):
    ht = ht_class()
    if fill:
        for key, value in items:
            ht[key] = value
    return (ht, items), {}


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_insert(benchmark, ht_class, items):
    def func(ht, items):
        for key, value in items:
            ht[key] = value

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=False))


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_update(benchmark, ht_class, items):
    def func(ht, items):
        for key, value in items:
            ht[key] = value  # update value for an existing ht entry

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=True))


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_lookup(benchmark, ht_class, items):
    def func(ht, items):
        for key, value in items:
            assert ht[key] == value

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=True))


@pytest.mark.parametrize("ht_class", HT_CLASSES)
def test_delete(benchmark, ht_class, items):
    def func(ht, items):
        for key, value in items:
            del ht[key]

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=True))
