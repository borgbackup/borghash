"""
Benchmark borghash.HashTable and HashTableNT against CPython's dict.
"""
from collections import namedtuple

import pytest

from borghash import HashTable, HashTableNT
from .hashtable_test import H2

VALUE_TYPE = namedtuple("value_type", "value")
VALUE_FMT = "<I"


@pytest.fixture(scope="module")
def items():
    # use quite a lot of items to reduce issues with timer resolution
    # and outside influences onto the measurement.
    items = []
    for x in range(1000000):
        key = H2(x)
        value_raw = key[-4:]
        value_nt = VALUE_TYPE(x % 2**32)
        items.append((key, value_raw, value_nt))
    return frozenset(items)


def bh():  # borghash
    return HashTable(key_size=32, value_size=4)  # 256bit keys, 4Byte (32bit) values


def bhnt():  # borghash
    return HashTableNT(key_size=32, value_type=VALUE_TYPE, value_format=VALUE_FMT)  # 256b key, 1-tuple with 32b value


def pd():  # python dict
    return dict()


TEST_PARAMS = [(bh, False), (bhnt, True), (pd, False), (pd, True)]


def setup(ht_class, items, fill=False, nt=False):
    ht = ht_class()
    if fill:
        for key, value_raw, value_nt in items:
            ht[key] = value_nt if nt else value_raw
    return (ht, items, nt), {}


@pytest.mark.parametrize("ht_class,nt", TEST_PARAMS)
def test_insert(benchmark, ht_class, nt, items):
    def func(ht, items, nt):
        for key, value_raw, value_nt in items:
            ht[key] = value_nt if nt else value_raw

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=False, nt=nt))


@pytest.mark.parametrize("ht_class,nt", TEST_PARAMS)
def test_update(benchmark, ht_class, nt, items):
    def func(ht, items, nt):
        for key, value_raw, value_nt in items:
            ht[key] = value_nt if nt else value_raw  # update value for an existing ht entry

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=True, nt=nt))


@pytest.mark.parametrize("ht_class,nt", TEST_PARAMS)
def test_lookup(benchmark, ht_class, nt, items):
    def func(ht, items, nt):
        for key, value_raw, value_nt in items:
            assert ht[key] == value_nt if nt else value_raw

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=True, nt=nt))


@pytest.mark.parametrize("ht_class,nt", TEST_PARAMS)
def test_delete(benchmark, ht_class, nt, items):
    def func(ht, items, nt):
        for key, _, _ in items:
            del ht[key]

    benchmark.pedantic(func, setup=lambda: setup(ht_class, items, fill=True, nt=nt))
