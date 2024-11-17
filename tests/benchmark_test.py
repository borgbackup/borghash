"""
Benchmark borghash.HashTable and HashTableNT against CPython's dict.
"""
import struct
from collections import namedtuple

import pytest

from borghash import HashTable, HashTableNT
from .hashtable_test import H2

VALUE_TYPE = namedtuple("value_type", "value")
VALUE_FMT_TYPE = namedtuple("value_format", "value")
VALUE_FMT = VALUE_FMT_TYPE("I")
KEY_SIZE = len(H2(0))
VALUE_SIZE = len(struct.pack("".join(VALUE_FMT), 0))
VALUE_BITS = VALUE_SIZE * 8

@pytest.fixture(scope="module")
def items():
    # use quite a lot of items to reduce issues with timer resolution
    # and outside influences onto the measurement.
    items = []
    for x in range(1000000):
        key = H2(x)
        value_raw = key[-VALUE_SIZE:]
        value_nt = VALUE_TYPE(x % 2**VALUE_BITS)
        items.append((key, value_raw, value_nt))
    return frozenset(items)


def bh():  # borghash
    return HashTable(key_size=KEY_SIZE, value_size=VALUE_SIZE)


def bhnt():  # borghash
    return HashTableNT(key_size=KEY_SIZE, value_type=VALUE_TYPE, value_format=VALUE_FMT)


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
