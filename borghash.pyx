"""
borghash - hashtable implementations in cython.

HashTable: low-level ht mapping fully random bytes keys to bytes values.
           key and value length can be chosen, but is fixed afterwards.
           the keys and values are stored in arrays separate from the hashtable.
           the hashtable only stores the 32bit indexes into the key/value arrays.

HashTableNT: wrapper around HashTable, providing namedtuple values and serialization.
"""
from __future__ import annotations
from typing import BinaryIO, Iterator, Any

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, memcmp
from libc.stdint cimport uint8_t, uint32_t

from collections import namedtuple
from collections.abc import Mapping
import json
import struct

MAGIC = b"BORGHASH"
assert len(MAGIC) == 8
VERSION = 1  # version of the on-disk (serialized) format produced by .write().
HEADER_FMT = "<8sII"  # magic, version, meta length

MIN_CAPACITY = 1000  # never shrink the hash table below this capacity

cdef uint32_t FREE_BUCKET = 0xFFFFFFFF
cdef uint32_t TOMBSTONE_BUCKET = 0xFFFFFFFE
# ...
cdef uint32_t RESERVED = 0xFFFFFF00  # all >= this is reserved

_NoDefault = object()

def _fill(this: Any, other: Any) -> None:
    """fill this mapping from other"""
    if other is None:
        return
    if isinstance(other, Mapping):
        for key in other:
            this[key] = other[key]
    elif hasattr(other, "keys"):
        for key in other.keys():
            this[key] = other[key]
    else:
        for key, value in other:
            this[key] = value


cdef class HashTable:
    def __init__(self, items=None, *,
                 key_size: int = 0, value_size: int = 0, capacity: int = MIN_CAPACITY,
                 max_load_factor: float = 0.5, min_load_factor: float = 0.10,
                 shrink_factor: float = 0.4, grow_factor: float = 2.0,
                 kv_grow_factor: float = 1.3) -> None:
        # the load of the ht (.table) shall be between 0.25 and 0.5, so it is fast and has few collisions.
        # it is cheap to have a low hash table load, because .table only stores uint32_t indexes into the
        # .keys and .values array.
        # the keys/values arrays have bigger elements and are not hash tables, thus collisions and load
        # factor are no concern there. the kv_grow_factor can be relatively small.
        if not key_size:
            raise ValueError("key_size must be specified and must be > 0.")
        if not value_size:
            raise ValueError("value_size must be specified and must be > 0.")
        self.ksize = key_size
        self.vsize = value_size
        # vvv hash table vvv
        self.max_load_factor = max_load_factor
        self.min_load_factor = min_load_factor
        self.shrink_factor = shrink_factor
        self.grow_factor = grow_factor
        self.initial_capacity = capacity
        self.capacity = 0
        self.used = 0
        self.tombstones = 0
        self.table = NULL
        self._resize_table(self.initial_capacity)
        # ^^^ hash table ^^^
        # vvv kv arrays vvv
        self.kv_grow_factor = kv_grow_factor
        self.kv_used = 0
        self.keys = NULL
        self.values = NULL
        self._resize_kv(int(self.initial_capacity * self.max_load_factor))
        # ^^^ kv arrays ^^^
        # vvv stats vvv
        self.stats_get = 0
        self.stats_set = 0
        self.stats_del = 0
        self.stats_iter = 0  # .items() calls
        self.stats_lookup = 0  # _lookup_index calls
        self.stats_linear = 0  # how many steps the linear search inside _lookup_index needed
        self.stats_resize_table = 0
        self.stats_resize_kv = 0
        # ^^^ stats ^^^
        _fill(self, items)

    def __del__(self) -> None:
        free(self.table)
        free(self.keys)
        free(self.values)

    def clear(self) -> None:
        """empty HashTable, start from scratch"""
        self.capacity = 0
        self.used = 0
        self._resize_table(self.initial_capacity)
        self.kv_used = 0
        self._resize_kv(int(self.initial_capacity * self.max_load_factor))

    def __len__(self) -> int:
        return self.used

    cdef int _get_index(self, uint8_t* key):
        """key must be perfectly random distributed bytes, so we don't need a hash function here."""
        cdef uint32_t key32 = (key[0] << 24) | (key[1] << 16) | (key[2] << 8) | key[3]
        return key32 % self.capacity

    cdef int _lookup_index(self, uint8_t* key_ptr, int* index_ptr):
        """
        search for a specific key.
        if found, return 1 and set *index_ptr to the index of the bucket in self.table.
        if not found, return 0 and set *index_ptr to the index of a free bucket in self.table.
        """
        cdef int index = self._get_index(key_ptr)
        cdef uint32_t kv_index
        self.stats_lookup += 1
        while (kv_index := self.table[index]) != FREE_BUCKET:
            self.stats_linear += 1
            if kv_index != TOMBSTONE_BUCKET and memcmp(self.keys + kv_index * self.ksize, key_ptr, self.ksize) == 0:
                if index_ptr:
                    index_ptr[0] = index
                return 1  # found
            index = (index + 1) % self.capacity
        if index_ptr:
            index_ptr[0] = index
        return 0  # not found

    def __setitem__(self, key: bytes, value: bytes) -> None:
        if len(key) != self.ksize or len(value) != self.vsize:
            raise ValueError("Key or value size does not match the defined sizes")

        cdef uint8_t* key_ptr = <uint8_t*> key
        cdef uint8_t* value_ptr = <uint8_t*> value
        cdef uint32_t kv_index
        cdef int index
        self.stats_set += 1
        if self._lookup_index(key_ptr, &index):
            kv_index = self.table[index]
            memcpy(self.values + kv_index * self.vsize, value_ptr, self.vsize)
            return

        if self.kv_used >= self.kv_capacity:
            self._resize_kv(int(self.kv_capacity * self.kv_grow_factor))
        if self.kv_used >= self.kv_capacity:
            # Should never happen. See "RESERVED" constant - we allow almost 4Gi kv entries.
            # For a typical 256bit key and a small 32bit value that would already consume 176GiB+
            # memory (plus spikes to even more when hashtable or kv arrays get resized).
            raise RuntimeError("KV array is full")

        kv_index = self.kv_used
        memcpy(self.keys + kv_index * self.ksize, key_ptr, self.ksize)
        memcpy(self.values + kv_index * self.vsize, value_ptr, self.vsize)
        self.kv_used += 1

        self.used += 1
        self.table[index] = kv_index  # _lookup_index has set index to a free bucket

        if self.used + self.tombstones > self.capacity * self.max_load_factor:
            self._resize_table(int(self.capacity * self.grow_factor))

    def __contains__(self, key: bytes) -> bool:
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        return bool(self._lookup_index(<uint8_t*> key, NULL))

    def __getitem__(self, key: bytes) -> bytes:
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        cdef uint32_t kv_index
        cdef int index
        self.stats_get += 1
        if self._lookup_index(<uint8_t*> key, &index):
            kv_index = self.table[index]
            return self.values[kv_index * self.vsize:(kv_index + 1) * self.vsize]
        else:
            raise KeyError("Key not found")

    def __delitem__(self, key: bytes) -> None:
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        cdef uint8_t* key_ptr = <uint8_t*> key
        cdef int index
        cdef uint32_t kv_index

        self.stats_del += 1
        if self._lookup_index(key_ptr, &index):
            kv_index = self.table[index]
            memset(self.keys + kv_index * self.ksize, 0, self.ksize)
            memset(self.values + kv_index * self.vsize, 0, self.vsize)
            self.table[index] = TOMBSTONE_BUCKET
            self.used -= 1
            self.tombstones += 1

            # Resize down if necessary
            if self.used < self.capacity * self.min_load_factor:
                new_capacity = max(int(self.capacity * self.shrink_factor), MIN_CAPACITY)
                self._resize_table(new_capacity)
        else:
            raise KeyError("Key not found")

    def setdefault(self, key: bytes, value: bytes) -> bytes:
        if not key in self:
            self[key] = value
        return self[key]

    def get(self, key: bytes, default: Any = None) -> bytes|Any:
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key: bytes, default: Any = _NoDefault) -> bytes|Any:
        try:
            value = self[key]
        except KeyError:
            if default is _NoDefault:
                raise
            return default
        else:
            del self[key]
            return value

    def items(self) -> Iterator[tuple[bytes, bytes]]:
        cdef int i
        cdef uint32_t kv_index
        self.stats_iter += 1
        for i in range(self.capacity):
            kv_index = self.table[i]
            if kv_index not in (FREE_BUCKET, TOMBSTONE_BUCKET):
                key = self.keys[kv_index * self.ksize:(kv_index + 1) * self.ksize]
                value = self.values[kv_index * self.vsize:(kv_index + 1) * self.vsize]
                yield key, value

    cdef void _resize_table(self, int new_capacity):
        cdef int i, index
        cdef uint32_t kv_index
        cdef uint32_t* new_table = <uint32_t*> malloc(new_capacity * sizeof(uint32_t))
        for i in range(new_capacity):
            new_table[i] = FREE_BUCKET

        self.stats_resize_table += 1
        current_capacity = self.capacity
        self.capacity = new_capacity
        for i in range(current_capacity):
            kv_index = self.table[i]
            if kv_index not in (FREE_BUCKET, TOMBSTONE_BUCKET):
                index = self._get_index(self.keys + kv_index * self.ksize)
                while new_table[index] != FREE_BUCKET:
                    index = (index + 1) % new_capacity
                new_table[index] = kv_index

        free(self.table)
        self.table = new_table
        self.tombstones = 0

    cdef void _resize_kv(self, int new_capacity):
        # We must never use kv indexes >= RESERVED, thus we'll never need more capacity either.
        cdef int capacity = min(new_capacity, RESERVED - 1)
        self.stats_resize_kv += 1
        self.keys = <uint8_t*> realloc(self.keys, capacity * self.ksize * sizeof(uint8_t))
        self.values = <uint8_t*> realloc(self.values, capacity * self.vsize * sizeof(uint8_t))
        self.kv_capacity = capacity

    def k_to_idx(self, key: bytes) -> int:
        """
        return the key's index in the keys array (index is stable while in memory).
        this can be used to "abbreviate" a known key (e.g. 256bit key -> 32bit index).
        """
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        cdef int index
        if self._lookup_index(<uint8_t*> key, &index):
            return self.table[index]  # == uint32_t kv_index
        else:
            raise KeyError("Key not found")

    def idx_to_k(self, idx: int) -> bytes:
        """
        for a given index, return the key stored at that index in the keys array.
        this is the reverse of k_to_idx (e.g. 32bit index -> 256bit key).
        """
        cdef uint32_t kv_index = <uint32_t> idx
        return self.keys[kv_index * self.ksize:(kv_index + 1) * self.ksize]

    def kv_to_idx(self, key: bytes, value: bytes) -> int:
        """
        return the key's/value's index in the keys/values array (index is stable while in memory).
        this can be used to "abbreviate" a known key/value pair. (e.g. 256bit key + 32bit value -> 32bit index).
        """
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        if len(value) != self.vsize:
            raise ValueError("Value size does not match the defined size")
        cdef int index
        cdef uint32_t kv_index
        if self._lookup_index(<uint8_t*> key, &index):
            kv_index = self.table[index]
            value_found = self.values[kv_index * self.vsize:(kv_index + 1) * self.vsize]
            if value == value_found:
                return kv_index
        raise KeyError("Key/Value not found")

    def idx_to_kv(self, idx: int) -> tuple[bytes, bytes]:
        """
        for a given index, return the key/value stored at that index in the keys/values array.
        this is the reverse of kv_to_idx (e.g. 32bit index -> 256bit key + 32bit value).
        """
        cdef uint32_t kv_index = <uint32_t> idx
        key = self.keys[kv_index * self.ksize:(kv_index + 1) * self.ksize]
        value = self.values[kv_index * self.vsize:(kv_index + 1) * self.vsize]
        return key, value

    @property
    def stats(self) -> dict[str, int]:
        return {
            "get": self.stats_get,
            "set": self.stats_set,
            "del": self.stats_del,
            "iter": self.stats_iter,
            "lookup": self.stats_lookup,
            "linear": self.stats_linear,
            "resize_table": self.stats_resize_table,
            "resize_kv": self.stats_resize_kv,
        }


cdef class HashTableNT:
    def __init__(self, items=None, *,
                 key_size: int = 0, value_format: str = "", value_type: Any = None,
                 capacity: int = MIN_CAPACITY) -> None:
        if not key_size:
            raise ValueError("key_size must be specified and must be > 0.")
        if not value_format:
            raise ValueError("value_format must be specified and must be non-empty.")
        if value_type is None:
            raise ValueError("value_type must be specified (a namedtuple type corresponding to value_format).")
        self.key_size = key_size
        self.value_struct = struct.Struct(value_format)
        self.value_size = self.value_struct.size
        self.value_type = value_type
        self.inner = HashTable(key_size=self.key_size, value_size=self.value_size, capacity=capacity)
        _fill(self, items)

    def clear(self) -> None:
        self.inner.clear()

    def _check_key(self, key: bytes) -> None:
        if not isinstance(key, bytes):
            raise TypeError(f"Expected an instance of bytes, got {type(key)}")
        if len(key) != self.key_size:
            raise ValueError(f"Key must be {self.key_size} bytes long")

    def _to_binary_value(self, value: Any) -> bytes:
        if not isinstance(value, self.value_type):
            if isinstance(value, tuple):
                value = self.value_type(*value)
            else:
                raise TypeError(f"Expected an instance of {self.value_type}, got {type(value)}")
        return self.value_struct.pack(*value)

    def _to_namedtuple_value(self, binary_value: bytes) -> Any:
        unpacked_data = self.value_struct.unpack(binary_value)
        return self.value_type(*unpacked_data)

    def _set_raw(self, key: bytes, value: bytes) -> None:
        self.inner[key] = value

    def _get_raw(self, key: bytes) -> bytes:
        return self.inner[key]

    def __setitem__(self, key: bytes, value: Any) -> None:
        self._check_key(key)
        self.inner[key] = self._to_binary_value(value)

    def __getitem__(self, key: bytes) -> Any:
        self._check_key(key)
        binary_value = self.inner[key]
        return self._to_namedtuple_value(binary_value)

    def __delitem__(self, key: bytes) -> None:
        self._check_key(key)
        del self.inner[key]

    def __contains__(self, key: bytes) -> bool:
        self._check_key(key)
        return key in self.inner

    def items(self) -> Iterator[tuple[bytes, Any]]:
        for key, binary_value in self.inner.items():
            yield (key, self._to_namedtuple_value(binary_value))

    def __len__(self) -> int:
        return len(self.inner)

    def get(self, key: bytes, default: Any = None) -> Any:
        self._check_key(key)
        try:
            binary_value = self.inner[key]
        except KeyError:
            return default
        else:
            return self._to_namedtuple_value(binary_value)

    def setdefault(self, key: bytes, default: Any) -> Any:
        self._check_key(key)
        binary_default = self._to_binary_value(default)
        binary_value = self.inner.setdefault(key, binary_default)
        return self._to_namedtuple_value(binary_value)

    def pop(self, key: bytes, default: Any = _NoDefault) -> Any:
        self._check_key(key)
        try:
            binary_value = self.inner.pop(key)
        except KeyError:
            if default is _NoDefault:
                raise
            return default
        else:
            return self._to_namedtuple_value(binary_value)

    def k_to_idx(self, key: bytes) -> int:
        return self.inner.k_to_idx(key)

    def idx_to_k(self, idx: int) -> bytes:
        return self.inner.idx_to_k(idx)

    def kv_to_idx(self, key: bytes, value: Any) -> int:
        binary_value = self._to_binary_value(value)
        return self.inner.kv_to_idx(key, binary_value)

    def idx_to_kv(self, idx: int) -> tuple[bytes, Any]:
        key, binary_value = self.inner.idx_to_kv(idx)
        return key, self._to_namedtuple_value(binary_value)

    @property
    def stats(self) -> dict[str, int]:
        return self.inner.stats

    def write(self, file: BinaryIO|str|bytes):
        if isinstance(file, (str, bytes)):
            with open(file, 'wb') as fd:
                self._write_fd(fd)
        else:
            self._write_fd(file)

    def _write_fd(self, fd: BinaryIO):
        meta = {
            'key_size': self.key_size,
            'value_size': self.value_size,
            'value_format': self.value_struct.format,
            'value_type_name': self.value_type.__name__,
            'value_type_fields': self.value_type._fields,
            'capacity': self.inner.capacity,
            'used': self.inner.used,  # count of keys / values
        }
        meta_bytes = json.dumps(meta).encode("utf-8")
        meta_size = len(meta_bytes)
        header_bytes = struct.pack(HEADER_FMT, MAGIC, VERSION, meta_size)
        fd.write(header_bytes)
        fd.write(meta_bytes)
        count = 0
        for key, value in self.inner.items():
            fd.write(key)
            fd.write(value)
            count += 1
        assert count == self.inner.used

    @classmethod
    def read(cls, file: BinaryIO|str|bytes):
        if isinstance(file, (str, bytes)):
            with open(file, 'rb') as fd:
                return cls._read_fd(fd)
        else:
            return cls._read_fd(file)

    @classmethod
    def _read_fd(cls, fd: BinaryIO):
        header_size = struct.calcsize(HEADER_FMT)
        header_bytes = fd.read(header_size)
        if len(header_bytes) < header_size:
            raise ValueError(f"Invalid file, file is too short.")
        magic, version, meta_size = struct.unpack(HEADER_FMT, header_bytes)
        if magic != MAGIC:
            raise ValueError(f"Invalid file, magic {MAGIC.decode()} not found.")
        if version != VERSION:
            raise ValueError(f"Unsupported file version {version}.")
        meta_bytes = fd.read(meta_size)
        if len(meta_bytes) < meta_size:
            raise ValueError(f"Invalid file, file is too short.")
        meta = json.loads(meta_bytes.decode("utf-8"))
        value_type = namedtuple(meta['value_type_name'], meta['value_type_fields'])
        ht = cls(key_size=meta['key_size'], value_format=meta['value_format'], value_type=value_type, capacity=meta['capacity'])
        count = 0
        ksize, vsize = meta['key_size'], meta['value_size']
        for i in range(meta['used']):
            key = fd.read(ksize)
            value = fd.read(vsize)
            ht._set_raw(key, value)
        return ht

    def size(self) -> int:
        """
        do a rough worst-case estimate of the on-disk size when using .write().

        the serialized size of the metadata is a bit hard to predict, but we cover that with one_time_overheads.
        """
        one_time_overheads = 4096  # very rough
        N = self.inner.used
        return int(N * (self.key_size + self.value_size) + one_time_overheads)


def demo():
    print("BorgHash demo")
    print("=============")
    print("Code:")
    code = """
from tempfile import NamedTemporaryFile
from time import time

count = 50000
value_type = namedtuple("Chunk", ["refcount", "size"])
# 256bit (32Byte) key, 2x 32bit (4Byte) values
ht = HashTableNT(key_size=32, value_format="<II", value_type=value_type)

t0 = time()
for i in range(count):
    # make up a 256bit key from i, first 32bits need to be well distributed.
    key = f"{i:4x}{' '*28}".encode()
    value = value_type(refcount=i, size=i * 2)
    ht[key] = value
assert len(ht) == count

t1 = time()
found = 0
for key, value in ht.items():
    i = int(key.decode(), 16)
    expected_value = value_type(refcount=i, size=i * 2)
    assert ht[key] == expected_value
    found += 1
assert found == count

t2 = time()
ht_written = ht
with NamedTemporaryFile(prefix="borghash-demo-ht-read", suffix=".tmp", delete=False) as tmpfile:
    ht_written.write(tmpfile)
    filename = tmpfile.name
assert len(ht_written) == count, f"{len(ht_written)} != {count}"

t3 = time()
ht_read = HashTableNT.read(filename)
assert len(ht_read) == count, f"{len(ht_read)} != {count}"

t4 = time()
for i in range(count):
    # make up a 256bit key from i, first 32bits need to be well distributed.
    key = f"{i:4x}{' '*28}".encode()
    expected_value = value_type(refcount=i, size=i * 2)
    assert ht_read.pop(key) == expected_value
assert len(ht_read) == 0

t5 = time()
print("Result:")
print(f"HashTableNT in-memory ops (count={count}): insert: {t1-t0:.3f}s, lookup: {t2-t1:.3f}s, pop: {t5-t4:.3f}s.")
print(f"HashTableNT serialization (count={count}): write: {t3-t2:.3f}s, read: {t4-t3:.3f}s.")
"""
    print(code)
    exec(code)
