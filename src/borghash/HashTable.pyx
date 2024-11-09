"""
HashTable: low-level ht mapping fully random bytes keys to bytes values.
           key and value length can be chosen, but is fixed afterwards.
           the keys and values are stored in arrays separate from the hashtable.
           the hashtable only stores the 32bit indexes into the key/value arrays.
"""
from __future__ import annotations
from typing import BinaryIO, Iterator, Any

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, memcmp
from libc.stdint cimport uint8_t, uint32_t

from collections.abc import Mapping

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

    cdef size_t _get_index(self, uint8_t* key):
        """key must be perfectly random distributed bytes, so we don't need a hash function here."""
        cdef uint32_t key32 = (key[0] << 24) | (key[1] << 16) | (key[2] << 8) | key[3]
        return key32 % self.capacity

    cdef int _lookup_index(self, uint8_t* key_ptr, size_t* index_ptr):
        """
        search for a specific key.
        if found, return 1 and set *index_ptr to the index of the bucket in self.table.
        if not found, return 0 and set *index_ptr to the index of a free bucket in self.table.
        """
        cdef size_t index = self._get_index(key_ptr)
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
        cdef size_t index
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
        cdef size_t index
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
        cdef size_t index
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
        cdef size_t i
        cdef uint32_t kv_index
        self.stats_iter += 1
        for i in range(self.capacity):
            kv_index = self.table[i]
            if kv_index not in (FREE_BUCKET, TOMBSTONE_BUCKET):
                key = self.keys[kv_index * self.ksize:(kv_index + 1) * self.ksize]
                value = self.values[kv_index * self.vsize:(kv_index + 1) * self.vsize]
                yield key, value

    cdef void _resize_table(self, size_t new_capacity):
        cdef size_t i, index
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

    cdef void _resize_kv(self, size_t new_capacity):
        # We must never use kv indexes >= RESERVED, thus we'll never need more capacity either.
        cdef size_t capacity = min(new_capacity, <size_t> RESERVED - 1)
        self.stats_resize_kv += 1
        self.keys = <uint8_t*> realloc(self.keys, capacity * self.ksize * sizeof(uint8_t))
        self.values = <uint8_t*> realloc(self.values, capacity * self.vsize * sizeof(uint8_t))
        self.kv_capacity = <uint32_t> capacity

    def k_to_idx(self, key: bytes) -> int:
        """
        return the key's index in the keys array (index is stable while in memory).
        this can be used to "abbreviate" a known key (e.g. 256bit key -> 32bit index).
        """
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        cdef size_t index
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
        cdef size_t index
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
