"""
borghash - hashtable implementations in cython.

HashTable: low-level ht mapping fully random bytes keys to bytes values.
           key and values length can be chosen, but is fixed afterwards.
           the keys and values are stored in arrays separate from the hashtable.
           the hashtable only stores the 32bit indexes into the key/value arrays.

HashTableNT: wrapper around HashTable, providing namedtuple values and serialization.
"""
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, memcmp
from libc.stdint cimport uint8_t, uint32_t
from collections import namedtuple
import struct

import msgpack

MIN_CAPACITY = 1000  # never shrink the hash table below this capacity

cdef uint32_t FREE_BUCKET = 0xFFFFFFFF
cdef uint32_t TOMBSTONE_BUCKET = 0xFFFFFFFE
# ...
cdef uint32_t RESERVED = 0xFFFFFF00  # all >= this is reserved

_NoDefault = object()

cdef class HashTable:
    cdef int ksize, vsize
    cdef int capacity, used, tombstones
    cdef float max_load_factor, min_load_factor, shrink_factor, grow_factor
    cdef uint32_t* table
    cdef int kv_capacity, kv_used
    cdef float kv_grow_factor
    cdef uint8_t* keys
    cdef uint8_t* values
    cdef int stats_get, stats_set, stats_del, stats_iter, stats_lookup, stats_linear
    cdef int stats_resize_table, stats_resize_kv

    def __init__(self, key_size: int, value_size: int, capacity: int = MIN_CAPACITY,
                 max_load_factor: float = 0.5, min_load_factor: float = 0.10,
                 shrink_factor: float = 0.4, grow_factor: float = 2.0,
                 kv_grow_factor: float = 1.3):
        # the load of the ht (.table) shall be between 0.25 and 0.5, so it is fast and has few collisions.
        # it is cheap to have a low hash table load, because .table only stores uint32_t indexes into the
        # .keys and .values array.
        # the keys/values arrays have bigger elements and are not hash tables, thus collisions and load
        # factor are no concern there. the kv_grow_factor can be relatively small.
        self.ksize = key_size
        self.vsize = value_size
        # vvv hash table vvv
        self.max_load_factor = max_load_factor
        self.min_load_factor = min_load_factor
        self.shrink_factor = shrink_factor
        self.grow_factor = grow_factor
        self.capacity = 0
        self.used = 0
        self.tombstones = 0
        self.table = NULL
        self._resize_table(capacity)
        # ^^^ hash table ^^^
        # vvv kv arrays vvv
        self.kv_grow_factor = kv_grow_factor
        self.kv_used = 0
        self.keys = NULL
        self.values = NULL
        self._resize_kv(int(capacity * max_load_factor))
        # ^^^ kv arrays ^^^
        self.stats_get = 0
        self.stats_set = 0
        self.stats_del = 0
        self.stats_iter = 0  # iteritems calls
        self.stats_lookup = 0  # _lookup_index calls
        self.stats_linear = 0  # how many steps the linear search inside _lookup_index needed
        self.stats_resize_table = 0
        self.stats_resize_kv = 0

    def __del__(self):
        free(self.table)
        free(self.keys)
        free(self.values)

    def __len__(self):
        return self.used

    cdef int _get_index(self, uint8_t* key):
        """key must be a perfectly random distributed value, so we don't need a hash function here."""
        cdef uint32_t key32 = (key[0] << 24) | (key[1] << 16) | (key[2] << 8) | key[3]
        return key32 % self.capacity

    cdef int _lookup_index(self, uint8_t* key_ptr, int* index_ptr = NULL):
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

    def __setitem__(self, key: bytes, value: bytes):
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
        # _lookup_index set index to a free bucket
        self.table[index] = kv_index

        if self.used + self.tombstones > self.capacity * self.max_load_factor:
            self._resize_table(int(self.capacity * self.grow_factor))

    def __contains__(self, key: bytes):
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        return bool(self._lookup_index(<uint8_t*> key))

    def __getitem__(self, key: bytes):
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

    def __delitem__(self, key: bytes):
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

    def setdefault(self, key: bytes, value: bytes):
        if not key in self:
            self[key] = value
        return self[key]

    def get(self, key: bytes, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key: bytes, default=_NoDefault):
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is _NoDefault:
                raise
            return default

    def iteritems(self):
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

    @property
    def stats(self):
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
    cdef int key_size
    cdef str value_format
    cdef object namedtuple_type
    cdef HashTable inner
    cdef int value_size

    def __cinit__(self, int key_size, str value_format, object namedtuple_type, int capacity = MIN_CAPACITY):
        self.key_size = key_size
        self.value_format = value_format
        self.value_size = struct.calcsize(self.value_format)
        self.namedtuple_type = namedtuple_type
        self.inner = HashTable(self.key_size, self.value_size, capacity=capacity)

    def _check_key(self, key):
        if not isinstance(key, bytes):
            raise TypeError(f"Expected an instance of bytes, got {type(key)}")
        if len(key) != self.key_size:
            raise ValueError(f"Key must be {self.key_size} bytes long")

    def _to_binary_value(self, value):
        if not isinstance(value, self.namedtuple_type):
            raise TypeError(f"Expected an instance of {self.namedtuple_type}, got {type(value)}")
        return struct.pack(self.value_format, *value)

    def _to_namedtuple_value(self, binary_value):
        unpacked_data = struct.unpack(self.value_format, binary_value)
        return self.namedtuple_type(*unpacked_data)

    def __setitem__(self, key: bytes, value):
        self._check_key(key)
        self.inner[key] = self._to_binary_value(value)

    def __getitem__(self, key: bytes):
        self._check_key(key)
        binary_value = self.inner[key]
        return self._to_namedtuple_value(binary_value)

    def __delitem__(self, key: bytes):
        self._check_key(key)
        del self.inner[key]

    def __contains__(self, key: bytes):
        self._check_key(key)
        return key in self.inner

    def iteritems(self):
        for key, binary_value in self.inner.iteritems():
            yield (key, self._to_namedtuple_value(binary_value))

    def __len__(self):
        return len(self.inner)

    def get(self, key: bytes, default=None):
        self._check_key(key)
        try:
            binary_value = self.inner[key]
        except KeyError:
            return default
        else:
            return self._to_namedtuple_value(binary_value)

    def setdefault(self, key: bytes, default):
        self._check_key(key)
        binary_default = self._to_binary_value(default)
        binary_value = self.inner.setdefault(key, binary_default)
        return self._to_namedtuple_value(binary_value)

    def pop(self, key: bytes, default=_NoDefault):
        self._check_key(key)
        try:
            binary_value = self.inner.pop(key)
        except KeyError:
            if default is _NoDefault:
                raise
            return default
        else:
            return self._to_namedtuple_value(binary_value)

    @property
    def stats(self):
        return self.inner.stats

    def write(self, file):
        if isinstance(file, (str, bytes)):
            with open(file, 'wb') as fd:
                self._write_fd(fd)
        else:
            self._write_fd(file)

    def _write_fd(self, fd):
        keys_array = []
        values_dict = {field: [] for field in self.namedtuple_type._fields}
        for key, value in self.iteritems():
            keys_array.append(key)
            for field, val in zip(self.namedtuple_type._fields, value):
                values_dict[field].append(val)
        data = {
            'key_size': self.key_size,
            'value_size': self.value_size,
            'value_format': self.value_format,
            'namedtuple_type_name': self.namedtuple_type.__name__,
            'namedtuple_type_fields': self.namedtuple_type._fields,
            'capacity': self.inner.capacity,
            'keys': keys_array,
            'values': values_dict,
        }
        packed = msgpack.packb(data, use_bin_type=True)
        fd.write(packed)

    @classmethod
    def read(cls, file):
        if isinstance(file, (str, bytes)):
            with open(file, 'rb') as fd:
                return cls._read_fd(fd)
        else:
            return cls._read_fd(file)

    @classmethod
    def _read_fd(cls, fd):
        packed = fd.read()
        data = msgpack.unpackb(packed, raw=False)
        namedtuple_type = namedtuple(data['namedtuple_type_name'], data['namedtuple_type_fields'])
        ht = cls(key_size=data['key_size'], value_format=data['value_format'], namedtuple_type=namedtuple_type, capacity=data['capacity'])
        keys_array = data['keys']
        values_dict = data['values']
        for i in range(len(keys_array)):
            key = keys_array[i]
            value = namedtuple_type(*(values_dict[field][i] for field in namedtuple_type._fields))
            ht[key] = value
        return ht
