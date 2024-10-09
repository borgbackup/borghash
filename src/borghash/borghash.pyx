"""
borghash - a hashtable in cython mapping fully random bytes keys to bytes values.
key and values length can be chosen, but is fixed afterwards.
"""
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, memcmp
from libc.stdint cimport uint8_t, uint32_t
import msgpack


MIN_CAPACITY = 1000  # never shrink the hash table below this capacity

cdef uint32_t FREE_BUCKET = 0xFFFFFFFF
cdef uint32_t TOMBSTONE_BUCKET = 0xFFFFFFFE

_NoDefault = object()

cdef struct KeyValue:
    uint8_t key[32]
    uint8_t value[32]

cdef class HashTable:
    cdef int ksize, vsize
    cdef int capacity, used, tombstones
    cdef float max_load_factor, min_load_factor, shrink_factor, grow_factor
    cdef uint32_t* table
    cdef int kv_capacity, kv_used
    cdef float kv_grow_factor
    cdef KeyValue* kv

    def __init__(self, key_size: int, value_size: int, capacity: int = MIN_CAPACITY,
                 max_load_factor: float = 0.5, min_load_factor: float = 0.10,
                 shrink_factor: float = 0.4, grow_factor: float = 2.0,
                 kv_grow_factor: float = 1.3):
        # the load of the ht (.table) shall be between 0.25 and 0.5, so it is fast and has few collisions.
        # it is cheap to have a low hash table load, because .table only stores uint32_t indexes into .kv.
        # the kv array has bigger elements and is not a hash table, thus collisions and load factor are
        # no concern there. the kv_grow_factor can be relatively small.
        self.ksize = key_size
        self.vsize = value_size
        # vvv hash table vvv
        self.capacity = capacity
        self.used = 0
        self.tombstones = 0
        self.max_load_factor = max_load_factor
        self.min_load_factor = min_load_factor
        self.shrink_factor = shrink_factor
        self.grow_factor = grow_factor
        self.table = <uint32_t*> malloc(self.capacity * sizeof(uint32_t))
        for i in range(self.capacity):
            self.table[i] = FREE_BUCKET
        # ^^^ hash table ^^^
        # vvv kv array vvv
        self.kv_capacity = int(capacity * max_load_factor)
        self.kv_used = 0
        self.kv_grow_factor = kv_grow_factor
        self.kv = <KeyValue*> malloc(self.kv_capacity * sizeof(KeyValue))
        # ^^^ kv array ^^^

    def __del__(self):
        free(self.table)
        free(self.kv)

    def __len__(self):
        return self.used

    cdef int get_index(self, uint8_t* key):
        """key must be a perfectly random distributed value, so we don't need a hash function here."""
        cdef uint32_t key32 = (key[0] << 24) | (key[1] << 16) | (key[2] << 8) | key[3]
        return key32 % self.capacity

    def __setitem__(self, key: bytes, value: bytes):
        if len(key) != self.ksize or len(value) != self.vsize:
            raise ValueError("Key or value size does not match the defined sizes")

        cdef uint8_t* key_ptr = <uint8_t*> key
        cdef uint8_t* value_ptr = <uint8_t*> value
        cdef uint32_t kv_index

        cdef int index = self.get_index(key_ptr)
        while self.table[index] not in (FREE_BUCKET, TOMBSTONE_BUCKET):
            kv_index = self.table[index]
            if memcmp(self.kv[kv_index].key, key_ptr, self.ksize) == 0:
                memcpy(<void *> self.kv[kv_index].value, value_ptr, self.vsize)
                return
            index = (index + 1) % self.capacity

        if self.kv_used >= self.kv_capacity:
            self.resize_kv(int(self.kv_capacity * self.kv_grow_factor))

        kv_index = self.kv_used
        memcpy(<void *> self.kv[kv_index].key, key_ptr, self.ksize)
        memcpy(<void *> self.kv[kv_index].value, value_ptr, self.vsize)
        self.kv_used += 1

        if self.table[index] == TOMBSTONE_BUCKET:
            self.tombstones -= 1
        self.used += 1
        self.table[index] = kv_index

        if self.used + self.tombstones > self.capacity * self.max_load_factor:
            self.resize_table(int(self.capacity * self.grow_factor))

    cdef uint32_t _lookup_kv_index(self, uint8_t* key_ptr):
        cdef int index = self.get_index(key_ptr)
        cdef int original_index = index
        cdef uint32_t kv_index
        while self.table[index] != FREE_BUCKET:
            kv_index = self.table[index]
            if self.table[index] != TOMBSTONE_BUCKET and memcmp(self.kv[kv_index].key, key_ptr, self.ksize) == 0:
                return kv_index
            index = (index + 1) % self.capacity
            if index == original_index:
                break
        return <uint32_t> 0xffffffff  # not found

    def __contains__(self, key: bytes):
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")

        return self._lookup_kv_index(<uint8_t*> key) != <uint32_t> 0xffffffff

    def __getitem__(self, key: bytes):
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")

        cdef uint32_t kv_index = self._lookup_kv_index(<uint8_t*> key)
        if kv_index == <uint32_t> 0xffffffff:
            raise KeyError("Key not found")
        else:
            return self.kv[kv_index].value[:self.vsize]

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
            if default != _NoDefault:
                return default
            raise

    def __delitem__(self, key: bytes):
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")

        cdef uint8_t* key_ptr = <uint8_t*> key

        cdef int index = self.get_index(key_ptr)
        cdef int original_index = index
        cdef uint32_t kv_index

        while self.table[index] != FREE_BUCKET:
            kv_index = self.table[index]
            if kv_index != TOMBSTONE_BUCKET and memcmp(self.kv[kv_index].key, key_ptr, self.ksize) == 0:
                memset(self.kv[kv_index].key, 0, self.ksize)
                memset(self.kv[kv_index].value, 0, self.vsize)
                self.table[index] = TOMBSTONE_BUCKET
                self.used -= 1
                self.tombstones += 1

                # Resize down if necessary
                if self.used < self.capacity * self.min_load_factor:
                    new_capacity = max(int(self.capacity * self.shrink_factor), MIN_CAPACITY)
                    self.resize_table(new_capacity)
                return
            index = (index + 1) % self.capacity
            if index == original_index:
                break
        raise KeyError("Key not found")

    def iteritems(self):
        cdef int i
        cdef uint32_t kv_index
        for i in range(self.capacity):
            kv_index = self.table[i]
            if kv_index not in (FREE_BUCKET, TOMBSTONE_BUCKET):
                key = self.kv[kv_index].key[:self.ksize]
                value = self.kv[kv_index].value[:self.vsize]
                yield key, value

    cdef void resize_table(self, int new_capacity):
        cdef int i, index
        cdef uint32_t kv_index
        cdef uint32_t* new_table = <uint32_t*> malloc(new_capacity * sizeof(uint32_t))
        for i in range(new_capacity):
            new_table[i] = FREE_BUCKET

        current_capacity = self.capacity
        self.capacity = new_capacity
        for i in range(current_capacity):
            kv_index = self.table[i]
            if kv_index not in (FREE_BUCKET, TOMBSTONE_BUCKET):
                index = self.get_index(self.kv[kv_index].key)
                while new_table[index] != FREE_BUCKET:
                    index = (index + 1) % new_capacity
                new_table[index] = kv_index

        free(self.table)
        self.table = new_table
        self.tombstones = 0

    cdef void resize_kv(self, int new_size):
        self.kv = <KeyValue*> realloc(self.kv, new_size * sizeof(KeyValue))
        self.kv_capacity = new_size

    def write(self, file):
        if isinstance(file, (str, bytes)):
            with open(file, 'wb') as fd:
                self._write_fd(fd)
        else:
            self._write_fd(file)

    def _write_fd(self, fd):
        cdef uint32_t kv_index
        entries = []
        for i in range(self.capacity):
            kv_index = self.table[i]
            if kv_index not in (FREE_BUCKET, TOMBSTONE_BUCKET):
                key_bytes = self.kv[kv_index].key[:self.ksize]
                value_bytes = self.kv[kv_index].value[:self.vsize]
                entries.append((key_bytes, value_bytes))
        data = {
            'ksize': self.ksize,
            'vsize': self.vsize,
            'capacity': self.capacity,
            'entries': entries
        }
        packed = msgpack.packb(data)
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
        ht = cls(key_size=data['ksize'], value_size=data['vsize'], capacity=data['capacity'])
        for key, value in data['entries']:
            ht[key] = value
        return ht
