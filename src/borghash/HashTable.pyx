"""
HashTable: low-level hash table mapping fully random bytes keys to bytes values.
           Key and value lengths can be chosen, but are fixed thereafter.
           The keys and values are stored together in an array separate from the hashtable.
           The hashtable only stores the 32-bit indices into the key/value array.
"""
from __future__ import annotations
from typing import BinaryIO, Iterator, Any

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, memcmp
from libc.stdint cimport uint8_t, uint32_t
from libc.errno cimport errno
from posix.unistd cimport close, ftruncate, lseek, SEEK_END
from posix.fcntl cimport open as c_open, O_RDWR, O_CREAT
from posix.mman cimport mmap, munmap, MAP_SHARED, PROT_READ, PROT_WRITE

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
                 kv_grow_factor: float = 1.3,
                 path: str = None, kv_offset: int = 0) -> None:
        # the load of the ht (.table) shall be between 0.25 and 0.5, so it is fast and has few collisions.
        # it is cheap to have a low hash table load, because .table only stores uint32_t indices into the
        # .kv array.
        # the .kv array has bigger elements and is not a hash table, thus collisions and load
        # factor are no concern there. the kv_grow_factor can be relatively small.
        if key_size < 4:
            raise ValueError("key_size must be specified and must be >= 4.")
        if not value_size:
            raise ValueError("value_size must be specified and must be > 0.")
        self.ksize = key_size
        self.vsize = value_size
        # vvv mmap vvv
        self.fd = -1
        self.mmap_size = 0
        self.kv_offset = kv_offset
        if path:
            self.fd = c_open(path.encode('utf-8'), O_RDWR | O_CREAT, 0o644)
            if self.fd == -1:
                raise OSError(errno, f"Could not open {path}")
        # ^^^ mmap ^^^
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
        # vvv kv array vvv
        self.kv_grow_factor = kv_grow_factor
        self.kv_used = 0
        self.kv = NULL
        if self.fd != -1:
            # For mmap, we determine current size and capacity from file size.
            file_size = lseek(self.fd, 0, SEEK_END)
            if file_size > self.kv_offset:  # kv array is not empty
                self.mmap_size = file_size
                # map the full file, starting from offset 0
                new_kv = mmap(NULL, self.mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, self.fd, 0)
                if new_kv == <void*> -1:
                    raise OSError(errno, "mmap failed")
                self.kv = <uint8_t*> new_kv + self.kv_offset
                self.kv_capacity = <uint32_t>((self.mmap_size - self.kv_offset) // (self.ksize + self.vsize))
            else:
                self._resize_kv(int(self.initial_capacity * self.max_load_factor))
        else:
            self._resize_kv(int(self.initial_capacity * self.max_load_factor))
        # ^^^ kv array ^^^
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
        if self.fd != -1:
            if self.kv != NULL:
                munmap(self.kv - self.kv_offset, self.mmap_size)
            close(self.fd)
        else:
            free(self.kv)

    def clear(self) -> None:
        """Empty the HashTable and start from scratch."""
        self.capacity = 0
        self.used = 0
        self._resize_table(self.initial_capacity)
        self.kv_used = 0
        self._resize_kv(int(self.initial_capacity * self.max_load_factor))

    def __len__(self) -> int:
        return self.used

    cdef size_t _get_index(self, uint8_t* key):
        """Key must be perfectly random bytes, so we don't need a hash function here."""
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
            if kv_index != TOMBSTONE_BUCKET and memcmp(self.kv + kv_index * (self.ksize + self.vsize), key_ptr, self.ksize) == 0:
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
            memcpy(self.kv + kv_index * (self.ksize + self.vsize) + self.ksize, value_ptr, self.vsize)
            return

        if self.kv_used >= self.kv_capacity:
            # "+ 1" ensures growth even for very small or 0 capacity.
            self._resize_kv(int(self.kv_capacity * self.kv_grow_factor + 1))
        if self.kv_used >= self.kv_capacity:
            # Should never happen. See "RESERVED" constant - we allow almost 4Gi kv entries.
            # For a typical 256-bit key and a small 32-bit value that would already consume 176GiB+
            # memory (plus spikes to even more when hashtable or kv arrays get resized).
            raise RuntimeError("KV array is full")

        kv_index = self.kv_used
        memcpy(self.kv + kv_index * (self.ksize + self.vsize), key_ptr, self.ksize)
        memcpy(self.kv + kv_index * (self.ksize + self.vsize) + self.ksize, value_ptr, self.vsize)
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
            return self.kv[kv_index * (self.ksize + self.vsize) + self.ksize : kv_index * (self.ksize + self.vsize) + self.ksize + self.vsize]
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
            memset(self.kv + kv_index * (self.ksize + self.vsize), 0, self.ksize + self.vsize)
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
                key = self.kv[kv_index * (self.ksize + self.vsize) : kv_index * (self.ksize + self.vsize) + self.ksize]
                value = self.kv[kv_index * (self.ksize + self.vsize) + self.ksize : kv_index * (self.ksize + self.vsize) + self.ksize + self.vsize]
                yield key, value

    cpdef void update_table_only(self, bytes key, uint32_t kv_index):
        cdef size_t index
        self._lookup_index(<uint8_t*> key, &index)
        # index is either a bucket containing the key (if it already existed)
        # or it is the first free/tombstone bucket in the probe sequence.
        if self.table[index] == FREE_BUCKET or self.table[index] == TOMBSTONE_BUCKET:
            self.used += 1
        self.table[index] = kv_index
        if self.used + self.tombstones > self.capacity * self.max_load_factor:
            self._resize_table(int(self.capacity * self.grow_factor))

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
                index = self._get_index(self.kv + kv_index * (self.ksize + self.vsize))
                while new_table[index] != FREE_BUCKET:
                    index = (index + 1) % new_capacity
                new_table[index] = kv_index

        free(self.table)
        self.table = new_table
        self.tombstones = 0

    cdef void _resize_kv(self, size_t new_capacity):
        # We must never use kv indices >= RESERVED; thus, we'll never need more capacity either.
        cdef size_t capacity = min(new_capacity, <size_t> RESERVED - 1)
        cdef size_t new_mmap_size
        cdef void* new_kv
        self.stats_resize_kv += 1
        if self.fd != -1:
            new_mmap_size = self.kv_offset + capacity * (self.ksize + self.vsize) * sizeof(uint8_t)
            if self.kv != NULL:
                # Don't shrink automatically during resize if we already have space.
                # This prevents truncating an existing file's data when it's opened
                # with a smaller initial_capacity than the file already contains.
                # HOWEVER, if capacity is kv_used, we might be in shrink_to_fit.
                # Let's allow shrinking if capacity < self.kv_capacity.
                if new_mmap_size <= self.mmap_size and capacity >= self.kv_capacity:
                    return
                munmap(self.kv - self.kv_offset, self.mmap_size)
            if ftruncate(self.fd, new_mmap_size) == -1:
                raise OSError(errno, "ftruncate failed")
            new_kv = mmap(NULL, new_mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, self.fd, 0)
            if new_kv == <void*> -1:
                raise OSError(errno, "mmap failed")
            self.kv = <uint8_t*> new_kv + self.kv_offset
            self.mmap_size = new_mmap_size
        else:
            # realloc is already highly optimized (in Linux). By using mremap internally, only the peak address space usage is "old size" + "new size", while the peak memory usage is only "new size".
            self.kv = <uint8_t*> realloc(self.kv, capacity * (self.ksize + self.vsize) * sizeof(uint8_t))
        self.kv_capacity = <uint32_t> capacity

    def shrink_to_fit(self) -> None:
        """Shrink the KV array and the file to the actually used size."""
        self._resize_kv(self.kv_used)
        if self.fd != -1:
            # _resize_kv already calls ftruncate to new_mmap_size,
            # which is kv_offset + capacity * entry_size.
            # Here capacity is self.kv_used.
            pass

    def k_to_idx(self, key: bytes) -> int:
        """
        Return the key's index in the keys array (index is stable while in memory).
        This can be used to "abbreviate" a known key (e.g., 256-bit key -> 32-bit index).
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
        For a given index, return the key stored at that index in the kv array.
        This is the reverse of k_to_idx (e.g., 32-bit index -> 256-bit key).
        """
        cdef uint32_t kv_index = <uint32_t> idx
        if kv_index >= self.kv_used:
             raise KeyError(f"Index {kv_index} out of range (kv_used={self.kv_used})")
        return self.kv[kv_index * (self.ksize + self.vsize) : kv_index * (self.ksize + self.vsize) + self.ksize]

    def kv_to_idx(self, key: bytes, value: bytes) -> int:
        """
        Return the key's/value's index in the kv array (index is stable while in memory).
        This can be used to "abbreviate" a known key/value pair (e.g., 256-bit key + 32-bit value -> 32-bit index).
        """
        if len(key) != self.ksize:
            raise ValueError("Key size does not match the defined size")
        if len(value) != self.vsize:
            raise ValueError("Value size does not match the defined size")
        cdef size_t index
        cdef uint32_t kv_index
        if self._lookup_index(<uint8_t*> key, &index):
            kv_index = self.table[index]
            value_found = self.kv[kv_index * (self.ksize + self.vsize) + self.ksize : kv_index * (self.ksize + self.vsize) + self.ksize + self.vsize]
            if value == value_found:
                return kv_index
        raise KeyError("Key/Value not found")

    def idx_to_kv(self, idx: int) -> tuple[bytes, bytes]:
        """
        For a given index, return the key/value stored at that index in the kv array.
        This is the reverse of kv_to_idx (e.g., 32-bit index -> 256-bit key + 32-bit value).
        """
        cdef uint32_t kv_index = <uint32_t> idx
        key = self.kv[kv_index * (self.ksize + self.vsize) : kv_index * (self.ksize + self.vsize) + self.ksize]
        value = self.kv[kv_index * (self.ksize + self.vsize) + self.ksize : kv_index * (self.ksize + self.vsize) + self.ksize + self.vsize]
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
