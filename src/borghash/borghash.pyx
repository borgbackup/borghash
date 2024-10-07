# distutils: language=c++
"""
cyhash - a hashtable in cython mapping 256bit fully random keys to
a bytes value (length can be chosen, but is fixed afterwards).
"""

from libc.stdint cimport uint32_t, uint8_t
from libcpp.vector cimport vector
from libcpp.utility cimport pair

import msgpack


cdef vector[uint32_t] _key_to_vector(bytes key):
    """256bit key to vector of uint32_t."""
    cdef vector[uint32_t] bkey
    assert len(key) * 8 == 256, f"Expected 256bit key, got {len(key) * 8}bit!"
    for i in range(0, 32, 4):  # Process the key in 4-byte (32-bit) chunks
        part = <uint32_t> (int.from_bytes(key[i:i + 4], byteorder='little'))
        bkey.push_back(part)
    return bkey


cdef bytes _vector_to_key(vector[uint32_t] bkey):
    """vector of uint32_t to 256bit key."""
    cdef bytes key_bytes = b''.join([k.to_bytes(4, byteorder='little') for k in bkey])
    return key_bytes


cdef vector[uint8_t] _value_to_vector(bytes value, int value_size):
    """value to vector of uint8_t."""
    cdef vector[uint8_t] bvalue
    assert len(value) == value_size, f"Expected value length of {value_size} bytes, got {len(value)}"
    bvalue.reserve(value_size)
    for i in range(value_size):
        bvalue.push_back(value[i])
    return bvalue


cdef bytes _vector_to_value(vector[uint8_t] bvalue):
    """vector of uint8_t to value."""
    return bytes([bvalue[i] for i in range(bvalue.size())])


cdef class HashTable:
    cdef int size
    cdef int value_size
    cdef float max_load_factor
    cdef float min_load_factor
    cdef int num_entries
    cdef vector[vector[pair[vector[uint32_t], vector[uint8_t]]]] table  # Table with chaining for collisions

    def __init__(self, size=100, value_size=4, max_load_factor=0.75, min_load_factor=0.3):
        self.size = size
        self.value_size = value_size  # Size of the stored values in bytes
        self.max_load_factor = max_load_factor
        self.min_load_factor = min_load_factor
        self.num_entries = 0
        self.table = vector[vector[pair[vector[uint32_t], vector[uint8_t]]]](<int> size)

    cdef int _index(self, vector[uint32_t] key_vector, int table_size):
        cdef uint32_t idx = key_vector[0] % table_size
        return idx

    cpdef insert(self, key, value):
        cdef vector[uint32_t] bkey = _key_to_vector(key)
        cdef vector[uint8_t] bvalue = _value_to_vector(value, self.value_size)

        if self.num_entries / self.size > self.max_load_factor:
            self._resize(int(self.size * 1.5))

        cdef int idx = self._index(bkey, self.size)

        if self.table[idx].empty():
            # Initialize the vector at this slot if empty
            self.table[idx] = vector[pair[vector[uint32_t], vector[uint8_t]]]()

        for i in range(self.table[idx].size()):
            if self.table[idx][i].first == bkey:
                self.table[idx][i].second = bvalue  # Update the value if key exists
                return

        # If key does not exist, add a new pair
        self.table[idx].push_back(pair[vector[uint32_t], vector[uint8_t]](bkey, bvalue))
        self.num_entries += 1

    cpdef bytes lookup(self, key):
        cdef vector[uint32_t] bkey = _key_to_vector(key)
        cdef int idx = self._index(bkey, self.size)

        if not self.table[idx].empty():
            for i in range(self.table[idx].size()):
                if self.table[idx][i].first == bkey:
                    return _vector_to_value(self.table[idx][i].second)

        raise KeyError(f'Key {key} not found')

    cpdef void remove(self, key):
        cdef vector[uint32_t] bkey = _key_to_vector(key)
        cdef int idx = self._index(bkey, self.size)

        if not self.table[idx].empty():
            for i in range(self.table[idx].size()):
                if self.table[idx][i].first == bkey:
                    self.table[idx].erase(self.table[idx].begin() + <int> i)
                    self.num_entries -= 1
                    if self.num_entries / self.size < self.min_load_factor:
                        self._resize(max(1, int(self.size * 0.5)))
                    return

        raise KeyError(f'Key {key} not found')

    cdef void _resize(self, int new_size):
        cdef vector[vector[pair[vector[uint32_t], vector[uint8_t]]]] new_table = vector[
            vector[pair[vector[uint32_t], vector[uint8_t]]]](new_size)
        cdef int new_idx
        for bucket in self.table:
            for kvp in bucket:
                new_idx = self._index(kvp.first, new_size)
                if new_table[new_idx].empty():
                    new_table[new_idx] = vector[pair[vector[uint32_t], vector[uint8_t]]]()

                new_table[new_idx].push_back(kvp)

        self.table = new_table
        self.size = new_size

    cpdef void save(self, file):
        cdef object data = {
            'size': self.size,
            'value_size': self.value_size,
            'max_load_factor': self.max_load_factor,
            'min_load_factor': self.min_load_factor,
            'num_entries': self.num_entries,
            'table': [
                [(_vector_to_key(pair.first), _vector_to_value(pair.second)) for pair in bucket]
                for bucket in self.table
            ]
        }
        file.write(msgpack.packb(data))

    @staticmethod
    def load(file) -> 'HashTable':
        cdef vector[uint32_t] bkey
        cdef vector[uint8_t] bvalue
        data = msgpack.unpackb(file.read())

        ht = HashTable(
            size=data['size'],
            value_size=data['value_size'],
            max_load_factor=data['max_load_factor'],
            min_load_factor=data['min_load_factor']
        )
        ht.num_entries = data['num_entries']
        ht.table = vector[vector[pair[vector[uint32_t], vector[uint8_t]]]](ht.size)

        for bucket_data in data['table']:
            bucket = vector[pair[vector[uint32_t], vector[uint8_t]]]()
            for key, value in bucket_data:
                print(key, value)
                bkey = _key_to_vector(key)
                bvalue = _value_to_vector(value, ht.value_size)
                bucket.push_back(pair[vector[uint32_t], vector[uint8_t]](bkey, bvalue))
            ht.table.push_back(bucket)

        ht._resize(ht.size)  # important: recompute correct bucket indexes
        return ht

    def items(self):
        return HashTableIterator(self)

    def __len__(self):
        return self.num_entries


cdef class HashTableIterator:
    cdef HashTable _hashtable
    cdef int _bucket_index
    cdef vector[pair[vector[uint32_t], vector[uint8_t]]] _pairs
    cdef int _pair_index

    def __init__(self, HashTable hashtable):
        self._hashtable = hashtable
        self._bucket_index = -1
        self._pair_index = 0
        self._advance_bucket()

    def _advance_bucket(self):
        self._bucket_index += 1
        while (self._bucket_index < self._hashtable.size and
               self._hashtable.table[self._bucket_index].empty()):
            self._bucket_index += 1

        if self._bucket_index < self._hashtable.size:
            self._pairs = self._hashtable.table[self._bucket_index]
            self._pair_index = 0
        else:
            self._pairs = []

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            # If we have more pairs in the current bucket
            if self._pair_index < self._pairs.size():
                pair = self._pairs[self._pair_index]
                key = _vector_to_key(pair.first)
                value = _vector_to_value(pair.second)
                self._pair_index += 1
                return key, value

            # Otherwise, move to the next bucket
            self._advance_bucket()

            # If we have gone through all buckets, stop iteration
            if self._bucket_index >= self._hashtable.size:
                raise StopIteration
