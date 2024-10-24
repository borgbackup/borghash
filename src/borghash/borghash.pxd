from libc.stdint cimport uint8_t, uint32_t

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

    cdef int _get_index(self, uint8_t* key)
    cdef int _lookup_index(self, uint8_t* key_ptr, int* index_ptr)
    cdef void _resize_table(self, int new_capacity)
    cdef void _resize_kv(self, int new_capacity)


cdef class HashTableNT:
    cdef int key_size
    cdef str value_format
    cdef object namedtuple_type
    cdef HashTable inner
    cdef int value_size
