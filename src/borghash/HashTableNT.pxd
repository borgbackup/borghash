from .HashTable cimport HashTable

cdef class HashTableNT:
    cdef public int key_size
    cdef public object byte_order
    cdef public object value_type
    cdef public object value_format
    cdef public object value_struct
    cdef public int value_size
    cdef public HashTable inner
