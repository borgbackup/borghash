cdef class HashTableNT:
    cdef int key_size
    cdef object byte_order
    cdef object value_type
    cdef object value_format
    cdef object value_struct
    cdef int value_size
    cdef object inner
