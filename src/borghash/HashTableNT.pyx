"""
HashTableNT: wrapper around HashTable, providing namedtuple values and serialization.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import BinaryIO, Iterator, Any

from collections import namedtuple
import json
import struct

from .HashTable import HashTable, MIN_CAPACITY, _fill

MAGIC = b"BORGHASH"
assert len(MAGIC) == 8
VERSION = 1  # version of the on-disk (serialized) format produced by .write().
HEADER_FMT = "<8sII"  # magic, version, meta length

BYTE_ORDER = dict(big=">", little="<", network="!", native="=")  # struct format chars

_NoDefault = object()

cdef class HashTableNT:
    def __init__(self, items=None, *,
                 key_size: int, value_type: Any, value_format: Any,
                 capacity: int = MIN_CAPACITY, byte_order="little") -> None:
        if not isinstance(key_size, int) or not key_size > 0:
            raise ValueError("key_size must be an integer and > 0.")
        if type(value_type) is not type:
            raise TypeError("value_type must be a namedtuple type.")
        if not isinstance(value_format, tuple):
            raise TypeError("value_format must be a namedtuple instance.")
        if value_format._fields != value_type._fields:
            raise TypeError("value_format's and value_type's element names must correspond.")
        if not all(isinstance(fmt, str) and len(fmt) > 0 for fmt in value_format):
            raise ValueError("value_format's elements must be str and non-empty.")
        if byte_order not in BYTE_ORDER:
            raise ValueError("byte_order must be one of: {','.join(BYTE_ORDER.keys())}")
        self.key_size = key_size
        self.value_type = value_type
        self.value_format = value_format
        self.byte_order = byte_order
        self.value_struct = struct.Struct(BYTE_ORDER[byte_order] + "".join(value_format))
        self.value_size = self.value_struct.size
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

    def update(self, other=(), /, **kwds):
        """Like dict.update, but other can also be a HashTableNT instance."""
        if isinstance(other, HashTableNT):
            for key, value in other.items():
                self[key] = value
        elif isinstance(other, Mapping):
            for key in other:
                self[key] = other[key]
        elif hasattr(other, "keys"):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value
        for key, value in kwds.items():
            self[key] = value

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
            'byte_order': self.byte_order,
            'value_type_name': self.value_type.__name__,
            'value_type_fields': self.value_type._fields,
            'value_format_name': self.value_format.__class__.__name__,
            'value_format_fields': self.value_format._fields,
            'value_format': self.value_format,
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
        value_format_t = namedtuple(meta['value_format_name'], meta['value_format_fields'])
        value_format = value_format_t(*meta['value_format'])
        ht = cls(key_size=meta['key_size'], value_format=value_format, value_type=value_type,
                 capacity=meta['capacity'], byte_order=meta['byte_order'])
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
