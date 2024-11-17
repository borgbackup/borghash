"""
Demonstration of borghash.
"""

def demo():
    print("BorgHash demo")
    print("=============")
    print("Code:")
    code = """
from tempfile import NamedTemporaryFile
from time import time
from collections import namedtuple

from .HashTableNT import HashTableNT

count = 50000
value_type = namedtuple("Chunk", ["refcount", "size"])
value_format_t = namedtuple("ChunkFormat", ["refcount", "size"])
value_format = value_format_t(refcount="I", size="I")
# 256bit (32Byte) key, 2x 32bit (4Byte) values
ht = HashTableNT(key_size=32, value_type=value_type, value_format=value_format)

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


if __name__ == "__main__":
    demo()
