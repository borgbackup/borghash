from setuptools import Extension, setup

try:
    from Cython.Build import cythonize
except ImportError:
    cythonize = None  # we don't have cython installed

ext = '.pyx' if cythonize else '.c'

extensions = [
    Extension("borghash.HashTable", ["src/borghash/HashTable" + ext]),
    Extension("borghash.HashTableNT", ["src/borghash/HashTableNT" + ext]),
]

if cythonize:
    extensions = cythonize(extensions, language_level="3str")

setup(
    package_data={"borghash": ["*.pxd", "*.pyx"]},
    ext_modules=extensions,
)
