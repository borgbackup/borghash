from setuptools import setup, Extension
from Cython.Build import cythonize

ext_modules = [
    Extension("borghash", ["borghash.pyx"]),
]
setup(
    name='borghash',
#    packages=['borghash'],
    ext_modules=cythonize(ext_modules))
