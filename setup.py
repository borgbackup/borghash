from setuptools import setup
from Cython.Build import cythonize

setup(
    package_data=dict(borghash=["borghash.pxd"]),
    ext_modules=cythonize("borghash.pyx")
)
