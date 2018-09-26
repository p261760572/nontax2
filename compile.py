# coding=utf-8


from distutils.core import setup
from distutils.extension import Extension

from Cython.Build import cythonize
from Cython.Distutils import build_ext

ext_modules = [
    Extension("*", ["app/*.py"]),
]
setup(
    name='nontax',
    cmdclass={'build_ext': build_ext},
    ext_modules= cythonize(ext_modules)
)

"""
python3 compile.py build_ext --inplace
find . -name "*.cpython-35m-x86_64-linux-gnu.so" | xargs rename .cpython-35m-x86_64-linux-gnu.so .so
find app -name "*.c" | xargs rm
find app -name "*.py" | xargs rm
rm -rf build
"""
