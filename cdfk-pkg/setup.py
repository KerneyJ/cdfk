from setuptools import setup, Extension

setup(
    name="cdfk",
    version="1.0",
    ext_modules=[Extension("cdfk", ["bind.c", "dflow.c"])]
)
