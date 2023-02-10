from setuptools import setup, Extension

setup(
    name="cdfk",
    version="1.0",
    packages=["cdfk"],
    ext_modules=[Extension("cdflow", ["cdfk/dflow.c"])],
)
