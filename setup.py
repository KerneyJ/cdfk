from setuptools import setup, Extension

setup(
    name="cdfk",
    version="1.0",
    ext_modules=[Extension("cdfk.backend", ["cdfk-pkg/backend/dflow.c"])],
    # packages=["cdfk", "cdfk.backend"],
    # package_dir={"cdfk": "cdfk-pkg"}
)
