from setuptools import setup
from Cython.Build import cythonize

setup(
    name="ReconciliationEngine",
    ext_modules=cythonize([
        "src/cython_ext/reconciliation_engine.pyx",
        "src/cython_ext/withdrawals_matcher.pyx"
    ]),
    zip_safe=False,
)
