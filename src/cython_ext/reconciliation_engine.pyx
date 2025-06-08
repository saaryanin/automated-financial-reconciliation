from setuptools import setup
from Cython.Build import cythonize
from withdrawals_matcher import ReconciliationEngine

setup(
    name="ReconciliationEngine",
    ext_modules=cythonize("src/reconciliation_engine.pyx"),  # or "cython_ext/reconciliation_engine.pyx"
    zip_safe=False,
)

def run_engine(*args):
    engine = ReconciliationEngine(...)
    return engine.do_something()
