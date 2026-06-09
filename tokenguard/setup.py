# setup.py — Cython build for TokenGate core execution module
#
# Compiles core_pinned_staggered_queue.py to a .pyd/.so extension.
# admission_gate.py intentionally stays as plain .py (subprocess bootstrap safety).
#
# Usage:
#   python setup.py build_ext --inplace
#
# Output:
#   core_pinned_staggered_queue.cpython-<ver>-<platform>.pyd  (Windows)
#   core_pinned_staggered_queue.cpython-<ver>-<platform>.so   (Linux/macOS)
#
# After building, drop the .pyd/.so into the tokengate package directory
# alongside the existing .py files. Python will prefer the compiled extension.

from setuptools import setup, Extension
from Cython.Build import cythonize
import sys

# ---------------------------------------------------------------------------
# Compiler directives — applied globally to the compiled module
# ---------------------------------------------------------------------------
# These are safe production defaults. Don't enable boundscheck=False or
# wraparound=False unless you've verified all list/dict accesses are bounded.

COMPILER_DIRECTIVES = {
    'language_level':      '3',      # Python 3 semantics throughout
    'boundscheck':         False,    # Skip index bounds checks (lists/arrays)
    'wraparound':          False,    # Disable negative index wraparound
    'nonecheck':           False,    # Skip None checks on typed variables
    'cdivision':           True,     # C-style integer division (no ZeroDivision check)
    'infer_types':         True,     # Let Cython infer C types where possible
    'optimize.use_switch': True,     # Compile int comparisons to C switch statements
    # NOTE: no cdef class — WorkerTaskQueue base is pure Python (admission_gate.py).
    # Compiles as a standard extension. All directive-level speedups still apply:
    # typed local variables, C int loops, no boundscheck overhead.
}

# ---------------------------------------------------------------------------
# Extension definition
# ---------------------------------------------------------------------------

extensions = [
    Extension(
        name='core_pinned_staggered_queue',
        sources=['core_pinned_staggered_queue.py'],
        extra_compile_args=['-O2'],
        include_dirs=['/clang64/include/python3.12'],  # replace 3.x with your actual version
    ),
]

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

setup(
    name='tokengate_core',
    ext_modules=cythonize(
        extensions,
        compiler_directives=COMPILER_DIRECTIVES,
        annotate=True,          # Generates core_pinned_staggered_queue.html
                                # Yellow lines = Python overhead still present
                                # White lines  = pure C (what you want)
    ),
)