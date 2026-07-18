# -*- coding: utf-8 -*-
# unhashable_checker.py
"""
Universal hashability shield for the TokenGate routing layer.

Architecture
============
Named library checks handle the types that need special treatment (e.g.
torch.Tensor raises RuntimeError instead of TypeError, so it slips past the
fast path).  Protocol-based structural detection handles the rest: any
array-like, sequence, mapping, or graph object from any library — present
or future — is caught by shape/dtype duck-typing before the fallback.

This means the file does NOT need updating when a new array library ships.

Layers (executed in order)
===========================
 1. Fast path           hash() already works — return unchanged
 1b. O(1) dispatch      exact-type lookup in _DISPATCH before isinstance chain
 2. Python built-ins    list, dict, set, bytearray, memoryview, slice,
                        array.array, tuple-containing-unhashable
 3. Standard library    deque, OrderedDict, defaultdict, Counter,
                        ChainMap, io.IOBase, xml.etree.Element
 4. NumPy               ndarray and all subclasses (matrix, memmap, etc.)
 5. Pandas              DataFrame, Series, Index, MultiIndex,
                        Categorical, all ExtensionArrays
 6. PyTorch             Tensor, nn.Parameter  (RuntimeError guard)
 7. TensorFlow / Keras  Tensor, Variable, EagerTensor
 8. JAX                 jax.Array, DeviceArray, ShapedArray
 9. Scientific arrays   CuPy, Dask, Xarray, Polars, PyArrow,
                        Awkward Array, Sparse, Zarr, H5Py, Numba
10. Graph libraries     NetworkX Graph/DiGraph/MultiGraph etc.
11. Geospatial          Shapely geometries, GeoPandas GeoDataFrame/GeoSeries
12. Document parsing    lxml etree Element, BeautifulSoup Tag,
                        xml.etree.ElementTree.Element
13. Database / ORM      SQLAlchemy Query/Table/Column,
                        Django QuerySet, Peewee Model
14. Distributed         PySpark DataFrame/RDD/Column
15. GUI / Game          Pygame Surface, Qt QObject subclasses,
                        PyGlet objects
16. Image / AV          PIL Image (non-hashable subclasses),
                        OpenCV, imageio, av Frame
17. GPU / Graphics      ModernGL (all object types), PyOpenGL,
                        wgpu objects
18. Structural: array   Any object with .shape + .dtype not caught above
19. Structural: mapping Any object with .keys()/.values()/.items()
20. Structural: graph   Any object with .nodes / .edges attributes
21. Structural: iter    Any other iterable with __len__ + __iter__
22. Dataclass           @dataclass(eq=True, frozen=False) — __hash__ is None
23. Generic fallback    repr() capped at 256 chars, then id()

Fingerprint semantics
=====================
This produces ROUTING FINGERPRINTS, not equality witnesses.

    Array-like   →  (lib_hint, shape, dtype_str)
    Container    →  recursively frozen equivalent
    GPU object   →  (type_name, id)          [stable within session]
    Dataclass    →  (type_name, id)          [identity routing]
    Unknown      →  (type_name, repr[:256])  [best-effort stability]

The array fingerprint deliberately ignores contents because hashing a
16×16×16 float32 volume on every token submission costs real time.
Shape + dtype is the correct grain for core-domain routing.

Public API
==========
    make_hashable(obj)        →  Hashable
    fast_make_hashable(obj)   →  Hashable  (builtins + stdlib only)
    safe_args_key(args)       →  tuple[Hashable, ...]
    is_hashable(obj)          →  bool
    HashPolicy                →  Enum (NONE | FAST | STANDARD | FULL)
    DigestPolicy              →  Enum (FULL | SHORT | FAST | MINIMAL)
"""
from __future__ import annotations

import array as _array
import collections
import io
from enum import Enum
from typing import Any, Hashable, cast

# Internal helpers
_SENTINEL = object()  # Detects __hash__ = None vs __hash__ not defined
_REPR_CAP = 256  # Max chars taken from repr() in fallback


def _safe_repr(obj: Any) -> str:
    try:
        r = repr(obj)
        return r[:_REPR_CAP] if len(r) > _REPR_CAP else r
    except Exception:
        return f"<{type(obj).__name__} at {id(obj):#x}>"


def _array_fingerprint(obj: Any, hint: str = '') -> tuple[Any, ...]:
    """Produce a (hint, shape, dtype) fingerprint for any array-like object."""
    shape = getattr(obj, 'shape', None)
    dtype = getattr(obj, 'dtype', None)
    name = hint or type(obj).__name__
    return name, shape, str(dtype) if dtype is not None else None


# Hash Policy — user-controlled hashing depth per operation
class HashPolicy(Enum):
    """Controls how deeply token args are hashed at routing time.

    Set via task_token_guard tags={"hash_policy": HashPolicy.FAST}.

    NONE     — no arg hashing; route_args is always ().
               Use for high-frequency ops with no sticky/conductor anchoring.
    FAST     — builtins and stdlib only (layers 1–3). Unrecognised types get
               identity routing (type_name, id). No library detection chain.
    STANDARD — full make_hashable pipeline (default, unchanged behaviour).
    FULL     — same as STANDARD; reserved for explicit subclass-fallthrough intent.
    """
    NONE = "none"
    FAST = "fast"
    STANDARD = "standard"
    FULL = "full"


# Digest Policy — user-controlled conductor seed size
class DigestPolicy(Enum):
    """Controls the hash algorithm and output length used for conductor seed generation.

    Set via task_token_guard tags={"digest_policy": DigestPolicy.FAST}.

    FULL    — SHA-256, 64-char hex (default, current behaviour).
              Maximum collision resistance. Use when token volume is very
              high or lead token lifetime is long.

    SHORT   — SHA-256 truncated to 16 chars (64-bit space).
              Safe at any realistic TokenGate volume. Faster dict ops and
              key comparisons than FULL.

    FAST    — BLAKE2s 8-byte digest → 16-char hex.
              Fastest cryptographic option. Same 64-bit collision space
              as SHORT, lower compute cost than SHA-256. Preferred for
              high-frequency lead operations.

    MINIMAL — SHA-256 truncated to 8 chars (32-bit space).
              Lowest overhead. Safe at low token volume with short-lived
              leads. Collisions at this level are benign — they merge
              domain chains into a shared mailbox cluster, shifting load
              distribution rather than corrupting data. The least-loaded
              mechanism compensates, but heavy tasks may fall back from
              their primary core under saturation.
    """
    FULL = "full"
    SHORT = "short"
    FAST = "fast"
    MINIMAL = "minimal"


# Populated at module load by _build_dispatch() after make_hashable is defined.
_DISPATCH: dict[type, Any] = {}


# Public API
def make_hashable(obj: Any) -> Hashable:  # noqa: C901
    """Convert any value to a stable hashable routing fingerprint.

    Never raises TypeError or RuntimeError.
    See module docstring for full coverage and fingerprint semantics.
    """

    # ── 1. Fast path
    # Catches RuntimeError too: non-scalar torch.Tensor raises RuntimeError,
    # not TypeError, from hash() — consistent with is_hashable().
    try:
        hash(obj)
        return cast(Hashable, obj)
    except (TypeError, RuntimeError):
        pass

    # ── 1b. O(1) exact-type dispatch
    # Populated at module load for dict, list, set, np.ndarray, pd.DataFrame,
    # torch.Tensor, and other common exact types.
    # Subclass misses fall through to the isinstance chain below — no regression.
    _handler = _DISPATCH.get(type(obj))
    if _handler is not None:
        return cast(Hashable, _handler(obj))

    # ── 2. Python built-in mutable containers

    if isinstance(obj, dict):
        return tuple(
            sorted((make_hashable(k), make_hashable(v)) for k, v in obj.items())
        )

    if isinstance(obj, (list, tuple)):
        # Handles plain list AND tuple-containing-unhashable
        return tuple(make_hashable(item) for item in obj)

    if isinstance(obj, (set, frozenset)):
        return frozenset(make_hashable(item) for item in obj)

    if isinstance(obj, bytearray):
        return bytes(obj)

    if isinstance(obj, memoryview):
        try:
            return bytes(obj)
        except TypeError:
            return 'memoryview', obj.format, obj.shape

    if isinstance(obj, slice):
        return 'slice', obj.start, obj.stop, obj.step

    if isinstance(obj, _array.array):
        return 'array.array', obj.typecode, len(obj)

    # ── 3. Standard library

    if isinstance(obj, collections.deque):
        return 'deque', tuple(make_hashable(i) for i in obj)

    if isinstance(obj, (collections.OrderedDict,
                        collections.defaultdict,
                        collections.Counter)):
        return tuple(sorted(
            (make_hashable(k), make_hashable(v)) for k, v in obj.items()
        ))

    if isinstance(obj, collections.ChainMap):
        return 'ChainMap', tuple(make_hashable(m) for m in obj.maps)

    if isinstance(obj, io.IOBase):
        return 'io', type(obj).__name__, id(obj)

    # xml.etree.ElementTree.Element defines __eq__ without __hash__ in Py 3.8+
    try:
        import xml.etree.ElementTree as _ET  # Type: Ignore: Only for installed versions
        if isinstance(obj, _ET.Element):
            return 'xml.Element', obj.tag, id(obj)
    except Exception:
        pass

    # ── 4. NumPy
    try:
        import numpy as np  # Type: Ignore: Only for installed versions
        if isinstance(obj, np.ndarray):
            return 'ndarray', obj.shape, str(obj.dtype)
        if isinstance(obj, np.generic):
            try:
                return cast(Hashable, obj.item())
            except (ValueError, TypeError):
                return 'np.generic', str(obj.dtype), _safe_repr(obj)
    except ImportError:
        pass

    # ── 5. Pandas
    try:
        import pandas as pd  # Type: Ignore: Only for installed versions
        if isinstance(obj, pd.DataFrame):
            return 'DataFrame', obj.shape, tuple(str(d) for d in obj.dtypes)
        if isinstance(obj, pd.Series):
            return 'Series', len(obj), str(obj.dtype), obj.name
        if isinstance(obj, pd.MultiIndex):
            return 'MultiIndex', obj.nlevels, len(obj)
        if isinstance(obj, pd.Index):
            return 'Index', len(obj), str(obj.dtype)
        if isinstance(obj, pd.Categorical):
            return 'Categorical', len(obj.categories), obj.ordered
        if isinstance(obj, pd.api.extensions.ExtensionArray):
            return 'ExtensionArray', type(obj).__name__, len(obj)
    except ImportError:
        pass

    # ── 6. PyTorch
    # Non-scalar tensors raise RuntimeError (not TypeError) on hash(),
    # so they sometimes pass the fast path and crash later.
    try:
        import torch  # Type: Ignore: Only for installed versions
        if isinstance(obj, torch.Tensor):
            return 'Tensor', tuple(obj.shape), str(obj.dtype), obj.device.type
    except (ImportError, Exception):
        pass

    # ── 7. TensorFlow / Keras
    try:
        import tensorflow as tf  # Type: Ignore: Only for installed versions
        if isinstance(obj, (tf.Tensor, tf.Variable)):
            shape = tuple(obj.shape.as_list()) if obj.shape.rank is not None else None
            return 'tf.Tensor', type(obj).__name__, shape, obj.dtype.name
    except (ImportError, Exception):
        pass

    # ── 8. JAX
    try:
        import jax  # Type: Ignore: Only for installed versions
        import jax.numpy as jnp  # Type: Ignore: Only for installed versions
        # jax.Array covers DeviceArray, ShapedArray, and all JAX array types
        if isinstance(obj, jax.Array):
            return 'jax.Array', obj.shape, str(obj.dtype)
    except (ImportError, Exception):
        pass

    # ── 9. Scientific array ecosystem

    # CuPy (GPU NumPy)
    try:
        import cupy as cp  # Type: Ignore: Only for installed versions
        if isinstance(obj, cp.ndarray):
            return 'cupy.ndarray', obj.shape, str(obj.dtype)
    except (ImportError, Exception):
        pass

    # Dask
    try:
        import dask.array as da  # Type: Ignore: Only for installed versions
        import dask.dataframe as dd  # Type: Ignore: Only for installed versions
        if isinstance(obj, da.Array):
            return 'dask.Array', obj.shape, str(obj.dtype)
        if isinstance(obj, dd.DataFrame):
            return 'dask.DataFrame', obj.columns.tolist()
        if isinstance(obj, dd.Series):
            return 'dask.Series', obj.name, str(obj.dtype)
    except (ImportError, Exception):
        pass

    # Xarray
    try:
        import xarray as xr  # Type: Ignore: Only for installed versions
        if isinstance(obj, xr.DataArray):
            return 'xr.DataArray', obj.shape, str(obj.dtype), obj.name
        if isinstance(obj, xr.Dataset):
            return 'xr.Dataset', tuple(sorted(obj.data_vars))
    except (ImportError, Exception):
        pass

    # Polars
    try:
        import polars as pl  # Type: Ignore: Only for installed versions
        if isinstance(obj, pl.DataFrame):
            return 'pl.DataFrame', obj.shape, tuple(str(d) for d in obj.dtypes)
        if isinstance(obj, pl.Series):
            return 'pl.Series', len(obj), str(obj.dtype), obj.name
        if isinstance(obj, pl.LazyFrame):
            return 'pl.LazyFrame', id(obj)
    except (ImportError, Exception):
        pass

    # PyArrow
    try:
        import pyarrow as pa  # Type: Ignore: Only for installed versions
        if isinstance(obj, pa.Table):
            return 'pa.Table', obj.shape, tuple(str(f.type) for f in obj.schema)
        if isinstance(obj, (pa.Array, pa.ChunkedArray)):
            return 'pa.Array', type(obj).__name__, len(obj), str(obj.type)
        if isinstance(obj, pa.RecordBatch):
            return 'pa.RecordBatch', obj.shape
    except (ImportError, Exception):
        pass

    # Awkward Array
    try:
        import awkward as ak  # Type: Ignore: Only for installed versions
        if isinstance(obj, ak.Array):
            return 'ak.Array', obj.ndim, str(obj.type)
    except (ImportError, Exception):
        pass

    # PyData Sparse
    try:
        import sparse  # Type: Ignore: Only for installed versions
        if isinstance(obj, sparse.SparseArray):
            return 'sparse.Array', obj.shape, str(obj.dtype)
    except (ImportError, Exception):
        pass

    # SciPy sparse
    try:
        import scipy.sparse as sp  # Type: Ignore: Only for installed versions
        if sp.issparse(obj):
            return 'scipy.sparse', type(obj).__name__, obj.shape, obj.dtype.str
    except (ImportError, Exception):
        pass

    # Zarr
    try:
        import zarr  # Type: Ignore: Only for installed versions
        if isinstance(obj, (zarr.Array, zarr.Group)):
            return 'zarr', type(obj).__name__, getattr(obj, 'shape', None)
    except (ImportError, Exception):
        pass

    # H5Py
    try:
        import h5py  # Type: Ignore: Only for installed versions
        if isinstance(obj, (h5py.Dataset, h5py.Group, h5py.File)):
            return 'h5py', type(obj).__name__, getattr(obj, 'name', id(obj))
    except (ImportError, Exception):
        pass

    # Numba typed lists/arrays
    try:
        from numba.typed import List as _NumbaList, Dict as _NumbaDict  # Type: Ignore: Only for installed versions
        if isinstance(obj, _NumbaList):
            return 'numba.List', len(obj)
        if isinstance(obj, _NumbaDict):
            return 'numba.Dict', len(obj)
    except (ImportError, Exception):
        pass

    # MXNet NDArray
    try:
        import mxnet as mx  # Type: Ignore: Only for installed versions
        if isinstance(obj, mx.nd.NDArray):
            return 'mx.NDArray', obj.shape, str(obj.dtype)
    except (ImportError, Exception):
        pass

    # PaddlePaddle Tensor
    try:
        import paddle  # Type: Ignore: Only for installed versions
        if isinstance(obj, paddle.Tensor):
            return 'paddle.Tensor', tuple(obj.shape), str(obj.dtype)
    except (ImportError, Exception):
        pass

    # ── 10. Graph libraries
    try:
        import networkx as nx  # Type: Ignore: Only for installed versions
        if isinstance(obj, nx.Graph):
            return ('nx.Graph', type(obj).__name__, obj.number_of_nodes(),
                    obj.number_of_edges())
    except (ImportError, Exception):
        pass

    # igraph
    try:
        import igraph  # Type: Ignore: Only for installed versions
        if isinstance(obj, igraph.Graph):
            return 'igraph.Graph', obj.vcount(), obj.ecount(), obj.is_directed()
    except (ImportError, Exception):
        pass

    # ── 11. Geospatial
    try:
        from shapely.geometry.base import BaseGeometry  # Type: Ignore: Only for installed versions
        if isinstance(obj, BaseGeometry):
            return 'shapely', type(obj).__name__, obj.geom_type, id(obj)
    except (ImportError, Exception):
        pass

    try:
        import geopandas as gpd  # Type: Ignore: Only for installed versions
        if isinstance(obj, gpd.GeoDataFrame):
            return 'GeoDataFrame', obj.shape
        if isinstance(obj, gpd.GeoSeries):
            return 'GeoSeries', len(obj)
    except (ImportError, Exception):
        pass

    # ── 12. Document / markup parsing
    try:
        from lxml import etree as _letree  # Type: Ignore: Only for installed versions
        if isinstance(obj, (_letree._Element, _letree._ElementTree)):
            return 'lxml.Element', getattr(obj, 'tag', None), id(obj)
    except (ImportError, Exception):
        pass

    try:
        import bs4  # Type: Ignore: Only for installed versions
        if isinstance(obj, bs4.element.Tag):
            return 'bs4.Tag', obj.name, id(obj)
        if isinstance(obj, bs4.BeautifulSoup):
            return 'BeautifulSoup', id(obj)
    except (ImportError, Exception):
        pass

    # ── 13. Database / ORM
    try:
        import sqlalchemy  # Type: Ignore: Only for installed versions
        # Query, Table, Column, Select, etc. all unhashable in SQLAlchemy 2.x
        if hasattr(sqlalchemy, 'orm') and isinstance(
                obj, sqlalchemy.orm.Query):
            return 'sa.Query', id(obj)
        if isinstance(obj, sqlalchemy.Table):
            return 'sa.Table', str(obj.name)
        if isinstance(obj, sqlalchemy.Column):
            return 'sa.Column', str(obj.name), id(obj)
        # Catch any other SQLAlchemy ClauseElement
        try:
            from sqlalchemy.sql.elements import ClauseElement  # Type: Ignore: Only for installed versions
            if isinstance(obj, ClauseElement):
                return 'sa.Clause', type(obj).__name__, id(obj)
        except Exception:
            pass
    except (ImportError, Exception):
        pass

    # Django QuerySet / Model instance
    try:
        from django.db.models import QuerySet as _DjQS, Model as _DjModel  # Type: Ignore: Only for installed versions
        if isinstance(obj, _DjQS):
            return 'django.QuerySet', obj.model.__name__, id(obj)
        if isinstance(obj, _DjModel):
            return 'django.Model', type(obj).__name__, obj.pk
    except (ImportError, Exception):
        pass

    # Peewee
    try:
        from peewee import Model as _PwModel, SelectBase as _PwSelect  # Type: Ignore: Only for installed versions
        if isinstance(obj, _PwSelect):
            return 'peewee.Query', id(obj)
        if isinstance(obj, _PwModel):
            return 'peewee.Model', type(obj).__name__, getattr(obj, '_pk', id(obj))
    except (ImportError, Exception):
        pass

    # ── 14. Distributed / Big Data
    try:
        from pyspark.sql import DataFrame as _SparkDF  # Type: Ignore: Only for installed versions
        from pyspark.rdd import RDD as _SparkRDD  # Type: Ignore: Only for installed versions
        if isinstance(obj, _SparkDF):
            return 'spark.DataFrame', tuple(obj.columns)
        if isinstance(obj, _SparkRDD):
            return 'spark.RDD', id(obj)
    except (ImportError, Exception):
        pass

    try:
        import ray  # Type: Ignore: Only for installed versions
        if hasattr(ray, 'ObjectRef') and isinstance(obj, ray.ObjectRef):
            return 'ray.ObjectRef', _safe_repr(obj)
    except (ImportError, Exception):
        pass

    # ── 15. GUI / Game
    try:
        import pygame  # Type: Ignore: Only for installed versions
        if isinstance(obj, pygame.Surface):
            return 'pygame.Surface', obj.get_size(), obj.get_bitsize()
    except (ImportError, Exception):
        pass

    # Qt — QObject subclasses define __eq__ without __hash__ in some bindings
    for _qt_mod in ('PyQt5.QtCore', 'PyQt6.QtCore',
                    'PySide2.QtCore', 'PySide6.QtCore'):
        try:
            import importlib
            _qt = importlib.import_module(_qt_mod)
            if isinstance(obj, _qt.QObject):
                return 'QObject', type(obj).__name__, id(obj)
        except (ImportError, Exception):
            pass

    # pyglet
    try:
        import pyglet  # Type: Ignore: Only for installed versions
        if isinstance(obj, pyglet.event.EventDispatcher):
            return 'pyglet', type(obj).__name__, id(obj)
    except (ImportError, Exception):
        pass

    # ── 16. Image / AV
    try:
        from PIL import Image as _PILImage  # Type: Ignore: Only for installed versions
        if isinstance(obj, _PILImage.Image):
            return 'PIL.Image', obj.mode, obj.size
    except (ImportError, Exception):
        pass

    # PyAV video/audio frames
    try:
        import av  # Type: Ignore: Only for installed versions
        if isinstance(obj, (av.VideoFrame, av.AudioFrame)):
            return 'av.Frame', type(obj).__name__, obj.pts
    except (ImportError, Exception):
        pass

    # ── 17. GPU / Graphics

    # ModernGL — check module name AND type name (covers subclasses / aliases)
    _MODERNGL_TYPES = frozenset({
        'Buffer', 'VertexArray', 'Texture', 'TextureArray', 'TextureCube',
        'Program', 'Framebuffer', 'Renderbuffer', 'Sampler', 'Query',
        'Scope', 'ComputeShader', 'ConditionalRender',
    })
    _type_name = type(obj).__name__
    _module = getattr(type(obj), '__module__', '') or ''

    if 'moderngl' in _module or _type_name in _MODERNGL_TYPES:
        return 'moderngl', _type_name, id(obj)

    # wgpu (WebGPU Python bindings)
    if 'wgpu' in _module:
        return 'wgpu', _type_name, id(obj)

    # PyOpenGL wrapped objects
    if 'OpenGL' in _module:
        return 'OpenGL', _type_name, id(obj)

    # ── 18. Structural: array-like
    _shape = getattr(obj, 'shape', _SENTINEL)
    _dtype = getattr(obj, 'dtype', _SENTINEL)
    if _shape is not _SENTINEL and _dtype is not _SENTINEL:
        return _type_name, _shape, str(_dtype)

    if _shape is not _SENTINEL:
        return _type_name, _shape

    # ── 19. Structural: mapping-like
    if (callable(getattr(obj, 'keys', None)) and
            callable(getattr(obj, 'values', None)) and
            callable(getattr(obj, 'items', None))):
        try:
            return tuple(
                sorted((make_hashable(k), make_hashable(v)) for k, v in obj.items())
            )
        except Exception:
            return _type_name, id(obj)

    # ── 20. Structural: graph-like
    if hasattr(obj, 'nodes') and hasattr(obj, 'edges'):
        try:
            return (_type_name,
                    len(obj.nodes) if hasattr(obj.nodes, '__len__') else '?',
                    len(obj.edges) if hasattr(obj.edges, '__len__') else '?')
        except Exception:
            return _type_name, id(obj)

    # ── 21. Structural: iterable with length
    if hasattr(obj, '__len__') and hasattr(obj, '__iter__'):
        try:
            return _type_name, len(obj)
        except Exception:
            pass

    # ── 22. Dataclass with __hash__ = None
    if getattr(type(obj), '__hash__', _SENTINEL) is None:
        return f'dataclass:{_type_name}', id(obj)

    # ── 23. Generic fallback
    try:
        return _type_name, _safe_repr(obj)
    except Exception:
        return _type_name, id(obj)


# fast_make_hashable — HashPolicy.FAST path (layers 1–3 only)
def fast_make_hashable(obj: Any) -> Hashable:
    """Reduced make_hashable for HashPolicy.FAST — layers 1–3 only.

    Covers Python builtins and standard library containers. Any type not
    handled here falls back to (type_name, id(obj)) — identity routing —
    rather than walking the full library-detection chain.

    Safe for operations where args are known primitives, or where
    best-effort routing stability is acceptable.
    """
    try:
        hash(obj)
        return cast(Hashable, obj)
    except (TypeError, RuntimeError):
        pass

    if isinstance(obj, dict):
        return tuple(sorted((fast_make_hashable(k), fast_make_hashable(v)) for k, v in obj.items()))
    if isinstance(obj, (list, tuple)):
        return tuple(fast_make_hashable(i) for i in obj)
    if isinstance(obj, (set, frozenset)):
        return frozenset(fast_make_hashable(i) for i in obj)
    if isinstance(obj, bytearray):
        return bytes(obj)
    if isinstance(obj, memoryview):
        try:
            return bytes(obj)
        except TypeError:
            return 'memoryview', obj.format, obj.shape
    if isinstance(obj, slice):
        return 'slice', obj.start, obj.stop, obj.step
    if isinstance(obj, _array.array):
        return 'array.array', obj.typecode, len(obj)
    if isinstance(obj, collections.deque):
        return 'deque', tuple(fast_make_hashable(i) for i in obj)
    if isinstance(obj, (collections.OrderedDict,
                        collections.defaultdict,
                        collections.Counter)):
        return tuple(sorted(
            (fast_make_hashable(k), fast_make_hashable(v)) for k, v in obj.items()
        ))
    if isinstance(obj, collections.ChainMap):
        return 'ChainMap', tuple(fast_make_hashable(m) for m in obj.maps)

    # Identity fallback — stable within session; skips all library detection
    return type(obj).__name__, id(obj)


# _DISPATCH population — runs once at module load
def _handle_memoryview(obj: Any) -> Hashable:
    """Named helper for memoryview dispatch (lambdas can't hold try/except)."""
    try:
        return bytes(obj)
    except TypeError:
        return 'memoryview', obj.format, obj.shape


def _build_dispatch() -> None:
    """Register exact-type handlers for the O(1) dispatch table.

    Called once at module load. Only registers types that are actually
    importable in the current environment — missing libraries are silently
    skipped. Subclass misses fall through to the isinstance chain in
    make_hashable — no coverage regression.
    """

    # ── Python builtins
    _DISPATCH[dict] = lambda o: tuple(sorted((make_hashable(k), make_hashable(v)) for k, v in o.items()))
    _DISPATCH[list] = lambda o: tuple(make_hashable(i) for i in o)
    _DISPATCH[set] = lambda o: frozenset(make_hashable(i) for i in o)
    _DISPATCH[bytearray] = bytes
    _DISPATCH[memoryview] = _handle_memoryview
    _DISPATCH[slice] = lambda o: ('slice', o.start, o.stop, o.step)
    _DISPATCH[_array.array] = lambda o: ('array.array', o.typecode, len(o))

    # ── Standard library
    _DISPATCH[collections.deque] = lambda o: ('deque', tuple(make_hashable(i) for i in o))
    _DISPATCH[collections.OrderedDict] = lambda o: tuple(
        sorted((make_hashable(k), make_hashable(v)) for k, v in o.items()))
    _DISPATCH[collections.defaultdict] = _DISPATCH[collections.OrderedDict]
    _DISPATCH[collections.Counter] = _DISPATCH[collections.OrderedDict]
    _DISPATCH[collections.ChainMap] = lambda o: ('ChainMap', tuple(make_hashable(m) for m in o.maps))

    # ── Optional libraries — registered only if present at import time
    try:
        import numpy as np
        _DISPATCH[np.ndarray] = lambda o: ('ndarray', o.shape, str(o.dtype))
    except ImportError:
        pass

    try:
        import pandas as pd
        _DISPATCH[pd.DataFrame] = lambda o: ('DataFrame', o.shape, tuple(str(d) for d in o.dtypes))
        _DISPATCH[pd.Series] = lambda o: ('Series', len(o), str(o.dtype), o.name)
        _DISPATCH[pd.MultiIndex] = lambda o: ('MultiIndex', o.nlevels, len(o))
        _DISPATCH[pd.Index] = lambda o: ('Index', len(o), str(o.dtype))
        _DISPATCH[pd.Categorical] = lambda o: ('Categorical', len(o.categories), o.ordered)
    except ImportError:
        pass

    try:
        import torch
        _DISPATCH[torch.Tensor] = lambda o: ('Tensor', tuple(o.shape), str(o.dtype), o.device.type)
    except (ImportError, Exception):
        pass

    try:
        import cupy as cp
        _DISPATCH[cp.ndarray] = lambda o: ('cupy.ndarray', o.shape, str(o.dtype))
    except (ImportError, Exception):
        pass

    try:
        from PIL import Image as _PILImage
        _DISPATCH[_PILImage.Image] = lambda o: ('PIL.Image', o.mode, o.size)
    except (ImportError, Exception):
        pass


_build_dispatch()


# Public API
def safe_args_key(args: tuple[Any, ...]) -> tuple[Any, ...]:
    """Convert a full token args tuple to a hashable routing key.

    Drop-in for any site currently doing hash(token.args) or using
    token.args directly as a dict/set key.

        # In sticky_token.py — replace:
        key = (op_name, args)
        # with:
        key = (op_name, safe_args_key(args))
    """
    return tuple(make_hashable(a) for a in args)


def is_hashable(obj: Any) -> bool:
    """Return True if obj can be passed to hash() without raising.

    Catches both TypeError (standard unhashable) and RuntimeError
    (torch.Tensor non-scalar).
    """
    try:
        hash(obj)
        return True
    except (TypeError, RuntimeError):
        return False
