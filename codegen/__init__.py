from .benchmark import Benchmark
from .utils import ROOT_DIR
try:
    from .generator import compile_ibis_to_noir, compile_preloaded_tables_evcxr
except AttributeError:
    print("Skipped import of ibis-renoir-compiler because of wrong version of ibis: it only supports ibis 8.0.0")
