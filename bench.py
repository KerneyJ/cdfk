#!venv/bin/python3

import cdfk
import parsl
import time
import sys

from parsl.executors import XQExecutor
from uuid import uuid4

def bench_50k_noop_dfk():
    pass

def bench_50k_noop_cdfk(worker_count):
    def noop():
        import time
        time.sleep(0)
        return None
    noop_count = 50000
    executor = XQExecutor(
        max_workers=worker_count,
    )
    executor.run_dir = f"parsl_run_dir/{str(uuid4())}"
    cdfk.init_dfk(noop_count)
    executor.start()
    futtable = []
    start = time.perf_counter()
    for i in range(noop_count):
        fut = cdfk.submit("xq", "add", time.time(), False, object(), executor, noop)
        futtable.append(fut)
    out = [f.result() for f in futtable]
    end = time.perf_counter()
    executor.shutdown()
    return end - start

if __name__ == "__main__":
    for n in [8]:
        print(f"{n},{50000 / bench_50k_noop_cdfk(n)}")
