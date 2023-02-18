#!venv/bin/python3

import cdfk
import time
import sys

from parsl.config import Config
from parsl.executors import XQExecutor
from parsl import python_app
from uuid import uuid4

xq_config = Config(
        executors=[XQExecutor(
            max_workers=8
        )],
        run_dir="runinfo"
)

dfk = cdfk.dflow.DataflowKernel(config=xq_config)
@python_app(data_flow_kernel=dfk)
def add():
    return 2 + 2

def bench_50k_noop_dfk():
    pass

def bench_10k_add_cdfk():
    task_count = 10000
    futtable = []
    start = time.perf_counter()
    for i in range(task_count):
        futtable.append(add())
    out = [f.result() for f in futtable]
    end = time.perf_counter()
    return end - start

if __name__ == "__main__":
    print(f"{10000 / bench_10k_add_cdfk()}")
