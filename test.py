#!venv/bin/python3
import cdfk

from parsl import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

run_dir = "parsl_run_dir/"
config = Config(
        executors=[HighThroughputExecutor(
            cores_per_worker=1,
            label="HTEX",
            managed=True,
            worker_debug=True,
            max_workers=4,
        )],
        run_dir="runinfo",
)
dfk = cdfk.dflow.DataflowKernel(config=config)

@python_app(data_flow_kernel=dfk)
def add():
    return 2 + 2
num = add()
while not num.done():
    pass
print(num.result())
