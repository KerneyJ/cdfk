#!venv/bin/python3
import cdfk
import unittest

from parsl import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

class TestCDFK(unittest.TestCase):
    run_dir = "parsl_run_dir/"
    def test_internal_executor(self):
        def add():
            return 1 + 3

        executor = cdfk.intrnlexec.FDFKInternalExecutor(worker_count=4)
        executor.run_dir = TestCDFK.run_dir
        executor.start()
        exec_fu = executor.submit(add)
        while not exec_fu.done():
            pass
        executor.shutdown()

    def test_cdfk(self):
        import cdflow
        import time
        def add():
            return 1 + 3
        cdflow.init_dfk(10)
        executor = cdfk.intrnlexec.FDFKInternalExecutor(worker_count=4)
        executor.run_dir = TestCDFK.run_dir
        executor.start()
        cdflow.add_executor_dfk(executor, executor.label)
        exec_fu = cdflow.submit("add", time.time(), False, object(), add)
        while not exec_fu.done():
            pass
        cdflow.dest_dfk()
        executor.shutdown()

    def test_py_dfk_wrapper(self):
        def add():
            return 1 + 3

        dfk = cdfk.dflow.DataflowKernel()
        exec_fu = dfk.submit(add, None, join=False)
        while not exec_fu.done():
            pass
        dfk.cleanup()

#    def test_python_app(self):
#        dfk = cdfk.dflow.DataflowKernel()
#        @python_app(data_flow_kernel=dfk)
#        def add():
#            return 1 + 3
#
#        num = add()
#        while not num.done():
#            pass
#        dfk.cleanup()

    def test_cdfk_with_htex(self):
        config = Config(
            executors=[HighThroughputExecutor(
                cores_per_worker=1,
                label=f"HTEX",
                managed=True,
                worker_debug=False,
                max_workers=4,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                    min_blocks=1,
                    nodes_per_block=1,
                ),
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
        dfk.cleanup()

if __name__ == "__main__":
    unittest.main(verbosity=2)
