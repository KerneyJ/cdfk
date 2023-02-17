#!venv/bin/python3
import cdfk
import unittest

"""
    def test_init_dfk(self):
        cdfk.init_dfk(10)

    def test_invoke_exec_submit(self):
        # Uses XQExecutor because this executor does not have a notion
        # of a provider. Was never sure why it was so challenging to run
        # the executor detached from the DFk but it seemed as though it
        # was the provider that made stuff not
        import time
        from uuid import uuid4
        from parsl.executors import XQExecutor

        executor = XQExecutor(
            max_workers=4,
        )
        executor.run_dir = f"parsl_run_dir/{str(uuid4())}"
        cdfk.init_dfk(10)

        def add():
            return 1 + 3;

        executor.start()
        exec_fu = cdfk.submit("xq", "add", time.time(), False, object(), executor, add)
        while not exec_fu.done():
            pass
        executor.shutdown()
"""

class TestFastDFK(unittest.TestCase):
    run_dir = "parsl_run_dir/"
    def test_internal_executor(self):
        def add():
            return 1 + 3

        executor = cdfk.intrnlexec.FDFKInternalExecutor(worker_count=4)
        executor.run_dir = TestFastDFK.run_dir
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
        executor.run_dir = TestFastDFK.run_dir
        executor.start()
        cdflow.add_executor_dfk(executor, executor.label)
        exec_fu = cdflow.submit("add", time.time(), False, object(), add)
        while not exec_fu.done():
            pass
        cdflow.dest_dfk()
        executor.shutdown()

    def test_py_dfk_exec(self):
        dfk = cdfk.dflow.FastDFK()
        dfk.info_executors()

    def test_py_dfk_wrapper(self):
        def add():
            return 1 + 3

        dfk = cdfk.dflow.FastDFK()
        exec_fu = dfk.submit(add, None, join=False)
        while not exec_fu.done():
            pass
        dfk.cleanup()

    def test_dflow(self):
        pass

if __name__ == "__main__":
    unittest.main(verbosity=2)
