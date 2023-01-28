#!../cdfkenv/bin/python3
import cdfk
import unittest

class TestCDFK(unittest.TestCase):

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
            max_workers=int(4),
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

if __name__ == "__main__":
    unittest.main(verbosity=2)
