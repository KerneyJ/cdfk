#!../cdfkenv/bin/python3
import cdfk

def test_init_dfk():
    print(cdfk.info_dfk())
    cdfk.init_dfk(10)
    print(cdfk.info_dfk())

def test_invoke_exec_submit():
    # Uses XQExecutor because this executor does not have a notion
    # of a provider. Was never sure why it was so challenging to run
    # the executor detached from the DFk but it seemed as though it
    # was the provider that made stuff not
    import time

    from parsl.executors import XQExecutor

    executor = XQExecutor(
        max_workers=int(4),
    )
    cdfk.init_dfk(10)

    def add():
        return 1 + 3;

    executor.start()
    exec_fu = cdfk.submit("xq", "add", time.time(), False, object(), executor, add)
    # executor.submit(add, None)
    print(cdfk.info_task(0))
    while not exec_fu.done():
        pass
    print(exec_fu)
    executor.shutdown()

if __name__ == "__main__":
    print("Testing init dfk")
    test_init_dfk()
    print("test concluded")

    print("Test invoke executor submit on real executor")
    test_invoke_exec_submit()
    print("test concluded")

