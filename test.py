#!../cdfkenv/bin/python3
import cdfk

def test_init_dfk():
    print(cdfk.info_dfk())
    cdfk.init_dfk(10)
    print(cdfk.info_dfk())

def test_create_task():
    import time
    cdfk.init_dfk(10)
    cdfk.submit("htex", "add", time.time(), False, object(), object(), object(), object(), object())# , object(), object(), object(), object())
    print(cdfk.info_task(0))

# Building a dummy executor class with
# a method named submit that takes no
# arguments and prints task submitted
def test_invoke_dumexec_submit():
    import time
    class DummyExecutor(object):
        def submit(self):
            print("task submitted")

    executor = DummyExecutor()
    cdfk.init_dfk(10)
    cdfk.submit("htex", "add", time.time(), False, object(), executor, object(), object(), object())
    print(cdfk.info_task(0))

def test_invoke_exec_submit(object):
    from parsl.executors import HighThroughputExecutor
    print(HighThroughputExecutor.submit)

if __name__ == "__main__":
    """
    print("Testing init dfk")
    test_init_dfk()
    print("test concluded")

    print("Testing create task and info task")
    test_create_task()
    print("test concluded")
    """

    print("Testing invoke executor submit")
    test_invoke_dumexec_submit()
    print("test concluded")
