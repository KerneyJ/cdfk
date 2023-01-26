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

if __name__ == "__main__":
    print("Testing init dfk")
    test_init_dfk()
    print("test concluded")

    print("Testing create task and info task")
    test_create_task()
    print("test concluded")
