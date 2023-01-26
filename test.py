#!../cdfkenv/bin/python3
import cdfk

def test_init_dfk():
    print(cdfk.info_dfk())
    cdfk.init_dfk(10)
    print(cdfk.info_dfk())

def test_create_task():
    pass

if __name__ == "__main__":
    test_init_dfk()
