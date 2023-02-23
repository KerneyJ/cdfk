#!venv/bin/python3
import cdfk
import pytest

from parsl import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

class TestHTEX(object):
    run_dir = "parsl_run_dir/"
    config = Config(
        executors=[HighThroughputExecutor(
            cores_per_worker=1,
            label="HTEX",
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

    @pytest.mark.timeout(5)
    def test_cdfk_htex_basic(self):
        @python_app(data_flow_kernel=TestHTEX.dfk)
        def add():
            return 2 + 2
        num = add()
        assert num.result() == 4

    @pytest.mark.timeout(5)
    def test_cdfk_htex_input(self):
        @python_app(data_flow_kernel=TestHTEX.dfk)
        def add(a, b):
            return a + b

        num = add(2, 2)
        assert num.result() == 4

    @pytest.mark.timeout(5)
    def test_cdfk_htex_dep(self):
        @python_app(data_flow_kernel=TestHTEX.dfk)
        def k(x):
            return x

        @python_app(data_flow_kernel=TestHTEX.dfk)
        def mul(left, right):
            return left * right

        a = k(2)
        b = mul(a, 3)
        c = mul(a, 5)
        d = mul(b, c)
        e = k(d)
        
        assert e.result() == (2 * 3) * (2 * 5)
