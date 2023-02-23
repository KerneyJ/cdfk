import os
import atexit
import logging
import cdflow
import time
import datetime

from uuid import uuid4
from parsl import set_file_logger
from parsl.config import Config
from .utils import make_rundir
from .intrnlexec import FDFKInternalExecutor


class DataflowKernel(object):
    def __init__(self, config=Config(), table_size=10000):
        self._config = config
        self.run_dir = make_rundir(config.run_dir)
        self.run_id = str(uuid4())

        # set_file_logger(f"{self.run_dir}/parsl.log", level=logging.DEBUG)
        logger = start_file_logger(filename=f"{self.run_dir}/parsl.log")
        cdflow.init_dfk(table_size)
        self.cleanup_called = False
        self.futlst = []
        self.run_dir = make_rundir(config.run_dir)
        self.run_id = str(uuid4())
        self.time_began = datetime.datetime.now()
        self.hub_address = None # TODO Is this necessary
        self.hub_interchange_port = None # TODO Is this necessary
        self._parsl_internal_executor = FDFKInternalExecutor(worker_count=4)
        self.add_executors([self._parsl_internal_executor] + config.executors)
        atexit.register(self.atexit_cleanup)
        logger.info("Successfully initialized DFK")

    def add_executors(self, executors):
        for executor in executors:
            executor.run_dir = self.run_dir
            executor.run_id = self.run_id
            executor.hub_address = self.hub_address # TODO Is this necessary
            executor.hub_port = self.hub_interchange_port # TODO Is this Necessary
            if hasattr(executor, 'provider'):
                if hasattr(executor.provider, 'script_dir'):
                    executor.provider.script_dir = os.path.join(self.run_dir, 'submit_scripts')
                    os.makedirs(executor.provider.script_dir, exist_ok=True)

                    if hasattr(executor.provider, 'channels'):
                        # logger.debug("Creating script_dir across multiple channels")
                        for channel in executor.provider.channels:
                            self._create_remote_dirs_over_channel(executor.provider, channel)
                    else:
                        self._create_remote_dirs_over_channel(executor.provider, executor.provider.channel)
            cdflow.add_executor_dfk(executor, executor.label)
            block_ids = executor.start()

    def submit(self, func, app_args, executors='all', cache=False, ignore_for_cache=None, app_kwargs={}, join=False):
        """
        Wrapper for cdflow submit
        cdflow.submit requires argumnets
          * exec_label: string
          * func_name: string
          * time_invoked: timestamp
          * join: True or False
          * future: Future Object
          * executor: Executor Object
          * func: Function object
          * fargs: Argument list object
          * fkwarg: Keyword argument list object
        """
        return  cdflow.submit(func.__name__, time.time(), join, object(), func)

    def info_executors(self):
        cdflow.info_exec_dfk()

    def cleanup(self):
        cdflow.shutdown_executor_dfk()
        cdflow.dest_dfk()
        self.cleanup_called = True

    def _create_remote_dirs_over_channel(self, provider, channel):
        """ Create script directories across a channel
        Parameters
        ----------
        provider: Provider obj
           Provider for which scripts dirs are being created
        channel: Channel obj
           Channel over which the remote dirs are to be created
        """
        run_dir = self.run_dir
        if channel.script_dir is None:
            channel.script_dir = os.path.join(run_dir, 'submit_scripts')

            # Only create dirs if we aren't on a shared-fs
            if not channel.isdir(run_dir):
                parent, child = pathlib.Path(run_dir).parts[-2:]
                remote_run_dir = os.path.join(parent, child)
                channel.script_dir = os.path.join(remote_run_dir, 'remote_submit_scripts')
                provider.script_dir = os.path.join(run_dir, 'local_submit_scripts')

        channel.makedirs(channel.script_dir, exist_ok=True)

    def atexit_cleanup(self) -> None:
        if not self.cleanup_called:
            # logger.info("DFK cleanup because python process is exiting")
            self.cleanup()
        else:
            pass
            # logger.info("python process is exiting, but DFK has already been cleaned up")

def start_file_logger(filename, name='parsl', level=logging.DEBUG, format_string=None):
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d " \
                        "%(process)d %(threadName)s "\
                        "[%(levelname)s]  %(message)s"
    l = logging.getLogger(name)
    l.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    l.addHandler(handler)
    return l
