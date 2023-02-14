import datetime

from uuid import uuid4
from parsl.config import Config

class FastDFK(object):
    import cdflow
    from .utils import make_rundir
    def __init__(self, config=Config(), table_size=10000):
        cdflow.init_dfk(table_size)
        self._config = config
        self.futlst = []
        self.run_id = str(uuid4())
        self.time_began = datetime.datetime.now()
        self.hub_address = None # TODO Is this necessary
        self.hub_interchange_port = None # TODO Is this necessary
        self._parsl_internal_executor = FDFKInternalExecutor() # TODO Implement this
        self.add_executors(self._parsl_internal_executor + config.executors)


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
                        logger.debug("Creating script_dir across multiple channels")
                        for channel in executor.provider.channels:
                            self._create_remote_dirs_over_channel(executor.provider, channel)
                    else:
                        self._create_remote_dirs_over_channel(executor.provider, executor.provider.channel)
            cdflow.add_executor_dfk(executor)

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
        exec_fu = cdflow.submit()

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
