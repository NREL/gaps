"""Legacy reV-like status manager"""

import logging
from abc import abstractmethod

import gaps.batch
import gaps.status
import gaps.pipeline
import gaps.hpc


logger = logging.getLogger(__name__)


def _as_gaps_hardware(hardware):
    """Convert hardware string to gaps-compliant hardware"""
    hardware = hardware.lower()
    if hardware == "pbs":
        hardware = "peregrine"
    return hardware


class PipelineError(Exception):
    """Error for pipeline execution failure"""


class HardwareStatusRetriever(gaps.status.HardwareStatusRetriever):
    """Query hardware for job status"""

    def __init__(self, hardware="slurm", subprocess_manager=None):
        """Initialize `HardwareStatusRetriever`

        Parameters
        ----------
        hardware : str
            Name of hardware that a pipeline is being run on: eagle,
            slurm, local. Defaults to "slurm".
        subprocess_manager : PBS | SLURM | None, optional
            Optional initialized subprocess manager to use to check job
            statuses. This can be input with cached queue data to avoid
            constantly querying the HPC. By default, `None`.
        """
        super().__init__(subprocess_manager)
        hardware = _as_gaps_hardware(hardware)
        self.hardware = gaps.status._validate_hardware(hardware)  # noqa

    def __getitem__(self, key):
        job_id, __ = key
        if not job_id:
            return None

        if self.subprocess_manager is not None:
            return self.subprocess_manager.check_status_using_job_id(job_id)

        return self.hardware.check_status_using_job_id(job_id)


class Status(gaps.status.Status):
    """Base class for data pipeline health and status information"""

    @classmethod
    def retrieve_job_status(
        cls,
        status_dir,
        module,
        job_name,
        hardware="slurm",
        subprocess_manager=None,
    ):
        """Update and retrieve job status.

        Parameters
        ----------
        status_dir : str
            Directory containing json status file.
        module : str
            Module that the job belongs to.
        job_name : str
            Unique job name identification.
        hardware : str
            Name of hardware that this pipeline is being run on: eagle,
            slurm, local. Defaults to "slurm". This specifies how job
            are queried for status.
        subprocess_manager : None | SLURM
            Optional initialized subprocess manager to use to check job
            statuses. This can be input with cached queue data to avoid
            constantly querying the HPC.

        Returns
        -------
        status : str | None
            Status string or None if job/module not found.
        """
        hardware = _as_gaps_hardware(hardware)
        hsr = HardwareStatusRetriever(hardware, subprocess_manager)
        status = (
            cls(status_dir)._retrieve_job_status(module, job_name, hsr)  # noqa
        )
        if status == gaps.status.StatusOption.NOT_SUBMITTED:
            status = None
        return status

    @classmethod
    def add_job(
        cls, status_dir, module, job_name, replace=False, job_attrs=None
    ):
        """Add a job to status json.

        Parameters
        ----------
        status_dir : str
            Directory containing json status file.
        module : str
            Module that the job belongs to.
        job_name : str
            Unique job name identification.
        replace : bool
            Flag to force replacement of pre-existing job status.
        job_attrs : dict
            Job attributes. Should include 'job_id' if running on HPC.
        """
        cls.mark_job_as_submitted(
            status_dir=status_dir,
            pipeline_step=module,
            job_name=job_name,
            replace=replace,
            job_attrs=job_attrs,
        )

    @staticmethod
    def make_job_file(status_dir, module, job_name, attrs):
        """Make a json file recording the status of a single job.

        Parameters
        ----------
        status_dir : str
            Directory to put json status file.
        module : str
            Module that the job belongs to.
        job_name : str
            Unique job name identification.
        attrs : str
            Dictionary of job attributes that represent the job status
            attributes.
        """
        gaps.status.Status.make_single_job_file(
            status_dir=status_dir,
            pipeline_step=module,
            job_name=job_name,
            attrs=attrs,
        )

    @staticmethod
    def _update_job_status_from_hardware(job_data, hardware_status_retriever):
        """Update job status from HPC hardware if needed"""

        # init defaults in case job/command not in status file yet
        job_status = job_data.get(gaps.status.StatusField.JOB_STATUS, None)
        job_id = job_data.get(gaps.status.StatusField.JOB_ID, None)
        job_hardware = job_data.get(gaps.status.StatusField.HARDWARE, None)

        # get job status from hardware
        current = hardware_status_retriever[job_id, job_hardware]

        # No current status and job was not successful: failed!
        if (
            current is None
            and job_status != gaps.status.StatusOption.SUCCESSFUL
        ):
            job_data[gaps.status.StatusField.JOB_STATUS] = (
                gaps.status.StatusOption.FAILED
            )

        # do not overwrite a successful or failed job status.
        elif (
            current != job_status
            and job_status not in gaps.status.StatusOption.members_as_str()
        ):
            job_data[gaps.status.StatusField.JOB_STATUS] = current


class Pipeline(gaps.pipeline.Pipeline):
    """Legacy reV-like pipeline execution framework"""

    @abstractmethod
    def __init__(self, pipeline, monitor=True, verbose=False):
        """

        When subclassing this object, you MUST set the following
        properties in the __init__ method:

        monitor : bool
            Flag to perform continuous monitoring of the pipeline.
        verbose : bool
            Flag to submit pipeline steps with "-v" flag for debug
            logging.
        _config : object
            reV-style config object. The config object MUST have the
            following attributes:

            name : str
                Job name (typically defaults to the output directory
                name).
            dirout : str
                Output file directory (typically the same directory
                that contains the config file).
            hardware : str
                Name of hardware that the pipeline is being run on
                (typically "eagle").
        _run_list : list
            List of dictionaries, each with a single key-value pair,
            where the key represents the command and the value
            represents the command config filepath to substitute into
            the :attr:`CMD_BASE` string.

        You must also call `self._init_status()` in the initializer.
        If you want logging outputs during the submit step, make sure
        to init the "gaps" logger.

        Parameters
        ----------
        pipeline : str | dict
            Pipeline config file path or dictionary.
        monitor : bool, optional
            Flag to perform continuous monitoring of the pipeline.
            By default, `True`.
        verbose : bool, optional
            Flag to submit pipeline steps with "-v" flag for debug
            logging. By default, `False`.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def CMD_BASE(self):  # noqa: N802
        """str: Formattable string of the base pipeline CLI command"""
        raise NotImplementedError

    @property
    @abstractmethod
    def COMMANDS(self):  # noqa: N802
        """list: List of pipeline command names (as str)"""
        raise NotImplementedError

    @property
    def _out_dir(self):
        """path-like: Output directory"""
        return self._config.dirout  # cspell:disable-line

    @property
    def _name(self):
        """str: Name of the pipeline job (directory of status file)"""
        return self._config.name

    @property
    def _hardware(self):
        """{'local', 'slurm', 'eagle'}: Hardware option"""
        return self._config.hardware

    def _submit(self, step):
        """Submit a step in the pipeline"""

        command, f_config = self._get_command_config(step)
        cmd = self._get_cmd(command, f_config)

        logger.info(
            "Pipeline submitting: %r for job %r", command, self._config.name
        )
        logger.debug("Pipeline submitting subprocess call:\n\t%r", cmd)

        try:
            stderr = gaps.hpc.submit(cmd)[1]
        except OSError:
            logger.exception(
                "Pipeline subprocess submission returned an error"
            )
            raise

        if stderr:
            logger.warning("Subprocess received stderr: \n%s", stderr)

    def _get_command_config(self, step):
        """Get the (command, config) key pair"""
        pipe_step = self._run_list[step]
        return pipe_step.command, pipe_step.config_path

    def _get_cmd(self, command, f_config):
        """Get the python cli call string"""

        if command not in self.COMMANDS:
            msg = (
                f"Could not recognize command {command!r}. "
                f"Available commands are: {self.COMMANDS!r}"
            )
            raise KeyError(msg) from None

        cmd = self.CMD_BASE.format(fp_config=f_config, command=command)
        if self.verbose:
            cmd += " -v"

        return cmd

    @classmethod
    def run(cls, pipeline, monitor=True, verbose=False):
        """Run the reV-style pipeline

        Parameters
        ----------
        pipeline : str | dict
            Pipeline config file path or dictionary.
        monitor : bool, optional
            Flag to perform continuous monitoring of the pipeline.
            By default, `True`.
        verbose : bool, optional
            Flag to submit pipeline steps with "-v" flag for debug
            logging. By default, `False`.
        """

        pipe = cls(pipeline, monitor=monitor, verbose=verbose)
        pipe._main()  # noqa: SLF001


class BatchJob(gaps.batch.BatchJob):
    """Legacy reV-like batch job framework

    To use this class, simply override the following two attributes:

        PIPELINE_CLASS : `Pipeline`
            Pipeline class with at least two class methods: `run` and
            `cancel_all` The `run` method must take `pipeline_config`,
            `monitor`, and `verbose` as arguments.
        PIPELINE_BACKGROUND_METHOD : callable
            Callable to run pipeline in the background with monitoring.
            If you set this as a static method (e.g.
            `PIPELINE_BACKGROUND_METHOD = staticmethod(my_callable)`),
            then this function should take exactly two arguments:
            `pipeline_config` and `verbose`. Otherwise, the function
            should take three arguments, where the first is a reference
            to this batch class, and the last two are the same as above.

    """

    @property
    @abstractmethod
    def PIPELINE_CLASS(self):  # noqa: N802
        """str: Formattable string of the base pipeline CLI command"""
        raise NotImplementedError

    @property
    @abstractmethod
    def PIPELINE_BACKGROUND_METHOD(self):  # noqa: N802
        """str: Formattable string of the base pipeline CLI command"""
        raise NotImplementedError

    def _run_pipelines(self, monitor_background=False, verbose=False):
        """Run the reV pipeline modules for each batch job

        Parameters
        ----------
        monitor_background : bool
            Flag to monitor all batch pipelines continuously in the
            background using the nohup command. Note that the
            stdout/stderr will not be captured, but you can set a
            pipeline "log_file" to capture logs.
        verbose : bool
            Flag to turn on debug logging for the pipelines.
        """

        for sub_directory in self.sub_dirs:
            pipeline_config = sub_directory / self._pipeline_fp.name
            if not pipeline_config.is_file():
                msg = (
                    f"Could not find pipeline config to run: "
                    f"'{pipeline_config.as_posix()}'"
                )
                raise PipelineError(msg)

            pipeline_config = pipeline_config.as_posix()
            if monitor_background:
                self.PIPELINE_BACKGROUND_METHOD(
                    pipeline_config, verbose=verbose
                )
            else:
                self.PIPELINE_CLASS.run(
                    pipeline_config, monitor=False, verbose=verbose
                )

    def _cancel_all(self):
        """Cancel all reV pipeline modules for all batch jobs"""
        for sub_directory in self.sub_dirs:
            pipeline_config = sub_directory / self._pipeline_fp.name
            if pipeline_config.is_file():
                self.PIPELINE_CLASS.cancel_all(pipeline_config.as_posix())

    @classmethod
    def cancel_all(cls, config, verbose=False):  # noqa: ARG003
        """Cancel all reV pipeline modules for all batch jobs

        Parameters
        ----------
        config : str
            File path to batch config json or csv (str).
        verbose : bool
            Flag to turn on debug logging.
        """

        cls(config)._cancel_all()  # noqa: SLF001

    @classmethod
    def delete_all(cls, config, verbose=False):  # noqa: ARG003
        """Delete all reV batch sub job folders based on the job summary
        csv in the batch config directory.

        Parameters
        ----------
        config : str
            File path to batch config json or csv (str).
        verbose : bool
            Flag to turn on debug logging.
        """
        cls(config).delete()

    @classmethod
    def run(
        cls,
        config,
        dry_run=False,
        delete=False,
        monitor_background=False,
        verbose=False,
    ):
        """Run the reV batch job from a config file

        Parameters
        ----------
        config : str
            File path to config json or csv (str).
        dry_run : bool
            Flag to make job directories without running.
        delete : bool
            Flag to delete all batch job sub directories based on the
            job summary csv in the batch config directory.
        monitor_background : bool
            Flag to monitor all batch pipelines continuously
            in the background using the nohup command. Note that the
            stdout/stderr will not be captured, but you can set a
            pipeline "log_file" to capture logs.
        verbose : bool
            Flag to turn on debug logging for the pipelines.
        """

        b = cls(config)
        if delete:
            b.delete()
        else:
            b._make_job_dirs()  # noqa: SLF001
            if not dry_run:
                b._run_pipelines(  # noqa: SLF001
                    monitor_background=monitor_background, verbose=verbose
                )
