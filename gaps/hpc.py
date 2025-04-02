"""HPC Execution utilities"""

import re
import shlex
import logging
import getpass
import subprocess  # noqa: S404
from math import floor
from pathlib import Path
from warnings import warn
from collections import namedtuple
from abc import ABC, abstractmethod


from gaps.exceptions import gapsExecutionError, gapsHPCError, gapsValueError
from gaps.warn import gapsHPCWarning


logger = logging.getLogger(__name__)


Q_COLUMNS = namedtuple("Q_COLUMNS", ["NAME", "ID", "STATUS"])
COMMANDS = namedtuple("COMMANDS", ["SUBMIT", "CANCEL"])
DEFAULT_STDOUT_PATH = "./stdout"
"""Default directory for .stdout and .stderr files"""


class HpcJobManager(ABC):
    """Abstract HPC job manager framework"""

    # get username as class attribute.
    USER = getpass.getuser()

    # set a max job name length, will raise error if too long.
    MAX_NAME_LEN = 100

    SHELL_FILENAME_FMT = "{}.sh"

    def __init__(self, user=None, queue_dict=None):
        """
        Parameters
        ----------
        user : str | None, optional
            HPC username. `None` will get your username using
            :func:`getpass.getuser`. By default, `None`.
        queue_dict : dict | None, optional
            Parsed HPC queue dictionary from :meth:`parse_queue_str`.
            `None` will get the queue info from the hardware.
            By default, `None`.
        """

        self._user = user or self.USER
        if queue_dict is not None and not isinstance(queue_dict, dict):
            msg = (
                f"HPC queue_dict arg must be None or Dict but received: "
                f"{queue_dict}, {type(queue_dict)}"
            )
            raise gapsHPCError(msg)

        self._queue = queue_dict

    @classmethod
    def parse_queue_str(cls, queue_str):
        """Parse the hardware queue string into a nested dictionary.

        This function parses the queue output string into a dictionary
        keyed by integer job ids with values as dictionaries of job
        properties (queue printout columns).

        Parameters
        ----------
        queue_str : str
            HPC queue output string. Typically a space-delimited string
            with line breaks.

        Returns
        -------
        queue_dict : dict
            HPC queue parsed into dictionary format keyed by integer job
            ids with values as dictionaries of job properties (queue
            printout columns).
        """

        queue_dict = {}
        header, queue_rows = cls._split_queue(queue_str)
        for row in queue_rows:
            job = [k.strip(" ") for k in row.strip(" ").split(" ") if k]
            job_id = int(job[header.index(cls.COLUMN_HEADERS.ID)])
            queue_dict[job_id] = dict(zip(header, job))

        return queue_dict

    @property
    def queue(self):
        """dict: HPC queue keyed by job ids -> job properties"""
        if self._queue is None:
            queue_str = self.query_queue()
            self._queue = self.parse_queue_str(queue_str)

        return self._queue

    def reset_query_cache(self):
        """Reset query dict cache so that hardware is queried again"""
        self._queue = None

    def check_status_using_job_id(self, job_id):
        """Check the status of a job using the HPC queue and job ID

        Parameters
        ----------
        job_id : int
            Job integer ID number.

        Returns
        -------
        status : str | None
            Queue job status string or `None` if not found.
        """
        job_id = int(job_id)
        return self.queue.get(job_id, {}).get(self.COLUMN_HEADERS.STATUS)

    def check_status_using_job_name(self, job_name):
        """Check the status of a job using the HPC queue and job name

        Parameters
        ----------
        job_name : str
            Job name string.

        Returns
        -------
        status : str | None
            Queue job status string or `None` if not found.
        """
        for attrs in self.queue.values():
            if attrs[self.COLUMN_HEADERS.NAME] == job_name:
                return attrs[self.COLUMN_HEADERS.STATUS]

        return None

    def cancel(self, arg):
        """Cancel a job

        Parameters
        ----------
        arg : int | list | str
            Integer job id(s) to cancel. Can be a list of integer
            job ids, 'all' to cancel all jobs, or a feature (-p short)
            to cancel all jobs with a given feature
        """
        self._validate_command_not_none("CANCEL")

        if isinstance(arg, (list, tuple)):
            for job_id in arg:
                self.cancel(job_id)

        elif str(arg).lower() == "all":
            self.reset_query_cache()
            for job_id in self.queue:
                self.cancel(job_id)

        elif isinstance(arg, (int, str)):
            cmd = f"{self.COMMANDS.CANCEL} {arg}"
            cmd = shlex.split(cmd)
            subprocess.call(cmd)  # noqa: S603

        else:
            msg = f"Could not cancel: {arg} with type {type(arg)}"
            raise gapsExecutionError(msg)

    def submit(
        self,
        name,
        keep_sh=False,
        **kwargs,
    ):
        """Submit a job on the HPC

        Parameters
        ----------
        name : str
            HPC job name.
        keep_sh : bool, optional
            Option to keep the submission script on disk.
            By default, `False`.
        **kwargs
            Extra keyword-argument pairs to be passed to
            :meth:`make_script_str`.

        Returns
        -------
        out : str
            Standard output from submission. If submitted successfully,
            this is the Job ID.
        err : str
            Standard error. This is an empty string if the job was
            submitted successfully.
        """
        self._validate_command_not_none("SUBMIT")
        self._validate_name_length(name)
        if self._job_is_running(name):
            logger.info(
                "Not submitting job %r because it is already in "
                "queue has been recently submitted",
                name,
            )
            return None, "already_running"

        self._setup_submission(name, **kwargs)

        out, err = submit(
            f"{self.COMMANDS.SUBMIT} {self.SHELL_FILENAME_FMT.format(name)}"
        )
        out = self._teardown_submission(name, out, err, keep_sh=keep_sh)
        return out, err

    def _validate_command_not_none(self, command):
        """Validate that a command is not `None`"""
        if getattr(self.COMMANDS, command) is None:
            msg = (
                f"{command!r} command has not been defined for class"
                f"{self.__class__.__name__!r}"
            )
            raise NotImplementedError(msg)

    def _validate_name_length(self, name):
        """Validate that the name does not exceed max length"""
        if len(name) > self.MAX_NAME_LEN:
            msg = (
                f"Cannot submit job with name longer than "
                f"{self.MAX_NAME_LEN} chars: {name!r}"
            )
            raise gapsValueError(msg)

    def _setup_submission(self, name, **kwargs):
        """Setup submission file and directories"""
        stdout_dir = Path(kwargs.get("stdout_path", DEFAULT_STDOUT_PATH))
        stdout_dir.mkdir(parents=True, exist_ok=True)

        script = self.make_script_str(name, **kwargs)

        make_sh(self.SHELL_FILENAME_FMT.format(name), script)

    def _teardown_submission(self, name, out, err, keep_sh=False):
        """Remove submission file and mark job as submitted"""
        if not keep_sh:
            Path(self.SHELL_FILENAME_FMT.format(name)).unlink()

        if err:
            warn(
                f"Received an error or warning during submission: {err}",
                gapsHPCWarning,
            )
        else:
            out = self._mark_job_as_submitted(name, out)

        return out

    def _mark_job_as_submitted(self, name, out):
        """Mark job as submitted in the queue"""
        job_id = int(_job_id_or_out(out).split(" ")[-1])
        out = str(job_id)
        logger.debug("Job %r with id #%s submitted successfully", name, job_id)
        self._queue[job_id] = {
            self.COLUMN_HEADERS.ID: job_id,
            self.COLUMN_HEADERS.NAME: name,
            self.COLUMN_HEADERS.STATUS: self.Q_SUBMITTED_STATUS,
        }
        return out

    @abstractmethod
    def _job_is_running(self, name):
        """Check whether the job is submitted/running"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _split_queue(cls, queue_str):
        """Split queue string into a header and a list of lines"""
        raise NotImplementedError

    @abstractmethod
    def query_queue(self):
        """Run the HPC queue command and return the raw stdout string

        Returns
        -------
        stdout : list
            HPC queue output string that can be split into a list on
            line breaks.
        """
        raise NotImplementedError

    @abstractmethod
    def make_script_str(self, name, **kwargs):
        """Generate the submission script"""
        raise NotImplementedError

    @property
    @abstractmethod
    def COLUMN_HEADERS(self):  # noqa: N802
        """`namedtuple`: Column header names"""
        raise NotImplementedError

    @property
    @abstractmethod
    def COMMANDS(self):  # noqa: N802
        """`namedtuple`: Command names"""
        raise NotImplementedError

    @property
    @abstractmethod
    def Q_SUBMITTED_STATUS(self):  # noqa: N802
        """str: String representing the submitted status for manager"""
        raise NotImplementedError


class PBS(HpcJobManager):
    """Subclass for PBS subprocess jobs"""

    COLUMN_HEADERS = Q_COLUMNS(NAME="Name", ID="Job id", STATUS="S")

    # String representing the submitted status for this manager
    Q_SUBMITTED_STATUS = "Q"

    COMMANDS = COMMANDS(SUBMIT="qsub", CANCEL="qdel")  # cspell:disable-line

    def query_queue(self):
        """Run the PBS qstat command and return the raw stdout string.

        Returns
        -------
        stdout : str
            qstat output string. Can be split on line breaks to get
            list.
        """
        stdout, _ = submit(f"qstat -u {self._user}")
        return _skip_q_rows(stdout, skip_rows=(0, 1))

    @classmethod
    def _split_queue(cls, queue_str):
        """Split queue into a header and a list of lines"""
        header = ("Job id", "Name", "User", "Time Use", "S", "Queue")
        return header, queue_str.split("\n")

    def _job_is_running(self, name):
        """Check whether the job is submitted/running"""
        return self.check_status_using_job_name(name) in {"Q", "R"}

    def make_script_str(  # noqa: PLR6301, PLR0913, PLR0917
        self,
        name,
        cmd,
        allocation,
        walltime,
        qos=None,
        memory=None,
        queue=None,
        feature=None,
        stdout_path=DEFAULT_STDOUT_PATH,
        conda_env=None,
        sh_script=None,
    ):
        """Generate the PBS submission script.

        Parameters
        ----------
        name : str
            PBS job name.
        cmd :  str
            Command to be submitted in PBS shell script. Example:
                'python -m reV.generation.cli_gen'
        allocation : str
            HPC allocation account. Example: 'rev'.
        walltime : int | float
            Node walltime request in hours. Example: 4.
        qos : str, optional
            Quality of service string for job. By default, `None`.
        memory : int , optional
            Node memory request in GB. By default, `None`.
        queue : str
            HPC queue to submit job to. Examples include: 'debug',
            'short', 'batch', 'batch-h', 'long', etc. By default,
            `None`, which uses `test_queue`.
        feature : str, optional
            PBS feature request (-l {feature}). Example:
            'feature=24core'. *Do not use this input for QOS. Use the
            ``qos`` arg instead.* By default, `None`.
        stdout_path : str, optional
            Path to print .stdout and .stderr files.
            By default, :attr:`DEFAULT_STDOUT_PATH`.
        conda_env : str, optional
            Conda environment to activate. By default, `None`.
        sh_script : str, optional
            Script to run before executing command. By default, `None`.

        Returns
        -------
        str
            PBS script to submit.
        """
        features = [
            str(feature).replace(" ", "")
            if feature and "qos" not in feature
            else "",
            f"walltime={format_walltime(walltime)}" if walltime else "",
            f"mem={memory}gb" if memory else "",
        ]
        if qos:
            features += [f"qos={qos}"]
        features = ",".join(filter(None, features))
        script_args = [
            "#!/bin/bash",
            f"#PBS -N {name} # job name",
            f"#PBS -A {allocation} # allocation account",
            f"#PBS -q {queue} # queue" if queue else "",
            # cspell:disable-next-line
            f"#PBS -o {stdout_path}/{name}_$PBS_JOBID.o",
            # cspell:disable-next-line
            f"#PBS -e {stdout_path}/{name}_$PBS_JOBID.e",
            f"#PBS -l {features}" if features else "",
            format_env(conda_env),
            # cspell:disable-next-line
            "echo Running on: $HOSTNAME, Machine Type: $MACHTYPE",
            "echo Running python in directory `which python`",
            sh_script,
            cmd,
        ]
        return "\n".join(filter(None, script_args))


class SLURM(HpcJobManager):
    """Subclass for SLURM subprocess jobs"""

    # cspell:disable-next-line
    COLUMN_HEADERS = Q_COLUMNS(NAME="NAME", ID="JOBID", STATUS="ST")

    # String representing the submitted status for this manager
    Q_SUBMITTED_STATUS = "PD"

    # cspell:disable-next-line
    COMMANDS = COMMANDS(SUBMIT="sbatch", CANCEL="scancel")

    def query_queue(self):
        """Run the HPC queue command and return the raw stdout string.

        Returns
        -------
        stdout : str
            HPC queue output string. Can be split on line breaks to get
            a list.
        """
        cmd = (
            f'squeue -u {self._user} --format="%.15i %.30P '
            f'%.{self.MAX_NAME_LEN}j %.20u %.10t %.15M %.25R %q"'
        )
        stdout, _ = submit(cmd)
        return _skip_q_rows(stdout)

    @classmethod
    def _split_queue(cls, queue_str):
        """Split queue into a header and a list of lines"""
        queue_rows = queue_str.split("\n")
        header = [
            k.strip(" ") for k in queue_rows[0].strip(" ").split(" ") if k
        ]
        return header, queue_rows[1:]

    def _job_is_running(self, name):
        """Check whether the job is submitted/running"""
        return self.check_status_using_job_name(name) is not None

    def make_script_str(  # noqa: PLR6301
        self,
        name,
        cmd,
        allocation,
        walltime,
        qos="normal",
        memory=None,
        feature=None,
        stdout_path=DEFAULT_STDOUT_PATH,
        conda_env=None,
        sh_script=None,
    ):
        """Generate the SLURM submission script.

        Parameters
        ----------
        name : str
            SLURM job name.
        cmd : str
            Command to be submitted in SLURM shell script. Example:
                'python -m reV.generation.cli_gen'
        allocation : str
            HPC allocation account. Example: 'rev'.
        walltime : int | float
            Node walltime request in hours. Example: 4.
        qos : {"normal", "high"}
            Quality of service specification for job. Jobs with "high"
            priority will be charged at 2x the rate. By default,
            ``"normal"``.
        memory : int , optional
            Node memory request in GB. By default, `None`.
        feature : str, optional
            Additional flags for SLURM job. Format is
            "--partition=debug" or "--depend=[state:job_id]".
            *Do not use this input to specify QOS. Use the ``qos`` input
            instead.* By default, `None`.
        stdout_path : str, optional
            Path to print .stdout and .stderr files.
            By default, :attr:`DEFAULT_STDOUT_PATH`.
        conda_env : str, optional
            Conda environment to activate. By default, `None`.
        sh_script : str, optional
            Script to run before executing command. By default, `None`.

        Returns
        -------
        str
            SLURM script to submit.
        """
        walltime = format_walltime(walltime)
        memory = memory * 1000 if memory else None

        script_args = [
            "#!/bin/bash",
            f"#SBATCH --account={allocation}" if allocation else "",
            f"#SBATCH --time={walltime}" if walltime else "",
            f"#SBATCH --job-name={name}  # job name",
            "#SBATCH --nodes=1  # number of nodes",
            f"#SBATCH --output={stdout_path}/{name}_%j.o",
            f"#SBATCH --error={stdout_path}/{name}_%j.e",
            f"#SBATCH --qos={qos}",
            f"#SBATCH {feature}  # extra feature" if feature else "",
            f"#SBATCH --mem={memory}  # node RAM in MB" if memory else "",
            format_env(conda_env),
            # cspell:disable-next-line
            "echo Running on: $HOSTNAME, Machine Type: $MACHTYPE",
            "echo Running python in directory `which python`",
            sh_script,
            cmd,
        ]
        return "\n".join(filter(None, script_args))


def make_sh(fname, script):
    """Make a shell script (.sh file) to execute a subprocess.

    Parameters
    ----------
    fname : str
        Name of the .sh file to create.
    script : str
        Contents to be written into the .sh file.
    """
    logger.debug(
        "The shell script %(n)r contains the following:\n"
        "~~~~~~~~~~ %(n)s ~~~~~~~~~~\n"
        "%(s)s\n"
        "~~~~~~~~~~ %(n)s ~~~~~~~~~~",
        {"n": fname, "s": script},
    )
    with Path(fname).open("w+", encoding="utf-8") as f:
        f.write(script)


def _subprocess_popen(cmd):
    """Open a subprocess popen constructor and submit a command.

    Parameters
    ----------
    cmd : str
        Command to be submitted using python subprocess.

    Returns
    -------
    stdout : str
        Subprocess standard output. This is decoded from the subprocess
        stdout with rstrip.
    stderr : str
        Subprocess standard error. This is decoded from the subprocess
        stderr with rstrip. After decoding/rstrip, this will be empty if
        the subprocess doesn't return an error.
    """

    cmd = shlex.split(cmd)

    # use subprocess to submit command and get piped o/e
    process = subprocess.Popen(  # noqa: S603
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    stderr = stderr.decode("ascii").rstrip()
    stdout = stdout.decode("ascii").rstrip()

    if process.returncode != 0:
        msg = (
            f"Subprocess submission failed with return code "
            f"{process.returncode} and stderr:\n{stderr}"
        )
        if "Invalid qos specification" in stderr:
            msg = (
                f"{msg}\n(This error typically occurs if your allocation "
                "runs out of AUs)"
            )
        raise OSError(msg)

    return stdout, stderr


def _subprocess_run(cmd, background_stdout=False):
    """Open a subprocess and submit a command

    Parameters
    ----------
    cmd : str
        Command to be submitted using python subprocess.
    background_stdout : bool
        Flag to capture the stdout/stderr from the background process
        in a nohup.out file.
    """

    nohup_cmd_fmt = ["nohup {}"]
    if not background_stdout:
        nohup_cmd_fmt += ["</dev/null >/dev/null 2>&1"]
    nohup_cmd_fmt += ["&"]

    cmd = " ".join(nohup_cmd_fmt).format(cmd)
    subprocess.run(cmd, shell=True, check=True)  # noqa: S602


def submit(cmd, background=False, background_stdout=False):
    """Open a subprocess and submit a command

    Parameters
    ----------
    cmd : str
        Command to be submitted using python subprocess.
    background : bool
        Flag to submit subprocess in the background. stdout stderr will
        be empty strings if this is True.
    background_stdout : bool
        Flag to capture the stdout/stderr from the background process
        in a nohup.out file.

    Returns
    -------
    stdout : str
        Subprocess standard output. This is decoded from the subprocess
        stdout with rstrip.
    stderr : str
        Subprocess standard error. This is decoded from the subprocess
        stderr with rstrip. After decoding/rstrip, this will be empty if
        the subprocess doesn't return an error.
    """

    if background:
        _subprocess_run(cmd, background_stdout=background_stdout)
        return "", ""

    stdout, stderr = _subprocess_popen(cmd)
    return stdout, stderr


def format_walltime(hours=None):
    """Get the SLURM walltime string in format "HH:MM:SS"

    Parameters
    ----------
    hours : float | int, optional
        Requested number of job hours. By default, `None`, which returns
        an empty string.

    Returns
    -------
    walltime : str
        SLURM walltime request in format "#SBATCH --time=HH:MM:SS"
    """
    if hours is not None:
        m_str = f"{round(60 * (hours % 1)):02d}"
        h_str = f"{floor(hours):02d}"
        return f"{h_str}:{m_str}:00"

    return hours


def format_env(conda_env=None):
    """Get special sbatch request strings for SLURM conda environments

    Parameters
    ----------
    conda_env : str, optional
        Conda environment to activate. By default, `None`, which returns
        an empty string.

    Returns
    -------
    env_str : str
        SBATCH shell script source activate environment request string.
    """

    if conda_env is not None:
        return (
            f"echo source activate {conda_env}\n"
            f"source activate {conda_env}\n"
            f"echo conda env activate complete!\n"
        )

    return ""


def _skip_q_rows(queue_str, skip_rows=None):
    """Remove rows from the queue_str that are to be skipped

    Parameters
    ----------
    queue_str : str
        HPC queue output string. Can be split on line breaks to get a
        list.
    skip_rows : list | None, optional
        Optional row index values to skip.

    Returns
    -------
    queue_str : str
        HPC queue output string. Can be split on line breaks to get a
        list.
    """
    if skip_rows is None:
        return queue_str

    queue_str = [
        row
        for i, row in enumerate(queue_str.split("\n"))
        if i not in skip_rows
    ]

    return "\n".join(queue_str)


def _job_id_or_out(input_str):
    """Extract job id from input

    Specifically, this function checks the input for a job id and
    returns just the job id if present. Otherwise, the full input is
    returned.

    Parameters
    ----------
    input_str : str
        Stdout from :func:`submit`

    Returns
    -------
    str
        If job id is present in `input_str` then this is just the job
        id. Otherwise, the full `input_str` is returned.
    """
    return re.sub(r"[^0-9]", "", str(input_str)) or input_str
