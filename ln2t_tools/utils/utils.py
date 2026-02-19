import os
import shutil
import logging
import time
import fcntl
import signal
import atexit
import json
import socket
import getpass
import re
from pathlib import Path
from typing import List, Optional, Dict
from warnings import warn
import subprocess

from bids import BIDSLayout

from ln2t_tools.utils.defaults import (
    DEFAULT_RAWDATA,
    DEFAULT_DERIVATIVES,
    DEFAULT_CODE,
    MAX_PARALLEL_INSTANCES,
    LOCKFILE_DIR
)

logger = logging.getLogger(__name__)

class InstanceManager:
    """Manages parallel instances of ln2t_tools to prevent resource overload."""
    
    def __init__(self, max_instances: int = MAX_PARALLEL_INSTANCES):
        self.max_instances = max_instances
        self.lockfile_dir = LOCKFILE_DIR
        self.lockfile_dir.mkdir(exist_ok=True)
        self.lockfile_path = None
        self.lock_fd = None
        
    def acquire_instance_lock(self, dataset: str = None, tool: str = None, participants: List[str] = None) -> bool:
        """Acquire a lock for this instance.
        
        Args:
            dataset: Dataset name being processed
            tool: Tool being used (freesurfer, fmriprep, qsiprep)
            participants: List of participant labels being processed
        
        Returns:
            True if lock acquired successfully, False if max instances reached
        """
        # Clean up stale lock files first
        self._cleanup_stale_locks()
        
        # Count current active instances
        active_locks = list(self.lockfile_dir.glob("ln2t_tools_*.lock"))
        
        if len(active_locks) >= self.max_instances:
            logger.warning(f"Maximum number of instances ({self.max_instances}) already running")
            return False
        
        # Create lock file for this instance using PID
        pid = os.getpid()
        self.lockfile_path = self.lockfile_dir / f"ln2t_tools_{pid}.lock"
        
        try:
            self.lock_fd = open(self.lockfile_path, 'w')
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Create lock data structure
            lock_data = {
                "pid": pid,
                "dataset": dataset or "unknown",
                "tool": tool or "unknown",
                "participants": participants or [],
                "hostname": socket.gethostname(),
                "user": getpass.getuser(),
                "start_time": int(time.time()),
                "lock_file": self.lockfile_path.name
            }
            
            # Write JSON data to lock file
            json.dump(lock_data, self.lock_fd, indent=2)
            self.lock_fd.flush()
            
            # Register cleanup on exit
            atexit.register(self.release_instance_lock)
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
            
            logger.info(f"Acquired instance lock: {self.lockfile_path.name}")
            return True
            
        except (IOError, OSError) as e:
            logger.error(f"Failed to acquire lock: {e}")
            if self.lock_fd:
                self.lock_fd.close()
            if self.lockfile_path and self.lockfile_path.exists():
                self.lockfile_path.unlink()
            return False
    
    def release_instance_lock(self) -> None:
        """Release the instance lock."""
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
            except:
                pass
            self.lock_fd = None
        
        if self.lockfile_path and self.lockfile_path.exists():
            try:
                self.lockfile_path.unlink()
                logger.info(f"Released instance lock: {self.lockfile_path.name}")
            except:
                pass
            self.lockfile_path = None
    
    def _cleanup_stale_locks(self) -> None:
        """Remove lock files from dead processes."""
        for lockfile in self.lockfile_dir.glob("ln2t_tools_*.lock"):
            try:
                with open(lockfile, 'r') as f:
                    lock_data = json.load(f)
                    pid = lock_data.get("pid")
                    
                    if pid:
                        # Check if process is still running
                        try:
                            os.kill(pid, 0)  # Signal 0 checks if process exists
                        except OSError:
                            # Process is dead, remove stale lock
                            lockfile.unlink()
                            logger.info(f"Removed stale lock file: {lockfile.name}")
                    else:
                        # Invalid lock file format, remove it
                        lockfile.unlink()
                        logger.info(f"Removed invalid lock file: {lockfile.name}")
            except (json.JSONDecodeError, KeyError, IOError, OSError):
                # Invalid lock file, remove it
                try:
                    lockfile.unlink()
                    logger.info(f"Removed invalid lock file: {lockfile.name}")
                except:
                    pass
    
    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, cleaning up...")
        self.release_instance_lock()
        exit(1)
    
    def get_active_instances(self) -> int:
        """Get the number of currently active instances.
        
        Returns:
            Number of active instances
        """
        self._cleanup_stale_locks()
        return len(list(self.lockfile_dir.glob("ln2t_tools_*.lock")))
    
    def list_active_instances(self) -> None:
        """List information about currently active instances."""
        self._cleanup_stale_locks()
        lockfiles = list(self.lockfile_dir.glob("ln2t_tools_*.lock"))
        
        if not lockfiles:
            logger.info("No active ln2t_tools instances found")
            return
        
        logger.info(f"Found {len(lockfiles)} active instances:")
        for i, lockfile in enumerate(lockfiles, 1):
            try:
                with open(lockfile, 'r') as f:
                    lock_data = json.load(f)
                    
                    pid = lock_data.get("pid", "unknown")
                    dataset = lock_data.get("dataset", "unknown")
                    tool = lock_data.get("tool", "unknown")
                    participants = lock_data.get("participants", [])
                    hostname = lock_data.get("hostname", "unknown")
                    user = lock_data.get("user", "unknown")
                    start_time = lock_data.get("start_time", 0)
                    
                    # Calculate duration
                    duration = time.time() - start_time if start_time else 0
                    
                    # Format participant list
                    participant_str = ", ".join(participants) if participants else "none"
                    
                    logger.info(f"  {i}. PID: {pid}, User: {user}@{hostname}")
                    logger.info(f"      Dataset: {dataset}, Tool: {tool}")
                    logger.info(f"      Participants: {participant_str}")
                    logger.info(f"      Running for: {duration:.1f}s, Lock: {lockfile.name}")
                    
            except (json.JSONDecodeError, KeyError, Exception) as e:
                logger.info(f"  {i}. Lock: {lockfile.name} (error reading: {e})")

def check_apptainer_is_installed(apptainer_path: str = "/usr/bin/apptainer") -> None:
    """Verify Apptainer is installed and accessible.
    
    Args:
        apptainer_path: Path to apptainer executable
        
    Raises:
        FileNotFoundError: If apptainer is not found
    """
    if not shutil.which(apptainer_path):
        raise FileNotFoundError(
            f"Apptainer not found at {apptainer_path}. "
            "Please install Apptainer first."
        )

def ensure_image_exists(
    apptainer_dir: Path,
    tool: str,
    version: str
) -> Path:
    """Ensure Apptainer image exists and return its path.
    
    Args:
        apptainer_dir: Directory containing Apptainer images
        tool: Tool name ('freesurfer', 'fmriprep', or 'qsiprep')
        version: Tool version
        
    Returns:
        Path to Apptainer image
        
    Raises:
        FileNotFoundError: If image not found
    """
    if tool == "freesurfer":
        tool_owner = "freesurfer"
    elif tool == "fastsurfer":
        tool_owner = "deepmi"
    elif tool == "fmriprep":
        tool_owner = "nipreps"
    elif tool == "qsiprep":
        tool_owner = "pennlinc"
    elif tool == "qsirecon":
        tool_owner = "pennlinc"
    elif tool == "meld_graph":
        tool_owner = "meldproject"
    elif tool == "cvrmap":
        tool_owner = "ln2t"
    elif tool == "mri2print":
        tool_owner = "ln2t"
    elif tool == "bids_validator":
        tool_owner = "bids"
    else:
        raise ValueError(f"Unsupported tool: {tool}")
    
    # Determine the Docker tag to use. For reproducibility callers should
    # provide full Docker tags (for example, "cuda-v2.4.2"). Keep a
    # compatibility shim so plain numeric versions like "2.4.2" still work
    # for FastSurfer by prefixing with "cuda-v" when appropriate.
    if tool == "fastsurfer":
        docker_tag = version if version and version.startswith("cuda-v") else f"cuda-v{version}"
    else:
        docker_tag = version

    # Use the docker tag in the image filename for reproducibility
    image_path = apptainer_dir / f"{tool_owner}.{tool}.{docker_tag}.sif"
    if not image_path.exists():
        logger.warning(
            f"Apptainer image not found: {image_path}\n"
            f"Attempting to build the {tool} image with Docker tag {docker_tag}..."
        )
        build_cmd = f"apptainer build {image_path} docker://{tool_owner}/{tool}:{docker_tag}"
        # Use subprocess so we get robust return codes and signals
        try:
            completed = subprocess.run(build_cmd, shell=True)
            if completed.returncode != 0 or not image_path.exists():
                raise FileNotFoundError(
                    f"Failed to build Apptainer image: {image_path}\n"
                    f"Please check Apptainer installation and Docker image availability."
                )
        except KeyboardInterrupt:
            logger.error("Apptainer build interrupted by user (KeyboardInterrupt)")
            raise FileNotFoundError(
                f"Interrupted while building Apptainer image: {image_path}"
            )
    return image_path

def list_available_datasets() -> None:
    """List available BIDS datasets in rawdata directory."""
    available = [name[:-8] for name in os.listdir(DEFAULT_RAWDATA) 
                if name.endswith("-rawdata")]
    
    if not available:
        logger.info(f"No datasets found in {DEFAULT_RAWDATA}")
        return
    
    logger.info("Available datasets:")
    for dataset in available:
        logger.info(f"  - {dataset}")

def list_missing_subjects(
    rawdata_dir: Path,
    output_dir: Path
) -> None:
    """List subjects present in rawdata but missing from output.
    
    Args:
        rawdata_dir: Path to BIDS rawdata directory
        output_dir: Path to derivatives output directory
    """
    raw_layout = BIDSLayout(rawdata_dir)
    raw_subjects = set(raw_layout.get_subjects())
    
    processed_subjects = {
        d.name[4:] for d in output_dir.glob("sub-*")
        if d.is_dir()
    }
    
    missing = raw_subjects - processed_subjects
    if missing:
        logger.info("Missing subjects:")
        for subject in sorted(missing):
            logger.info(f"  - {subject}")
    else:
        logger.info("No missing subjects found")

def check_file_exists(file_path: str):
    if not os.path.isfile(file_path):
        print(f"File {file_path} does not exist.")
        return False
    else:
        print(f"File {file_path} found.")
        return True


def check_participants_exist(layout, participant_list):
    """Check if participants exist in the BIDS layout.
    
    Args:
        layout: BIDSLayout object
        participant_list: List of participant labels or None to use all participants
    
    Returns:
        list: List of valid participant labels
    """
    if not participant_list:
        # If no participants specified, use all available in the dataset
        return layout.get_subjects()
        
    true_participant_list = []
    for participant in participant_list:
        if participant in layout.get_subjects():
            true_participant_list.append(participant)
        else:
            warn(f"Participant {participant} not found in the dataset, removing from the list.")

    if not true_participant_list:
        raise ValueError("No valid participants found in the dataset.")

    return true_participant_list


def get_t1w_list(layout, participant_label):
    t1w_list = layout.get(subject=participant_label,
                          scope="raw",
                          suffix="T1w",
                          return_type="filename",
                          extension=".nii.gz")

    return t1w_list

def get_flair_list(layout, participant_label):
    flair_list = layout.get(subject=participant_label,
                          scope="raw",
                          suffix="FLAIR",
                          return_type="filename",
                          extension=".nii.gz")

    if len(flair_list):
        warn(f"Found FLAIR images. Ignoring them (for now).")

    return flair_list


def get_freesurfer_output(
    derivatives_dir: Path,
    participant_label: str,
    version: str,
    session: Optional[str] = None,
    run: Optional[str] = None
) -> Optional[Path]:
    """Check if FreeSurfer output exists for a subject.
    
    Args:
        derivatives_dir: Path to derivatives directory
        participant_label: Subject ID
        version: FreeSurfer version
        session: Optional session ID
        run: Optional run number
        
    Returns:
        Path to FreeSurfer output directory if it exists, None otherwise
    """
    subject_id = f"sub-{participant_label}"
    if session:
        subject_id += f"_ses-{session}"
    if run:
        subject_id += f"_run-{run}"
        
    fs_dir = derivatives_dir / f"freesurfer_{version}" / subject_id
    return fs_dir if fs_dir.exists() and (fs_dir / "surf/rh.white").exists() else None


def get_freesurfer_output_with_fallback(
    derivatives_dir: Path,
    participant_label: str,
    version: str,
    requested_session: Optional[str] = None,
    run: Optional[str] = None
) -> tuple[Optional[Path], Optional[str]]:
    """Check if FreeSurfer output exists for a subject, with session fallback.
    
    This function first tries to find FreeSurfer output for the exact session
    requested. If not found, it searches for any available FreeSurfer output
    for that participant (from a different session).
    
    This is useful for multi-session datasets where some sessions may only
    have functional data without anatomical scans.
    
    Args:
        derivatives_dir: Path to derivatives directory
        participant_label: Subject ID
        version: FreeSurfer version
        requested_session: The session we're looking for (can be None)
        run: Optional run number
        
    Returns:
        Tuple of (path_to_freesurfer_output, warning_message)
        - If exact match found: (path, None)
        - If fallback used: (path, warning_message)
        - If nothing found: (None, None)
    """
    # First, try exact match with requested session
    exact_match = get_freesurfer_output(
        derivatives_dir=derivatives_dir,
        participant_label=participant_label,
        version=version,
        session=requested_session,
        run=run
    )
    
    if exact_match:
        return exact_match, None
    
    # If no exact match and we have a specific session requested,
    # search for any available FreeSurfer output for this participant
    fs_base_dir = derivatives_dir / f"freesurfer_{version}"
    if not fs_base_dir.exists():
        return None, None
    
    # Look for any FreeSurfer output directories for this participant
    participant_prefix = f"sub-{participant_label}"
    fallback_dirs = []
    
    for item in fs_base_dir.iterdir():
        if item.is_dir() and item.name.startswith(participant_prefix):
            # Check if this is a valid FreeSurfer output
            if (item / "surf/rh.white").exists():
                fallback_dirs.append(item)
    
    if not fallback_dirs:
        return None, None
    
    # Sort by name for reproducibility (prefer earlier sessions)
    fallback_dirs.sort(key=lambda x: x.name)
    fallback_dir = fallback_dirs[0]
    
    # Extract session info from fallback directory name for warning message
    fallback_name = fallback_dir.name
    requested_desc = f"ses-{requested_session}" if requested_session else "no session"
    
    warning_msg = (
        f"FreeSurfer output not found for {participant_prefix} with {requested_desc}. "
        f"Using anatomical data from '{fallback_name}' instead. "
        f"This is common when a session only has functional data."
    )
    
    return fallback_dir, warning_msg


def build_apptainer_cmd(tool: str, **options) -> str:
    """Build Apptainer command for neuroimaging tools.
    
    This function builds the base Apptainer command with required bindings
    for each tool. Tool-specific options are passed via the 'tool_args'
    parameter and appended verbatim to the command.
    
    Parameters
    ----------
    tool : str
        Tool name ('freesurfer', 'fmriprep', 'qsiprep', etc.)
    **options : dict
        Required options vary by tool:
        - All tools: apptainer_img, rawdata, derivatives, participant_label
        - Most tools: fs_license (FreeSurfer license path)
        - tool_args: string of additional arguments passed to the container
        
    Returns
    -------
    str
        Complete Apptainer command string
    """
    tool_args = options.get('tool_args', '').strip()
    
    if tool == "freesurfer":
        if "fs_license" not in options:
            raise ValueError("FreeSurfer license file path is required")
        
        # Build subject ID with session and run if present
        subject_id = f"sub-{options['participant_label']}"
        if options.get('session'):
            subject_id += f"_ses-{options['session']}"
        if options.get('run'):
            subject_id += f"_run-{options['run']}"
        
        # Convert host paths to container paths
        rawdata_host = Path(options['rawdata'])
        t1w_host = Path(options['t1w'])
        
        try:
            t1w_relative = t1w_host.relative_to(rawdata_host)
            t1w_container = f"/rawdata/{t1w_relative}"
        except ValueError:
            t1w_container = str(t1w_host)
        
        cmd = (
            f"apptainer run --cleanenv --containall -B {options['fs_license']}:/usr/local/freesurfer/.license "
            f"-B {options['rawdata']}:/rawdata:ro -B {options['derivatives']}:/derivatives "
            f"{options['apptainer_img']} recon-all -all -subjid {subject_id} "
            f"-i {t1w_container} "
            f"-sd /derivatives/{options['output_label']}"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
        
    elif tool == "fmriprep":
        if "fs_license" not in options:
            raise ValueError("FreeSurfer license file path is required")
        
        fs_subjects_dir = options.get('fs_subjects_dir')
        
        # Build bindings
        bindings = [
            f"-B {options['fs_license']}:/opt/freesurfer/license.txt:ro",
            f"-B {options['rawdata']}:/data:ro",
            f"-B {options['derivatives']}:/out"
        ]
        
        # Environment variables for FreeSurfer
        env_flags = "--env FS_LICENSE=/opt/freesurfer/license.txt"
        
        # Add FreeSurfer subjects directory binding if available
        fs_args = ""
        if fs_subjects_dir:
            bindings.append(f"-B {fs_subjects_dir}:/fsdir:ro")
            env_flags += " --env SUBJECTS_DIR=/fsdir"
            fs_args = " --fs-subjects-dir /fsdir"
        
        cmd = (
            f"apptainer run "
            f"{' '.join(bindings)} "
            f"{env_flags} "
            f"--cleanenv "
            f"{options['apptainer_img']} "
            f"/data /out participant "
            f"--participant-label {options['participant_label']} "
            f"--skip-bids-validation{fs_args}"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
        
    elif tool == "qsiprep":
        if "fs_license" not in options:
            raise ValueError("FreeSurfer license file path is required")
        
        # QSIPrep requires work directory and clean environment
        workdir = os.environ.get('HOME', '/tmp')
        
        cmd = (
            f"apptainer run --cleanenv --containall "
            f"-B {options['fs_license']}:/opt/freesurfer/license.txt "
            f"-B {options['rawdata']}:/data:ro "
            f"-B {options['derivatives']}:/out "
            f"-B {workdir}:/tmp/work "
            f"{options['apptainer_img']} " 
            f"/data /out participant "
            f"--participant-label {options['participant_label']} "
            f"--fs-license-file /opt/freesurfer/license.txt "
            f"--skip-bids-validation "
            f"--work-dir /tmp/work/work"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
        
    elif tool == "qsirecon":
        if "fs_license" not in options:
            raise ValueError("FreeSurfer license file path is required")
        
        qsiprep_dir = options.get('qsiprep_dir', '')
        if not qsiprep_dir:
            raise ValueError("qsiprep_dir is required for QSIRecon")
        
        # Extract version from apptainer image filename
        img_filename = Path(options['apptainer_img']).name
        version_match = re.search(r'qsirecon\.([^.]+)\.sif', img_filename)
        version = version_match.group(1) if version_match else '1.1.1'
        
        # Extract dataset name from qsiprep_dir path
        # qsiprep_dir is typically /path/to/derivatives/qsiprep_VERSION
        qsiprep_path = Path(qsiprep_dir)
        dataset_name = None
        if qsiprep_path.parent.name.endswith('-derivatives'):
            dataset_name = qsiprep_path.parent.name.replace('-derivatives', '')
        
        # Create QSIRecon work directory on the host to avoid filling output directory
        workdir = os.environ.get('HOME', '/tmp')
        qsirecon_workdir = Path(workdir) / '.qsirecon_work'
        qsirecon_workdir.mkdir(parents=True, exist_ok=True)
        
        # Build bindings list
        bindings = [
            f"-B {options['fs_license']}:/opt/freesurfer/license.txt",
            f"-B {qsiprep_dir}:/data:ro",
            f"-B {options['derivatives']}:/out",
            f"-B {qsirecon_workdir}:/work"
        ]
        
        # Add code directory binding if dataset_name is available
        if dataset_name:
            code_dir = Path.home() / "code" / f"{dataset_name}-code"
            bindings.append(f"-B {code_dir}:/code:ro")
        
        cmd = (
            f"apptainer run --containall --writable-tmpfs "
            + " ".join(bindings) + " "
            f"{options['apptainer_img']} "
            f"/data /out participant "
            f"--participant-label {options['participant_label']} "
            f"--fs-license-file /opt/freesurfer/license.txt "
            f"-w /work"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
        
    elif tool == "fastsurfer":
        if "fs_license" not in options:
            raise ValueError("FreeSurfer license file path is required for FastSurfer")
        
        # Build subject ID with session and run if present
        subject_id = f"sub-{options['participant_label']}"
        if options.get('session'):
            subject_id += f"_ses-{options['session']}"
        if options.get('run'):
            subject_id += f"_run-{options['run']}"
        
        # Convert host paths to container paths
        rawdata_host = Path(options['rawdata'])
        t1w_host = Path(options['t1w'])
        
        try:
            t1w_relative = t1w_host.relative_to(rawdata_host)
            t1w_container = f"/data/{t1w_relative}"
        except ValueError:
            t1w_container = str(t1w_host)
        
        # FastSurfer benefits from GPU, enable by default
        cmd = (
            f"apptainer exec --nv "
            f"-B {options['fs_license']}:/fs_license/license.txt:ro "
            f"-B {options['rawdata']}:/data:ro "
            f"-B {options['derivatives']}:/output "
            f"{options['apptainer_img']} "
            f"/fastsurfer/run_fastsurfer.sh "
            f"--sid {subject_id} "
            f"--sd /output/{options['output_label']} "
            f"--t1 {t1w_container} "
            f"--fs_license /fs_license/license.txt"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
        
    elif tool == "meld_graph":
        # MELD Graph for lesion detection
        meld_data_dir = options.get('meld_data_dir', '')
        fs_license = options.get('fs_license', '/opt/freesurfer/.license')
        participant_label = options['participant_label']
        
        # Build the base command with GPU support
        cmd_parts = [
            "apptainer exec --nv",
            f"-B {meld_data_dir}:/data",
            f"-B {fs_license}:/license.txt:ro",
            "--env FS_LICENSE=/license.txt",
        ]
        
        # Add FreeSurfer outputs bind if using precomputed
        fs_subjects_dir = options.get('fs_subjects_dir', '')
        if fs_subjects_dir:
            cmd_parts.append(f"-B {fs_subjects_dir}:/data/output/fs_outputs")
        
        cmd_parts.append(f"{options['apptainer_img']}")
        
        # Build the Python command
        subject_arg = f"-id sub-{participant_label}"
        cmd_parts.append(f"/bin/bash -c 'cd /app && python scripts/new_patient_pipeline/new_pt_pipeline.py {subject_arg}")
        
        if tool_args:
            cmd_parts[-1] = cmd_parts[-1] + f" {tool_args}"
        
        cmd_parts[-1] = cmd_parts[-1] + "'"
        
        return " ".join(cmd_parts)
        
    elif tool == "cvrmap":
        # CVRmap for cerebrovascular reactivity mapping
        fmriprep_dir = options.get('fmriprep_dir', '')
        if not fmriprep_dir:
            raise ValueError("fmriprep_dir is required for CVRmap")
        
        cmd = (
            f"apptainer run "
            f"-B {options['rawdata']}:/data:ro "
            f"-B {options['derivatives']}:/derivatives "
            f"-B {fmriprep_dir}:/fmriprep:ro "
            f"{options['apptainer_img']} "
            f"/data /derivatives/{options['output_label']} participant "
            f"--participant-label {options['participant_label']} "
            f"--derivatives fmriprep=/fmriprep"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
    
    elif tool == "mri2print":
        # MRI2Print for 3D printable models - requires FreeSurfer outputs
        # mri2print uses non-BIDS-app syntax: mri2print -f /path/to/freesurfer -o /output subject_id
        if not options.get('fs_subjects_dir'):
            raise ValueError("FreeSurfer subjects directory is required for mri2print")
        
        bindings = [
            f"-B {options['fs_subjects_dir']}:/fsdir",
            f"-B {options['derivatives']}:/derivatives"
        ]
        
        cmd = (
            f"apptainer run "
            f"{' '.join(bindings)} "
            f"{options['apptainer_img']} "
            f"-f /fsdir "
            f"-o /derivatives/{options['output_label']} "
            f"{options['participant_label']}"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
    
    elif tool == "bids_validator":
        # BIDS Validator for dataset validation
        cmd = (
            f"apptainer run "
            f"-B {options['rawdata']}:/data:ro "
            f"{options['apptainer_img']} "
            f"/data"
        )
        if tool_args:
            cmd += f" {tool_args}"
        return cmd
    
    else:
        raise ValueError(f"Unsupported tool: {tool}")


def launch_apptainer(apptainer_cmd: str) -> int:
    """Launch Apptainer command and return exit code.
    
    Args:
        apptainer_cmd: The Apptainer command to execute
        
    Returns:
        Exit code (0 = success, non-zero = error)
    """
    logger.info("=" * 80)
    logger.info("Launching Apptainer container")
    logger.info("=" * 80)
    logger.info(f"Command:\n{apptainer_cmd}")
    logger.info("=" * 80)
    
    try:
        # Run with shell=True to preserve full command string semantics
        completed = subprocess.run(apptainer_cmd, shell=True)
        return completed.returncode
    except KeyboardInterrupt:
        logger.error("Apptainer run interrupted by user (KeyboardInterrupt)")
        # Use 130 to indicate termination by SIGINT
        return 130


def get_additional_contrasts(
    layout: BIDSLayout,
    participant_label: str,
    session: Optional[str] = None,
    run: Optional[str] = None
) -> Dict[str, Optional[str]]:
    """Get T2w and FLAIR images for a subject if they exist.
    
    Args:
        layout: BIDSLayout object
        participant_label: Subject ID
        session: Optional session ID
        run: Optional run number
        
    Returns:
        Dictionary with T2w and FLAIR file paths
    """
    filters = {
        'subject': participant_label,
        'scope': 'raw',
        'extension': '.nii.gz',
        'session': session,
        'run': run
    }
    
    # Remove None values from filters
    filters = {k: v for k, v in filters.items() if v is not None}
    
    t2w = layout.get(suffix='T2w', return_type='filename', **filters)
    flair = layout.get(suffix='FLAIR', return_type='filename', **filters)
    
    return {
        't2w': t2w[0] if t2w else None,
        'flair': flair[0] if flair else None
    }


def setup_meld_data_structure(
    dataset_derivatives: Path,
    dataset_code: Path,
    meld_version: str
) -> tuple[Path, Path, Path]:
    """Setup MELD Graph specific directory structure.
    
    MELD expects:
    - /data/input/{subject_id}/T1/ and /data/input/{subject_id}/FLAIR/
    - /data/output/predictions_reports/
    - /data/output/fs_outputs/ (for FreeSurfer outputs)
    - /data/output/preprocessed_surf_data/
    - Config files in separate code directory
    - Weights and data in derivatives directory
    
    Args:
        dataset_derivatives: Path to dataset derivatives directory (~/derivatives/{dataset}-derivatives)
        dataset_code: Path to dataset code directory (~/code/{dataset}-code)
        meld_version: MELD Graph version
        
    Returns:
        Tuple of (meld_data_dir, meld_config_dir, meld_output_dir)
    """
    # Create MELD-specific directories in derivatives folder for weights/output
    meld_base = dataset_derivatives / f"meld_graph_{meld_version}"
    meld_data_dir = meld_base / "data"
    
    # Config files stay in code directory
    meld_config_dir = dataset_code / f"meld_graph_{meld_version}" / "config"
    
    # Create structure
    meld_input_dir = meld_data_dir / "input"
    meld_output_dir = meld_data_dir / "output"
    
    meld_input_dir.mkdir(parents=True, exist_ok=True)
    meld_output_dir.mkdir(parents=True, exist_ok=True)
    meld_config_dir.mkdir(parents=True, exist_ok=True)
    
    # Create output subdirectories
    (meld_output_dir / "predictions_reports").mkdir(exist_ok=True)
    (meld_output_dir / "fs_outputs").mkdir(exist_ok=True)
    (meld_output_dir / "preprocessed_surf_data").mkdir(exist_ok=True)
    
    # Note: meld_params directory will be created by download_meld_weights()
    # when it downloads the templates (fsaverage_sym, etc.)
    # We don't create it here to avoid the "already exists" check in get_meld_params()
    
    return meld_data_dir, meld_config_dir, meld_output_dir


def create_meld_config_json(
    config_dir: Path,
    use_bids: bool = True
) -> Path:
    """Create MELD configuration JSON file.
    
    Args:
        config_dir: Directory to save config file
        use_bids: Whether to use BIDS format (default: True)
        
    Returns:
        Path to created config file
    """
    config_file = config_dir / "meld_bids_config.json"
    
    if config_file.exists():
        logger.info(f"MELD config already exists: {config_file}")
        return config_file
    
    if use_bids:
        config = {
            "T1": {
                "session": None,
                "datatype": "anat",
                "suffix": "T1w"
            },
            "FLAIR": {
                "session": None,
                "datatype": "anat",
                "suffix": "FLAIR"
            }
        }
    else:
        # MELD format - simple structure
        config = {
            "format": "MELD"
        }
    
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    logger.info(f"Created MELD config: {config_file}")
    return config_file


def create_meld_dataset_description(config_dir: Path, dataset_name: str) -> Path:
    """Create dataset_description.json for MELD.
    
    Args:
        config_dir: Directory to save file
        dataset_name: Name of the dataset
        
    Returns:
        Path to created file
    """
    desc_file = config_dir / "dataset_description.json"
    
    if desc_file.exists():
        return desc_file
    
    description = {
        "Name": dataset_name,
        "BIDSVersion": "1.0.2"
    }
    
    with open(desc_file, 'w') as f:
        json.dump(description, f, indent=2)
    
    logger.info(f"Created dataset description: {desc_file}")
    return desc_file


def prepare_meld_input_symlinks(
    meld_input_dir: Path,
    layout: BIDSLayout,
    participant_label: str
) -> bool:
    """Create symlinks in MELD input structure for a participant.
    
    MELD expects: input/{subject_id}/T1/T1.nii.gz and input/{subject_id}/FLAIR/FLAIR.nii.gz
    
    Args:
        meld_input_dir: MELD input directory
        layout: BIDS layout
        participant_label: Subject ID
        
    Returns:
        True if successful, False otherwise
    """
    subject_input_dir = meld_input_dir / f"sub-{participant_label}"
    
    # Get T1 file
    t1_files = layout.get(
        subject=participant_label,
        suffix='T1w',
        extension='.nii.gz',
        return_type='filename'
    )
    
    if not t1_files:
        logger.warning(f"No T1w found for {participant_label}")
        return False
    
    # Create T1 directory and symlink
    t1_dir = subject_input_dir / "T1"
    t1_dir.mkdir(parents=True, exist_ok=True)
    t1_link = t1_dir / "T1.nii.gz"
    
    if not t1_link.exists():
        t1_link.symlink_to(t1_files[0])
        logger.info(f"Created T1 symlink for {participant_label}")
    
    # Get FLAIR if available
    flair_files = layout.get(
        subject=participant_label,
        suffix='FLAIR',
        extension='.nii.gz',
        return_type='filename'
    )
    
    if flair_files:
        flair_dir = subject_input_dir / "FLAIR"
        flair_dir.mkdir(parents=True, exist_ok=True)
        flair_link = flair_dir / "FLAIR.nii.gz"
        
        if not flair_link.exists():
            flair_link.symlink_to(flair_files[0])
            logger.info(f"Created FLAIR symlink for {participant_label}")
    
    return True


def download_meld_weights(
    apptainer_img: str,
    meld_data_dir: Path,
    fs_license: str
) -> bool:
    """Download MELD Graph model weights and parameters using prepare_classifier.py.
    
    Args:
        apptainer_img: Path to MELD Graph apptainer image
        meld_data_dir: Path to MELD data directory
        fs_license: Path to FreeSurfer license
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("Downloading MELD Graph model weights and parameters...")
    
    # Use prepare_classifier.py with --skip-config to avoid interactive prompts
    # This downloads: test data, meld_params, and models
    # Note: --nv flag added for consistency, though GPU not needed for download
    cmd = (
        f"apptainer exec "
        f"--nv "
        f"-B {meld_data_dir}:/data "
        f"-B {fs_license}:/license.txt:ro "
        f"--env FS_LICENSE=/license.txt "
        f"{apptainer_img} "
        f"/bin/bash -c 'cd /app && python scripts/new_patient_pipeline/prepare_classifier.py --skip-config'"
    )
    
    logger.info(f"Running: {cmd}")
    exit_status = os.system(cmd)
    exit_code = exit_status >> 8
    
    if exit_code == 0:
        logger.info("Successfully downloaded MELD Graph weights and parameters")
        
        # Verify meld_params was actually populated
        meld_params_path = meld_data_dir / "meld_params"
        if meld_params_path.exists() and any(meld_params_path.iterdir()):
            logger.info(f"✓ Verified meld_params directory is populated")
        else:
            logger.warning(f"⚠ meld_params directory still empty after download!")
            return False
        
        return True
    else:
        logger.error(f"Failed to download MELD Graph weights and parameters (exit code: {exit_code})")
        return False


def get_dataset_initials(dataset: str) -> str:
    """Infer dataset initials from the dataset name.
    
    The dataset name follows the pattern: YYYY-Name_Parts-hexhash
    Initials are the first letter of each word in the name part.
    
    Args:
        dataset: Dataset name (e.g., "2024-Happy_Panda-236462bcdc71")
        
    Returns:
        Initials string (e.g., "HP")
        
    Examples:
        >>> get_dataset_initials("2024-Happy_Panda-236462bcdc71")
        'HP'
        >>> get_dataset_initials("2023-My_Cool_Dataset-abc123")
        'MCD'
    """
    parts = dataset.split('-')
    if len(parts) >= 2:
        name_part = parts[1]
        words = name_part.replace('_', ' ').split()
        return ''.join([w[0].upper() for w in words if w])
    return ''


def get_missing_participants(
    dataset: str,
    tool: str,
    rawdata_dir: Optional[Path] = None,
    derivatives_dir: Optional[Path] = None,
    tool_version: Optional[str] = None,
    tool_output_label: Optional[str] = None
) -> List[str]:
    """Find participants in rawdata but missing from tool derivatives.
    
    Compares participants in rawdata/{dataset}-rawdata to those in
    derivatives/{dataset}-derivatives/{tool_version}/{tool_output_label}.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    tool : str
        Tool name (freesurfer, fastsurfer, fmriprep, qsiprep, qsirecon, meld_graph, etc.)
    rawdata_dir : Optional[Path]
        Path to rawdata directory (default: ~/rawdata)
    derivatives_dir : Optional[Path]
        Path to derivatives directory (default: ~/derivatives)
    tool_version : Optional[str]
        Tool version (e.g., "7.3.2"). Used to build output directory name.
        If None, will try to auto-detect from existing directories.
    tool_output_label : Optional[str]
        Custom output label (e.g., "my_custom_output"). If None, uses "{tool}_{version}".
        
    Returns
    -------
    List[str]
        List of missing participant IDs (without 'sub-' prefix), sorted
    """
    from ln2t_tools.utils.defaults import DEFAULT_RAWDATA, DEFAULT_DERIVATIVES
    
    if rawdata_dir is None:
        rawdata_dir = DEFAULT_RAWDATA
    if derivatives_dir is None:
        derivatives_dir = DEFAULT_DERIVATIVES
    
    rawdata_dir = Path(rawdata_dir).resolve()
    derivatives_dir = Path(derivatives_dir).resolve()
    
    # Find rawdata participants
    rawdata_dataset_dir = rawdata_dir / f"{dataset}-rawdata"
    if not rawdata_dataset_dir.exists():
        logger.error(f"Rawdata directory not found: {rawdata_dataset_dir}")
        return []
    
    # Get all sub-* directories from rawdata
    rawdata_participants = set()
    for subdir in rawdata_dataset_dir.iterdir():
        if subdir.is_dir() and subdir.name.startswith('sub-'):
            participant_id = subdir.name[4:]  # Remove 'sub-' prefix
            rawdata_participants.add(participant_id)
    
    if not rawdata_participants:
        logger.warning(f"No participants found in {rawdata_dataset_dir}")
        return []
    
    # Find tool derivatives participants
    derivatives_dataset_dir = derivatives_dir / f"{dataset}-derivatives"
    
    # Build tool output directory name
    if tool_output_label:
        tool_output_dir = derivatives_dataset_dir / tool_output_label
    else:
        # Try to find matching tool output directory
        # Pattern: {tool}_{version} or just {tool}
        if not derivatives_dataset_dir.exists():
            logger.warning(f"Derivatives directory not found: {derivatives_dataset_dir}")
            return sorted(list(rawdata_participants))
        
        # Find the tool directory (could be multiple versions)
        matching_dirs = list(derivatives_dataset_dir.glob(f"{tool}*"))
        if not matching_dirs:
            logger.warning(f"No {tool} output found in {derivatives_dataset_dir}")
            return sorted(list(rawdata_participants))
        
        # Use the most recent one (by creation time) or specified version
        if tool_version:
            tool_output_dir = derivatives_dataset_dir / f"{tool}_{tool_version}"
        else:
            # Sort by modification time, get the most recent
            tool_output_dir = sorted(matching_dirs, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    
    # Get all sub-* directories from tool derivatives
    tool_participants = set()
    if tool_output_dir.exists():
        for subdir in tool_output_dir.iterdir():
            if subdir.is_dir() and subdir.name.startswith('sub-'):
                participant_id = subdir.name[4:]  # Remove 'sub-' prefix
                tool_participants.add(participant_id)
    
    # Find missing participants (in rawdata but not in tool derivatives)
    missing = rawdata_participants - tool_participants
    
    return sorted(list(missing))


def print_missing_participants_report(
    dataset: str,
    tool: str,
    missing_participants: List[str],
    command_prefix: str = "ln2t_tools"
) -> None:
    """Print a nicely formatted report of missing participants and suggested command.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    tool : str
        Tool name
    missing_participants : List[str]
        List of missing participant IDs
    command_prefix : str
        Command prefix (default: "ln2t_tools")
    """
    if not missing_participants:
        logger.info(f"\n{'='*70}")
        logger.info(f"✓ No missing participants for {tool}")
        logger.info(f"All rawdata participants have {tool} derivatives")
        logger.info(f"{'='*70}\n")
        return
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Missing Participants for {tool.upper()}")
    logger.info(f"{'='*70}")
    logger.info(f"Dataset: {dataset}")
    logger.info(f"Tool: {tool}")
    logger.info(f"Missing: {len(missing_participants)} participant(s)\n")
    
    for participant in missing_participants:
        logger.info(f"  - {participant}")
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Suggested Command (ready to copy-paste):")
    logger.info(f"{'='*70}\n")
    
    # Build suggested command
    cmd = f"{command_prefix} {tool} --dataset {dataset} --participant-label"
    for participant in missing_participants:
        cmd += f" {participant}"
    
    logger.info(cmd)
    logger.info(f"\n{'='*70}\n")

