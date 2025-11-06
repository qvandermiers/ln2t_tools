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
from pathlib import Path
from typing import List, Optional, Dict
from warnings import warn

from bids import BIDSLayout

from ln2t_tools.utils.defaults import (
    DEFAULT_RAWDATA,
    DEFAULT_DERIVATIVES,
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
    elif tool == "fmriprep":
        tool_owner = "nipreps"
    elif tool == "qsiprep":
        tool_owner = "pennlinc"
    elif tool == "qsirecon":
        tool_owner = "pennlinc"
    elif tool == "meld_graph":
        tool_owner = "meldproject"
    else:
        raise ValueError(f"Unsupported tool: {tool}")
    image_path = apptainer_dir / f"{tool_owner}.{tool}.{version}.sif"
    if not image_path.exists():
        logger.warning(
            f"Apptainer image not found: {image_path}\n"
            f"Attempting to build the {tool} version {version} image..."
        )
        build_cmd = (
            f"apptainer build {image_path} docker://{tool_owner}/{tool}:{version}"
        )
        result = os.system(build_cmd)
        if result != 0 or not image_path.exists():
            raise FileNotFoundError(
                f"Failed to build Apptainer image: {image_path}\n"
                f"Please check Apptainer installation and Docker image availability."
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

def build_apptainer_cmd(tool: str, **options) -> str:
    """Build Apptainer command for neuroimaging tools."""
    if tool == "freesurfer":
        if "fs_license" not in options:
            raise ValueError("FreeSurfer license file path is required")
        
        # Build subject ID with session and run if present
        subject_id = f"sub-{options['participant_label']}"
        if options.get('session'):
            subject_id += f"_ses-{options['session']}"
        if options.get('run'):
            subject_id += f"_run-{options['run']}"
            
        return (
            f"apptainer run -B {options['fs_license']}:/usr/local/freesurfer/.license "
            f"-B {options['rawdata']}:/rawdata:ro -B {options['derivatives']}:/derivatives "
            f"{options['apptainer_img']} recon-all -all -subjid {subject_id} "
            f"-i {options['t1w']} "
            f"-sd /derivatives/{options['output_label']} "
            f"{options.get('additional_options', '')}"
        )
    elif tool == "fmriprep":
        fs_subjects_dir = options.get('fs_subjects_dir', '')
        fs_bind_option = (
            f"-B {fs_subjects_dir.parent}:/opt/freesurfer/subjects:ro" 
            if fs_subjects_dir else ""
        )
        fs_subjects_dir_option = (
            f"--fs-subjects-dir /opt/freesurfer/subjects/{fs_subjects_dir.name} " 
            if fs_subjects_dir else ""
        )
        
        return (
            f"apptainer run "
            f"-B {options['fs_license']}:/opt/freesurfer/license.txt "
            f"-B {options['rawdata']}:/data:ro "
            f"-B {options['derivatives']}:/derivatives "
            f"{fs_bind_option} "
            f"{options['apptainer_img']} "
            f"/data /derivatives participant "
            f"--participant-label {options['participant_label']} "
            f"--output-spaces {options.get('output_spaces', 'MNI152NLin2009cAsym:res-2')} "
            f"--nprocs {options.get('nprocs', 8)} "
            f"--omp-nthreads {options.get('omp_nthreads', 8)} "
            f"--fs-license-file /opt/freesurfer/license.txt "
            f"{options.get('fs_no_reconall', '')} "
            f"{fs_subjects_dir_option}"
        )
    elif tool == "qsiprep":
        return (
            f"apptainer run "
            f"-B {options['fs_license']}:/opt/freesurfer/license.txt "
            f"-B {options['rawdata']}:/data:ro "
            f"-B {options['derivatives']}:/out "
            f"{options['apptainer_img']} "
            f"/data /out participant "
            f"--participant-label {options['participant_label']} "
            f"--output-resolution {options['output_resolution']} "
            f"--denoise-method {options.get('denoise_method', 'dwidenoise')} "
            f"--nprocs {options.get('nprocs', 8)} "
            f"--omp-nthreads {options.get('omp_nthreads', 8)} "
            f"--fs-license-file /opt/freesurfer/license.txt "
            f"--skip-bids-validation "
            f"{options.get('dwi_only', '')} "
            f"{options.get('anat_only', '')}"
        )
    elif tool == "qsirecon":
        # QSIRecon for DWI reconstruction - requires QSIPrep preprocessed data
        qsiprep_dir = options.get('qsiprep_dir', '')
        if not qsiprep_dir:
            raise ValueError("qsiprep_dir is required for QSIRecon")
        
        return (
            f"apptainer run "
            f"-B {options['fs_license']}:/opt/freesurfer/license.txt "
            f"-B {qsiprep_dir}:/data:ro "
            f"-B {options['derivatives']}:/out "
            f"{options['apptainer_img']} "
            f"/data /out participant "
            f"--participant-label {options['participant_label']} "
            f"--recon-spec {options.get('recon_spec', 'mrtrix_multishell_msmt_ACT-hsvs')} "
            f"--nprocs {options.get('nprocs', 8)} "
            f"--omp-nthreads {options.get('omp_nthreads', 8)} "
            f"--fs-license-file /opt/freesurfer/license.txt "
            f"--skip-bids-validation "
            f"{options.get('additional_options', '')}"
        )
    elif tool == "meld_graph":
        # MELD Graph for lesion detection - requires FreeSurfer derivatives
        fs_subjects_dir = options.get('fs_subjects_dir', '')
        fs_bind_option = (
            f"-B {fs_subjects_dir}:/freesurfer:ro" 
            if fs_subjects_dir else ""
        )
        
        return (
            f"apptainer run "
            f"-B {options['rawdata']}:/data:ro "
            f"-B {options['derivatives']}:/output "
            f"{fs_bind_option} "
            f"{options['apptainer_img']} "
            f"--subject_id {options['participant_label']} "
            f"--subjects_dir /freesurfer "
            f"--output_dir /output "
            f"{options.get('additional_options', '')}"
        )
    
    else:
        raise ValueError(f"Unsupported tool: {tool}")


def launch_apptainer(apptainer_cmd):
    print(f"Launching apptainer image {apptainer_cmd}")
    os.system(apptainer_cmd)


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

