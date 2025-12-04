"""Default values and constants for ln2t_tools."""
from pathlib import Path

# Resource management
MAX_PARALLEL_INSTANCES = 10
LOCKFILE_DIR = Path("/tmp/ln2t_tools_locks")

# Default directories
DEFAULT_RAWDATA = Path.home() / Path("rawdata")
DEFAULT_DERIVATIVES = Path.home() / Path("derivatives")
DEFAULT_CODE = Path.home() / Path("code")
DEFAULT_SOURCEDATA = Path.home() / Path("sourcedata")
DEFAULT_APPTAINER_DIR = Path("/opt/apptainer")

# Tool versions
DEFAULT_FS_VERSION = "7.3.2"
DEFAULT_FASTSURFER_VERSION = "cuda-v2.4.2"  # Full Docker tag for reproducibility
DEFAULT_FMRIPREP_VERSION = "25.1.4"
DEFAULT_QSIPREP_VERSION = "1.0.1"
DEFAULT_QSIRECON_VERSION = "1.1.1"
DEFAULT_MELDGRAPH_VERSION = "v2.2.3"
DEFAULT_MELD_FS_VERSION = "7.2.0"  # MELD Graph requires FreeSurfer 7.2.0 or earlier

# FreeSurfer license
DEFAULT_FS_LICENSE = Path("/opt/freesurfer/.license")