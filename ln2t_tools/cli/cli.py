import argparse
import warnings
import traceback
import sys
from pathlib import Path
from typing import Optional
from types import TracebackType  # Add this import

from ln2t_tools.utils.defaults import (
    DEFAULT_FS_LICENSE,
    DEFAULT_APPTAINER_DIR,
    MAX_PARALLEL_INSTANCES
)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="LN2T Tools - Neuroimaging Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "tool",
        nargs='?',  # Make tool optional
        choices=["freesurfer", "fmriprep", "qsiprep", "qsirecon", "meld_graph", "import"],
        help="Neuroimaging tool to use (optional if using config file)"
    )

    parser.add_argument(
        "--dataset",
        help="BIDS dataset name (without -rawdata suffix)"
    )

    parser.add_argument(
        "--participant-label",
        nargs='+',
        help="One or more participant labels (without 'sub-' prefix)"
    )

    parser.add_argument(
        "--participants-file",
        type=str,
        help="Path to a text file with one subject ID per line (with or without 'sub-' prefix), used for harmonization runs"
    )

    parser.add_argument(
        "--output-label",
        help="Custom label for output directory"
    )

    parser.add_argument(
        "--fs-license",
        type=Path,
        default=DEFAULT_FS_LICENSE,
        help="Path to FreeSurfer license file"
    )

    parser.add_argument(
        "--apptainer-dir",
        type=Path,
        default=DEFAULT_APPTAINER_DIR,
        help="Path to Apptainer images directory"
    )

    parser.add_argument(
        "--version",
        help="Tool version to use"
    )

    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List available BIDS datasets"
    )

    parser.add_argument(
        "--list-missing",
        action="store_true",
        help="List subjects missing from output"
    )

    parser.add_argument(
        "--list-instances",
        action="store_true",
        help="Show currently running instances"
    )

    parser.add_argument(
        "--fs-no-reconall",
        action="store_true",
        help="Skip FreeSurfer surface reconstruction (fMRIPrep only)"
    )

    parser.add_argument(
        "--output-spaces",
        default="MNI152NLin2009cAsym:res-2",
        help="Output spaces for fMRIPrep (default: MNI152NLin2009cAsym:res-2)"
    )

    parser.add_argument(
        "--nprocs",
        type=int,
        default=8,
        help="Number of processes to use (default: 8)"
    )

    parser.add_argument(
        "--omp-nthreads",
        type=int,
        default=8,
        help="Number of OpenMP threads (default: 8)"
    )

    # QSIPrep specific arguments
    parser.add_argument(
        "--output-resolution",
        type=float,
        help="Isotropic voxel size in mm for QSIPrep output (required for QSIPrep)"
    )

    parser.add_argument(
        "--denoise-method",
        choices=["dwidenoise", "patch2self", "none"],
        default="dwidenoise",
        help="Denoising method for QSIPrep (default: dwidenoise)"
    )

    parser.add_argument(
        "--dwi-only",
        action="store_true",
        help="Process only DWI data, ignore anatomical data (QSIPrep only)"
    )

    parser.add_argument(
        "--anat-only",
        action="store_true",
        help="Process only anatomical data (QSIPrep only)"
    )

    # QSIRecon specific arguments
    parser.add_argument(
        "--qsiprep-version",
        help="QSIPrep version to use as input for QSIRecon (default: uses DEFAULT_QSIPREP_VERSION)"
    )

    parser.add_argument(
        "--recon-spec",
        default="mrtrix_multishell_msmt_ACT-hsvs",
        help="Reconstruction spec for QSIRecon (default: mrtrix_multishell_msmt_ACT-hsvs)"
    )

    # MELD Graph specific arguments
    parser.add_argument(
        "--fs-version",
        help="FreeSurfer version to use for MELD Graph input (default: uses DEFAULT_MELD_FS_VERSION)"
    )

    parser.add_argument(
        "--download-weights",
        action="store_true",
        help="Download MELD Graph model weights (run once before first use)"
    )

    parser.add_argument(
        "--harmonize",
        action="store_true",
        help="Compute harmonization parameters for the provided cohort (use with --participant-label or --participants-file)"
    )

    parser.add_argument(
        "--harmonize-only",
        action="store_true",
        help="Compute harmonization parameters only (requires --harmo-code)"
    )

    parser.add_argument(
        "--harmo-code",
        help="Harmonization code for scanner (e.g., H1, H2). Use when running with harmonization"
    )

    parser.add_argument(
        "--demographics",
        help="Path to demographics CSV file for harmonization (optional - will auto-generate from participants.tsv if not provided)"
    )

    parser.add_argument(
        "--use-precomputed-fs",
        action="store_true",
        help="Use precomputed FreeSurfer outputs instead of running FreeSurfer"
    )

    parser.add_argument(
        "--skip-segmentation",
        action="store_true",
        help="Skip FreeSurfer segmentation step (use with --use-precomputed-fs)"
    )

    parser.add_argument(
        "--no-gpu",
        action="store_true",
        help="Disable GPU and use CPU for MELD Graph inference (slower but uses less memory)"
    )

    parser.add_argument(
        "--gpu-memory-limit",
        type=int,
        default=128,
        help="GPU memory split size in MB for PyTorch (default: 128). Try 256, 512, or higher if getting CUDA OOM errors"
    )

    parser.add_argument(
        "--slurm",
        action="store_true",
        help="Submit job to SLURM HPC cluster (lyra.ulb.be) instead of running locally"
    )

    parser.add_argument(
        "--slurm-user",
        type=str,
        help="Username for HPC cluster (required if --slurm is used)"
    )

    parser.add_argument(
        "--slurm-host",
        type=str,
        default="lyra.ulb.be",
        help="HPC cluster hostname (default: lyra.ulb.be)"
    )

    parser.add_argument(
        "--slurm-rawdata",
        type=str,
        help="Path to rawdata on HPC (default: $GLOBALSCRATCH/rawdata on cluster)"
    )

    parser.add_argument(
        "--slurm-derivatives",
        type=str,
        help="Path to derivatives on HPC (default: $GLOBALSCRATCH/derivatives on cluster)"
    )

    parser.add_argument(
        "--slurm-apptainer-dir",
        type=str,
        help="Path to apptainer images directory on HPC (required if --slurm is used)"
    )

    parser.add_argument(
        "--slurm-fs-license",
        type=str,
        help="Path to FreeSurfer license on HPC (default: $HOME/licenses/license.txt on cluster)"
    )

    parser.add_argument(
        "--slurm-fs-version",
        type=str,
        default="7.2.0",
        help="FreeSurfer version to use on HPC (default: 7.2.0)"
    )

    parser.add_argument(
        "--slurm-partition",
        type=str,
        default=None,
        help="SLURM partition to use (default: None - let cluster decide)"
    )

    parser.add_argument(
        "--slurm-time",
        type=str,
        default="1:00:00",
        help="SLURM job time limit (default: 1:00:00)"
    )

    parser.add_argument(
        "--slurm-mem",
        type=str,
        default="32G",
        help="SLURM memory allocation (default: 32G)"
    )

    parser.add_argument(
        "--slurm-gpus",
        type=int,
        default=1,
        help="Number of GPUs to request (default: 1)"
    )

    parser.add_argument(
        "--max-instances",
        type=int,
        default=MAX_PARALLEL_INSTANCES,
        help=f"Maximum number of parallel instances (default: {MAX_PARALLEL_INSTANCES})"
    )

    # Import tool specific arguments
    parser.add_argument(
        "--datatype",
        choices=["dicom", "physio", "mrs", "all"],
        default="all",
        help="Type of source data to import (default: all)"
    )

    parser.add_argument(
        "--session",
        help="Session label (without 'ses-' prefix) for multi-session datasets"
    )

    parser.add_argument(
        "--ds-initials",
        help="Dataset initials prefix for source data (e.g., 'CB', 'HP')"
    )

    parser.add_argument(
        "--compress-source",
        action="store_true",
        help="Compress source data after successful import (creates .tar.gz archives)"
    )

    parser.add_argument(
        "--deface",
        action="store_true",
        help="Deface anatomical images after import (import tool only)"
    )

    parser.add_argument(
        "--import-env",
        type=Path,
        help="Path to Python virtual environment for import tools (default: ~/venvs/general_purpose_env)"
    )

    parser.add_argument(
        "--phys2bids",
        action="store_true",
        help="Use phys2bids for physiological data import (default: use in-house processing)"
    )

    parser.add_argument(
        "--physio-config",
        type=Path,
        help="Path to physiological data configuration file (JSON format with DummyVolumes). "
             "If not specified, auto-detects from sourcedata/configs/physio.json or physio/config.json"
    )

    return parser.parse_args()


def setup_terminal_colors() -> None:
    """Configure colored output for warnings and errors."""
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'

    def warning_formatter(
        message: str,
        category: Warning,
        filename: str,
        lineno: int,
        line: Optional[str] = None
    ) -> str:
        """Format warning messages with color."""
        return f"{YELLOW}{filename}:{lineno}: {category.__name__}: {message}{RESET}\n"

    def exception_handler(
        exc_type: type,
        exc_value: Exception,
        exc_traceback: TracebackType  # Now TracebackType is properly imported
    ) -> None:
        """Format exception messages with color."""
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        print(f"{RED}{tb_str}{RESET}", file=sys.stderr)

    warnings.formatwarning = warning_formatter
    sys.excepthook = exception_handler

