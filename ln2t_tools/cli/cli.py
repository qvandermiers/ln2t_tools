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
        choices=["freesurfer", "fmriprep", "qsiprep", "qsirecon", "meld_graph"],
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
        help="FreeSurfer version to use for MELD Graph input (default: uses DEFAULT_FS_VERSION)"
    )

    parser.add_argument(
        "--max-instances",
        type=int,
        default=MAX_PARALLEL_INSTANCES,
        help=f"Maximum number of parallel instances (default: {MAX_PARALLEL_INSTANCES})"
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

