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


def add_common_arguments(parser):
    """Add arguments common to all tools."""
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
        "--max-instances",
        type=int,
        default=MAX_PARALLEL_INSTANCES,
        help=f"Maximum number of parallel instances (default: {MAX_PARALLEL_INSTANCES})"
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

    # Global options (without subcommands)
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List available BIDS datasets"
    )

    parser.add_argument(
        "--list-missing",
        action="store_true",
        help="List subjects missing from output (requires --dataset and tool)"
    )

    parser.add_argument(
        "--list-instances",
        action="store_true",
        help="Show currently running instances"
    )

    # Create subparsers for each tool
    subparsers = parser.add_subparsers(dest='tool', help='Neuroimaging tool to use')

    # FreeSurfer subcommand
    parser_freesurfer = subparsers.add_parser(
        'freesurfer',
        help='FreeSurfer cortical reconstruction',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_common_arguments(parser_freesurfer)

    # fMRIPrep subcommand
    parser_fmriprep = subparsers.add_parser(
        'fmriprep',
        help='fMRIPrep functional MRI preprocessing',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_common_arguments(parser_fmriprep)
    parser_fmriprep.add_argument(
        "--fs-no-reconall",
        action="store_true",
        help="Skip FreeSurfer surface reconstruction"
    )
    parser_fmriprep.add_argument(
        "--output-spaces",
        default="MNI152NLin2009cAsym:res-2",
        help="Output spaces (default: MNI152NLin2009cAsym:res-2)"
    )
    parser_fmriprep.add_argument(
        "--nprocs",
        type=int,
        default=8,
        help="Number of processes to use (default: 8)"
    )
    parser_fmriprep.add_argument(
        "--omp-nthreads",
        type=int,
        default=8,
        help="Number of OpenMP threads (default: 8)"
    )

    # QSIPrep subcommand
    parser_qsiprep = subparsers.add_parser(
        'qsiprep',
        help='QSIPrep diffusion MRI preprocessing',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_common_arguments(parser_qsiprep)
    parser_qsiprep.add_argument(
        "--output-resolution",
        type=float,
        required=True,
        help="Isotropic voxel size in mm for output (required)"
    )
    parser_qsiprep.add_argument(
        "--denoise-method",
        choices=["dwidenoise", "patch2self", "none"],
        default="dwidenoise",
        help="Denoising method (default: dwidenoise)"
    )
    parser_qsiprep.add_argument(
        "--dwi-only",
        action="store_true",
        help="Process only DWI data, ignore anatomical data"
    )
    parser_qsiprep.add_argument(
        "--anat-only",
        action="store_true",
        help="Process only anatomical data"
    )
    parser_qsiprep.add_argument(
        "--nprocs",
        type=int,
        default=8,
        help="Number of processes to use (default: 8)"
    )
    parser_qsiprep.add_argument(
        "--omp-nthreads",
        type=int,
        default=8,
        help="Number of OpenMP threads (default: 8)"
    )

    # QSIRecon subcommand
    parser_qsirecon = subparsers.add_parser(
        'qsirecon',
        help='QSIRecon diffusion MRI reconstruction',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_common_arguments(parser_qsirecon)
    parser_qsirecon.add_argument(
        "--qsiprep-version",
        help="QSIPrep version to use as input (default: uses DEFAULT_QSIPREP_VERSION)"
    )
    parser_qsirecon.add_argument(
        "--recon-spec",
        default="mrtrix_multishell_msmt_ACT-hsvs",
        help="Reconstruction spec (default: mrtrix_multishell_msmt_ACT-hsvs)"
    )
    parser_qsirecon.add_argument(
        "--nprocs",
        type=int,
        default=8,
        help="Number of processes to use (default: 8)"
    )
    parser_qsirecon.add_argument(
        "--omp-nthreads",
        type=int,
        default=8,
        help="Number of OpenMP threads (default: 8)"
    )

    # MELD Graph subcommand
    parser_meld = subparsers.add_parser(
        'meld_graph',
        help='MELD Graph lesion detection',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_common_arguments(parser_meld)
    parser_meld.add_argument(
        "--participants-file",
        type=str,
        help="Path to a text file with one subject ID per line (with or without 'sub-' prefix), used for harmonization runs"
    )
    parser_meld.add_argument(
        "--fs-version",
        help="FreeSurfer version to use as input (default: uses DEFAULT_MELD_FS_VERSION)"
    )
    parser_meld.add_argument(
        "--download-weights",
        action="store_true",
        help="Download MELD Graph model weights (run once before first use)"
    )
    parser_meld.add_argument(
        "--harmonize",
        action="store_true",
        help="Compute harmonization parameters for the provided cohort"
    )
    parser_meld.add_argument(
        "--harmonize-only",
        action="store_true",
        help="Compute harmonization parameters only (requires --harmo-code)"
    )
    parser_meld.add_argument(
        "--harmo-code",
        help="Harmonization code for scanner (e.g., H1, H2)"
    )
    parser_meld.add_argument(
        "--demographics",
        help="Path to demographics CSV file (optional - auto-generated from participants.tsv if not provided)"
    )
    parser_meld.add_argument(
        "--use-precomputed-fs",
        action="store_true",
        help="Use precomputed FreeSurfer outputs instead of running FreeSurfer"
    )
    parser_meld.add_argument(
        "--skip-segmentation",
        action="store_true",
        help="Skip FreeSurfer segmentation step (use with --use-precomputed-fs)"
    )
    parser_meld.add_argument(
        "--no-gpu",
        action="store_true",
        help="Disable GPU and use CPU for inference (slower but uses less memory)"
    )
    parser_meld.add_argument(
        "--gpu-memory-limit",
        type=int,
        default=128,
        help="GPU memory split size in MB for PyTorch (default: 128)"
    )
    parser_meld.add_argument(
        "--slurm",
        action="store_true",
        help="Submit job to SLURM HPC cluster instead of running locally"
    )
    parser_meld.add_argument(
        "--slurm-user",
        type=str,
        help="Username for HPC cluster (required if --slurm is used)"
    )
    parser_meld.add_argument(
        "--slurm-host",
        type=str,
        default="lyra.ulb.be",
        help="HPC cluster hostname (default: lyra.ulb.be)"
    )
    parser_meld.add_argument(
        "--slurm-rawdata",
        type=str,
        help="Path to rawdata on HPC (default: $GLOBALSCRATCH/rawdata on cluster)"
    )
    parser_meld.add_argument(
        "--slurm-derivatives",
        type=str,
        help="Path to derivatives on HPC (default: $GLOBALSCRATCH/derivatives on cluster)"
    )
    parser_meld.add_argument(
        "--slurm-apptainer-dir",
        type=str,
        help="Path to apptainer images directory on HPC (required if --slurm is used)"
    )
    parser_meld.add_argument(
        "--slurm-fs-license",
        type=str,
        help="Path to FreeSurfer license on HPC (default: $HOME/licenses/license.txt on cluster)"
    )
    parser_meld.add_argument(
        "--slurm-fs-version",
        type=str,
        default="7.2.0",
        help="FreeSurfer version to use on HPC (default: 7.2.0)"
    )
    parser_meld.add_argument(
        "--slurm-partition",
        type=str,
        default=None,
        help="SLURM partition to use (default: None - let cluster decide)"
    )
    parser_meld.add_argument(
        "--slurm-time",
        type=str,
        default="1:00:00",
        help="SLURM job time limit (default: 1:00:00)"
    )
    parser_meld.add_argument(
        "--slurm-mem",
        type=str,
        default="32G",
        help="SLURM memory allocation (default: 32G)"
    )
    parser_meld.add_argument(
        "--slurm-gpus",
        type=int,
        default=1,
        help="Number of GPUs to request (default: 1)"
    )

    # Import subcommand
    parser_import = subparsers.add_parser(
        'import',
        help='Import source data to BIDS format',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_common_arguments(parser_import)
    parser_import.add_argument(
        "--datatype",
        choices=["dicom", "physio", "mrs", "all"],
        default="all",
        help="Type of source data to import (default: all)"
    )
    parser_import.add_argument(
        "--session",
        help="Session label (without 'ses-' prefix) for multi-session datasets"
    )
    parser_import.add_argument(
        "--ds-initials",
        help="Dataset initials prefix for source data (e.g., 'CB', 'HP')"
    )
    parser_import.add_argument(
        "--compress-source",
        action="store_true",
        help="Compress source data after successful import (creates .tar.gz archives)"
    )
    parser_import.add_argument(
        "--deface",
        action="store_true",
        help="Deface anatomical images after import"
    )
    parser_import.add_argument(
        "--import-env",
        type=Path,
        help="Path to Python virtual environment for import tools (default: ~/venvs/general_purpose_env)"
    )
    parser_import.add_argument(
        "--phys2bids",
        action="store_true",
        help="Use phys2bids for physiological data import (default: use in-house processing)"
    )
    parser_import.add_argument(
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

