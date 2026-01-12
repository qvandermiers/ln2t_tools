import argparse
import warnings
import traceback
import sys
import logging
from pathlib import Path
from typing import Optional
from types import TracebackType  # Add this import

from ln2t_tools.utils.defaults import (
    DEFAULT_FS_LICENSE,
    DEFAULT_APPTAINER_DIR,
    MAX_PARALLEL_INSTANCES
)

# Import tool registry for dynamic tool loading
from ln2t_tools.tools import get_all_tools, auto_discover_tools

# Custom logging levels
MINIMAL = 25  # Between INFO (20) and WARNING (30)
logging.addLevelName(MINIMAL, "MINIMAL")


def configure_logging(verbosity: str) -> None:
    """Configure logging based on verbosity level.
    
    Parameters
    ----------
    verbosity : str
        One of: 'silent', 'minimal', 'verbose', 'debug'
        
        - silent: Only errors (ERROR level)
        - minimal: Essential info only (custom MINIMAL level, 25)
        - verbose: Detailed steps (INFO level, default)
        - debug: Everything including debug messages (DEBUG level)
    """
    level_map = {
        'silent': logging.ERROR,
        'minimal': MINIMAL,
        'verbose': logging.INFO,
        'debug': logging.DEBUG
    }
    
    level = level_map.get(verbosity, logging.INFO)
    
    # Select formatter based on verbosity
    if verbosity == 'debug':
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    elif verbosity == 'minimal':
        formatter = logging.Formatter('%(message)s')
    elif verbosity == 'silent':
        formatter = logging.Formatter('%(levelname)s: %(message)s')
    else:  # verbose
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Configure ln2t_tools logger specifically
    ln2t_logger = logging.getLogger('ln2t_tools')
    ln2t_logger.setLevel(level)
    
    # Update all existing handlers
    for handler in root_logger.handlers:
        handler.setLevel(level)
        handler.setFormatter(formatter)
    
    # If no handlers, add one
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)


def log_minimal(logger, message: str) -> None:
    """Log a message at MINIMAL level.
    
    Use this for essential information that should appear even in minimal mode.
    
    Parameters
    ----------
    logger : logging.Logger
        Logger instance
    message : str
        Message to log
    """
    logger.log(MINIMAL, message)


def add_common_arguments(parser, exclude_participant_label=False):
    """Add arguments common to all tools.
    
    Args:
        parser: argparse parser to add arguments to
        exclude_participant_label: If True, skip adding --participant-label argument
                                   (for dataset-wide tools like bids_validator)
    """
    parser.add_argument(
        "--dataset",
        help="BIDS dataset name (without -rawdata suffix)"
    )
    
    if not exclude_participant_label:
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
    
    parser.add_argument(
        "--tool-args",
        type=str,
        default="",
        help="Additional arguments passed directly to the tool container. "
             "Use quotes to pass multiple arguments, e.g., "
             '--tool-args "--output-resolution 2.0 --denoise-method dwidenoise". '
             "These arguments are appended verbatim to the container command. "
             "Refer to each tool's documentation for available options."
    )


def add_hpc_arguments(parser):
    """Add HPC cluster submission arguments."""
    parser.add_argument(
        "--hpc",
        action="store_true",
        help="Submit job to HPC cluster instead of running locally"
    )
    parser.add_argument(
        "--hpc-username",
        type=str,
        help="Username for HPC cluster (required if --hpc is used)"
    )
    parser.add_argument(
        "--hpc-hostname",
        type=str,
        help="HPC cluster hostname (required if --hpc is used)"
    )
    parser.add_argument(
        "--hpc-keyfile",
        type=str,
        default="~/.ssh/id_rsa",
        help="Path to SSH private key file (default: ~/.ssh/id_rsa)"
    )
    parser.add_argument(
        "--hpc-gateway",
        type=str,
        help="ProxyJump gateway hostname (optional, e.g., gwceci.ulb.ac.be)"
    )
    parser.add_argument(
        "--hpc-rawdata",
        type=str,
        help="Path to rawdata on HPC (default: $GLOBALSCRATCH/rawdata on cluster)"
    )
    parser.add_argument(
        "--hpc-derivatives",
        type=str,
        help="Path to derivatives on HPC (default: $GLOBALSCRATCH/derivatives on cluster)"
    )
    parser.add_argument(
        "--hpc-apptainer-dir",
        type=str,
        help="Path to apptainer images directory on HPC (default: $GLOBALSCRATCH/apptainer on cluster)"
    )
    parser.add_argument(
        "--hpc-fs-license",
        type=str,
        help="Path to FreeSurfer license on HPC (default: $HOME/licenses/license.txt on cluster)"
    )
    parser.add_argument(
        "--hpc-partition",
        type=str,
        help="HPC partition to use (default: None - let cluster decide)"
    )
    parser.add_argument(
        "--hpc-time",
        type=str,
        default="24:00:00",
        help="HPC job time limit (default: 24:00:00)"
    )
    parser.add_argument(
        "--hpc-mem",
        type=str,
        default="32G",
        help="HPC memory allocation (default: 32G)"
    )
    parser.add_argument(
        "--hpc-cpus",
        type=int,
        default=8,
        help="Number of CPUs to request (default: 8)"
    )
    parser.add_argument(
        "--hpc-gpus",
        type=int,
        default=1,
        help="Number of GPUs to request (default: 1, only for GPU-capable tools)"
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Uses the tool registry to dynamically create subparsers for each
    registered tool. This allows new tools to be added without modifying
    this file.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    # Auto-discover tools from the tools/ directory
    auto_discover_tools()
    
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

    parser.add_argument(
        "--verbosity",
        choices=["silent", "minimal", "verbose", "debug"],
        default="verbose",
        help="Logging verbosity level: silent (errors only), minimal (essential info), verbose (detailed steps, default), debug (everything)"
    )

    # Create subparsers for each tool
    subparsers = parser.add_subparsers(dest='tool', help='Neuroimaging tool to use')

    # Dataset-wide tools that don't operate on individual participants
    dataset_wide_tools = ['bids_validator']

    # Dynamically create subparsers from registered tools
    for tool_name, tool_class in get_all_tools().items():
        tool_parser = subparsers.add_parser(
            tool_name,
            help=tool_class.help_text if hasattr(tool_class, 'help_text') else tool_class.description,
            description=tool_class.description,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        # Add common arguments (dataset, participant, version, etc.)
        # For dataset-wide tools, exclude --participant-label
        add_common_arguments(tool_parser, exclude_participant_label=(tool_name in dataset_wide_tools))
        # Add HPC arguments for cluster submission (not for dataset-wide tools)
        if tool_name not in dataset_wide_tools:
            add_hpc_arguments(tool_parser)
        # NOTE: Tool-specific arguments are NO LONGER added here.
        # Instead, users should use --tool-args to pass any tool-specific
        # options directly to the container. This decouples ln2t_tools
        # from tool-specific CLI changes.

    # Import subcommand (special case - not a standard tool)
    parser_import = subparsers.add_parser(
        'import',
        help='Import source data to BIDS format (ADMIN ONLY)',
        description="""
Import source data to BIDS format.

⚠️  WARNING: ADMIN ONLY
This tool is intended for administrators only. It requires:
  - READ access to sourcedata directory
  - WRITE access to rawdata directory
  
Standard users should not use this tool. Imported data will be provided
by administrators in the rawdata directory.
""",
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
        "--skip-source-compression",
        action="store_true",
        help="Skip compressing source data after import (by default, source is compressed and original deleted)"
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
    parser_import.add_argument(
        "--keep-tmp-files",
        action="store_true",
        help="Keep temporary files created by dcm2bids (tmp_dcm2bids directory in rawdata)"
    )
    # Pre-import options (for MRS and physio data)
    parser_import.add_argument(
        "--pre-import",
        action="store_true",
        help="Run pre-import step: gather source files from scanner backup locations. "
             "Use with --datatype mrs or --datatype physio to specify which data to pre-import."
    )
    parser_import.add_argument(
        "--mrraw-dir",
        type=Path,
        help="Path to scanner mrraw directory for MRS (default: /home/ln2t-worker/PETMR/backup/auto/daily_backups/mrraw)"
    )
    parser_import.add_argument(
        "--mrs-tmp-dir",
        type=Path,
        help="Path to scanner tmp directory with exam folders for MRS (default: /home/ln2t-worker/PETMR/backup/auto/daily_backups/tmp)"
    )
    parser_import.add_argument(
        "--physio-backup-dir",
        type=Path,
        help="Path to physio backup directory (default: $HOME/PETMR/backup/auto/daily_backups/gating)"
    )
    parser_import.add_argument(
        "--pre-import-tolerance-hours",
        type=float,
        default=None,
        help="Time tolerance in hours for finding source files by datetime during pre-import. "
             "Default: 1.0, or value from PhysioPreImportTolerance in physio config file."
    )
    parser_import.add_argument(
        "--matching-tolerance-sec",
        type=float,
        default=None,
        help="Time tolerance in seconds for matching physio recordings to fMRI runs. "
             "Default: 35.0, or value from PhysioMatchingTolerance in physio config file. "
             "GE physio starts 30s before fMRI, so 35s accounts for offset + timing variations."
    )
    parser_import.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually copying files (for --pre-import)"
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

