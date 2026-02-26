import argparse
import warnings
import traceback
import sys
import logging
import textwrap
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


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class ColoredHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom formatter with colored section headers."""

    def __init__(self, prog, indent_increment=2, max_help_position=40, width=100):
        super().__init__(prog, indent_increment, max_help_position, width)

    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = f'{Colors.BOLD}Usage:{Colors.END} '
        return super()._format_usage(usage, actions, groups, prefix)

    def start_section(self, heading):
        if heading:
            heading = f'{Colors.BOLD}{Colors.CYAN}{heading}{Colors.END}'
        super().start_section(heading)


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
    required = parser.add_argument_group(
        f'{Colors.BOLD}Required Arguments{Colors.END}'
    )
    
    required.add_argument(
        "--dataset",
        help="BIDS dataset name (without -rawdata suffix)"
    )
    
    general = parser.add_argument_group(
        f'{Colors.BOLD}General Options{Colors.END}'
    )
    
    if not exclude_participant_label:
        general.add_argument(
            "--participant-label",
            nargs='+',
            help="One or more participant labels (without 'sub-' prefix)"
        )
    
    general.add_argument(
        "--output-label",
        help="Custom label for output directory"
    )
    
    general.add_argument(
        "--version",
        help="Tool version to use"
    )
    
    general.add_argument(
        "--list-missing",
        action="store_true",
        help="List participants in rawdata that are missing from tool derivatives. "
             "Shows which subjects need processing and provides a pre-typed command to run them."
    )
    
    processing = parser.add_argument_group(
        f'{Colors.BOLD}Processing Options{Colors.END}'
    )
    
    processing.add_argument(
        "--max-instances",
        type=int,
        default=MAX_PARALLEL_INSTANCES,
        help=f"Maximum number of parallel instances (default: {MAX_PARALLEL_INSTANCES})"
    )
    
    processing.add_argument(
        "--tool-args",
        type=str,
        default="",
        help="Additional arguments passed directly to the tool container. "
             "Use quotes with equals sign or after --, e.g., "
             '--tool-args="--json --ignoreWarnings" or -- --tool-args "--json". '
             "These arguments are appended verbatim to the container command. "
             "Refer to each tool's documentation for available options."
    )
    
    paths = parser.add_argument_group(
        f'{Colors.BOLD}Path Options{Colors.END}'
    )
    
    paths.add_argument(
        "--fs-license",
        type=Path,
        default=DEFAULT_FS_LICENSE,
        help="Path to FreeSurfer license file"
    )
    
    paths.add_argument(
        "--apptainer-dir",
        type=Path,
        default=DEFAULT_APPTAINER_DIR,
        help="Path to Apptainer images directory"
    )
    
    paths.add_argument(
        "--fs-version",
        default=None,
        help="FreeSurfer version to use for input data (when tool depends on FreeSurfer). "
             "Default: auto-detect latest"
    )


def add_hpc_arguments(parser):
    """Add HPC cluster submission arguments."""
    hpc_submit = parser.add_argument_group(
        f'{Colors.BOLD}HPC Submission Options{Colors.END}'
    )
    
    hpc_submit.add_argument(
        "--hpc",
        action="store_true",
        help="Submit job to HPC cluster instead of running locally"
    )
    
    hpc_auth = parser.add_argument_group(
        f'{Colors.BOLD}HPC Authentication{Colors.END}'
    )
    
    hpc_auth.add_argument(
        "--hpc-username",
        type=str,
        help="Username for HPC cluster (required if --hpc is used)"
    )
    hpc_auth.add_argument(
        "--hpc-hostname",
        type=str,
        help="HPC cluster hostname (required if --hpc is used)"
    )
    hpc_auth.add_argument(
        "--hpc-keyfile",
        type=str,
        default="~/.ssh/id_rsa",
        help="Path to SSH private key file (default: ~/.ssh/id_rsa)"
    )
    hpc_auth.add_argument(
        "--hpc-gateway",
        type=str,
        help="ProxyJump gateway hostname (optional, e.g., gwceci.ulb.ac.be)"
    )
    
    hpc_paths = parser.add_argument_group(
        f'{Colors.BOLD}HPC Paths{Colors.END}'
    )
    
    hpc_paths.add_argument(
        "--hpc-rawdata",
        type=str,
        help="Path to rawdata on HPC (default: $GLOBALSCRATCH/rawdata on cluster)"
    )
    hpc_paths.add_argument(
        "--hpc-derivatives",
        type=str,
        help="Path to derivatives on HPC (default: $GLOBALSCRATCH/derivatives on cluster)"
    )
    hpc_paths.add_argument(
        "--hpc-apptainer-dir",
        type=str,
        help="Path to apptainer images directory on HPC (default: $GLOBALSCRATCH/apptainer on cluster)"
    )
    hpc_paths.add_argument(
        "--hpc-fs-license",
        type=str,
        help="Path to FreeSurfer license on HPC (default: $HOME/licenses/license.txt on cluster)"
    )
    
    hpc_resources = parser.add_argument_group(
        f'{Colors.BOLD}HPC Resources{Colors.END}'
    )
    
    hpc_resources.add_argument(
        "--hpc-partition",
        type=str,
        help="HPC partition to use (default: None - let cluster decide)"
    )
    hpc_resources.add_argument(
        "--hpc-time",
        type=str,
        default="24:00:00",
        help="HPC job time limit (default: 24:00:00)"
    )
    hpc_resources.add_argument(
        "--hpc-mem",
        type=str,
        default="32G",
        help="HPC memory allocation (default: 32G)"
    )
    hpc_resources.add_argument(
        "--hpc-cpus",
        type=int,
        default=8,
        help="Number of CPUs to request (default: 8)"
    )
    hpc_resources.add_argument(
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
    
    description = textwrap.dedent(f"""
    {Colors.BOLD}{Colors.GREEN}╔══════════════════════════════════════════════════════════════════════════════╗
    ║                     LN2T TOOLS v0.1.0                                        ║
    ║               Neuroimaging Pipeline Runner                                   ║
    ║          Brain imaging data processing and analysis platform                 ║
    ╚══════════════════════════════════════════════════════════════════════════════╝{Colors.END}

    {Colors.BOLD}Description:{Colors.END}
      Unified command-line interface for running neuroimaging analysis pipelines
      on BIDS-formatted datasets. Supports multiple tools including FreeSurfer,
      fMRIPrep, QSIPrep, and more.

    {Colors.BOLD}Workflow:{Colors.END}
      1. Import source data to BIDS format
      2. Run analysis tools on BIDS dataset
      3. Monitor processing progress
      4. Manage HPC cluster submissions (optional)
    """)

    epilog = textwrap.dedent(f"""
    {Colors.BOLD}{Colors.GREEN}═══════════════════════════════════════════════════════════════════════════════{Colors.END}
    {Colors.BOLD}EXAMPLES{Colors.END}
    {Colors.GREEN}═══════════════════════════════════════════════════════════════════════════════{Colors.END}

    {Colors.BOLD}Listing and Exploring:{Colors.END}

      {Colors.YELLOW}# List available BIDS datasets{Colors.END}
      ln2t_tools --list-datasets

      {Colors.YELLOW}# Show available tools for a specific dataset{Colors.END}
      ln2t_tools freesurfer --help

    {Colors.BOLD}Importing Data:{Colors.END}

      {Colors.YELLOW}# Import all source data (DICOM, physio, MRS) to BIDS{Colors.END}
      ln2t_tools import --dataset MyStudy --datatype all

      {Colors.YELLOW}# Import only DICOM data{Colors.END}
      ln2t_tools import --dataset MyStudy --datatype dicom

      {Colors.YELLOW}# Deface anatomical images after import{Colors.END}
      ln2t_tools import --dataset MyStudy --deface

    {Colors.BOLD}Running Analysis Tools:{Colors.END}

      {Colors.YELLOW}# Run FreeSurfer on all participants{Colors.END}
      ln2t_tools freesurfer --dataset MyStudy

      {Colors.YELLOW}# Run FreeSurfer on specific participants{Colors.END}
      ln2t_tools freesurfer --dataset MyStudy --participant-label 01 02 03

      {Colors.YELLOW}# Enable verbose logging{Colors.END}
      ln2t_tools freesurfer --dataset MyStudy --verbosity debug

      {Colors.YELLOW}# Pass additional arguments to tool{Colors.END}
      ln2t_tools freesurfer --dataset MyStudy --tool-args "--openmp 4"

    {Colors.BOLD}HPC Cluster Submission:{Colors.END}

      {Colors.YELLOW}# Submit job to HPC cluster{Colors.END}
      ln2t_tools freesurfer --dataset MyStudy --hpc --hpc-username myuser \\
          --hpc-hostname login.cluster.edu

      {Colors.YELLOW}# Check HPC job status{Colors.END}
      ln2t_tools --hpc-status recent --dataset MyStudy

    {Colors.BOLD}Managing Instances:{Colors.END}

      {Colors.YELLOW}# List currently running instances{Colors.END}
      ln2t_tools --list-instances

      {Colors.YELLOW}# Find missing subjects that need processing{Colors.END}
      ln2t_tools --dataset MyStudy freesurfer --list-missing

    {Colors.BOLD}{Colors.GREEN}═══════════════════════════════════════════════════════════════════════════════{Colors.END}
    {Colors.BOLD}AVAILABLE TOOLS{Colors.END}
    {Colors.GREEN}═══════════════════════════════════════════════════════════════════════════════{Colors.END}

      Use 'ln2t_tools <tool> --help' to see options for a specific tool.

    {Colors.BOLD}MORE INFORMATION{Colors.END}
    {Colors.GREEN}═══════════════════════════════════════════════════════════════════════════════{Colors.END}

      Documentation:  https://github.com/ln2t/ln2t_tools
      Report Issues:  https://github.com/ln2t/ln2t_tools/issues
      Version:        0.1.0
    """)
    
    parser = argparse.ArgumentParser(
        prog="ln2t_tools",
        description=description,
        epilog=epilog,
        formatter_class=ColoredHelpFormatter,
        add_help=False,
    )

    # Global options (without subcommands)
    general = parser.add_argument_group(
        f'{Colors.BOLD}General Options{Colors.END}'
    )

    general.add_argument(
        "-h", "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    general.add_argument(
        "--version",
        action="version",
        version="ln2t_tools 0.1.0",
        help="Show program version and exit.",
    )

    general.add_argument(
        "--verbosity",
        choices=["silent", "minimal", "verbose", "debug"],
        default="verbose",
        help="Logging verbosity level: silent (errors only), minimal (essential info), verbose (detailed steps, default), debug (everything)"
    )

    dataset_ops = parser.add_argument_group(
        f'{Colors.BOLD}Dataset Operations{Colors.END}'
    )

    dataset_ops.add_argument(
        "--list-datasets",
        action="store_true",
        help="List available BIDS datasets"
    )

    dataset_ops.add_argument(
        "--list-missing",
        action="store_true",
        help="List subjects missing from output (requires --dataset and tool)"
    )

    dataset_ops.add_argument(
        "--list-instances",
        action="store_true",
        help="Show currently running instances"
    )

    hpc_ops = parser.add_argument_group(
        f'{Colors.BOLD}HPC Cluster Operations{Colors.END}'
    )

    hpc_ops.add_argument(
        "--hpc-status",
        nargs='?',
        const='recent',
        metavar='JOB_ID',
        help="Check status of HPC jobs. Provide job ID(s) to check specific jobs, or leave empty for recent. "
             "Combine with --dataset or --tool to filter by dataset/tool. "
             "Usage: --hpc-status (recent), --hpc-status 12345 (specific), "
             "--hpc-status --dataset D (all for dataset)"
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
            formatter_class=ColoredHelpFormatter
        )
        # Add common arguments (dataset, participant, version, etc.)
        # For dataset-wide tools, exclude --participant-label
        add_common_arguments(tool_parser, exclude_participant_label=(tool_name in dataset_wide_tools))
        # Add HPC arguments for cluster submission (not for dataset-wide tools)
        if tool_name not in dataset_wide_tools:
            add_hpc_arguments(tool_parser)
        # Add tool-specific arguments if the tool class provides them
        if hasattr(tool_class, 'add_arguments'):
            tool_class.add_arguments(tool_parser)

    # Import subcommand (special case - not a standard tool)
    parser_import = subparsers.add_parser(
        'import',
        help='Import source data to BIDS format (ADMIN ONLY)',
        description=f"""
{Colors.BOLD}{Colors.GREEN}╔══════════════════════════════════════════════════════════════════════════════╗
║                    IMPORT SOURCE DATA TO BIDS                          ║
╚══════════════════════════════════════════════════════════════════════════════╝{Colors.END}

{Colors.BOLD}Important:{Colors.END}
  ⚠️  {Colors.BOLD}ADMIN ONLY{Colors.END}
  This tool is intended for administrators only. It requires:
    • READ access to sourcedata directory
    • WRITE access to rawdata directory
  
  Standard users should not use this tool.

""",
        formatter_class=ColoredHelpFormatter
    )
    add_common_arguments(parser_import)
    parser_import.add_argument(
        "--datatype",
        choices=["dicom", "physio", "mrs", "meg", "all"],
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
        "--overwrite",
        action="store_true",
        help="Overwrite existing imported data. By default, skips import if participant folder already exists"
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
    parser_import.add_argument(
        "--only-uncompressed",
        action="store_true",
        help="Only check for uncompressed source data folders (e.g., AB001) and disregard compressed archives (e.g., AB001.tar.gz). "
             "Useful for avoiding data duplication issues when both uncompressed and compressed versions exist."
    )
    # Pre-import and full-import options (for MRS and physio data)
    parser_import.add_argument(
        "--pre-import",
        action="store_true",
        help="Run pre-import step: gather source files from scanner backup locations. "
             "Use with --datatype mrs or --datatype physio to specify which data to pre-import."
    )
    parser_import.add_argument(
        "--full",
        action="store_true",
        help="Run pre-import followed by import in sequence. For DICOM and MEG data, runs standard import. "
             "For MRS and PHYSIO data, runs pre-import first, then chains to standard import. "
             "Participants are auto-discovered from DICOM or specified via --participant-label."
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

