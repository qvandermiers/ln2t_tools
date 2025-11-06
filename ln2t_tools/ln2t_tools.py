import os
import logging
import pandas as pd
from typing import Optional, List, Dict
from pathlib import Path
from bids import BIDSLayout

from ln2t_tools.cli.cli import parse_args, setup_terminal_colors
from ln2t_tools.utils.utils import (
    list_available_datasets,
    list_missing_subjects,
    check_apptainer_is_installed,
    ensure_image_exists,
    check_file_exists,
    check_participants_exist,
    get_flair_list,
    launch_apptainer,
    build_apptainer_cmd,
    get_freesurfer_output,
    InstanceManager
)
from ln2t_tools.utils.defaults import (
    DEFAULT_RAWDATA,
    DEFAULT_DERIVATIVES,
    DEFAULT_FS_VERSION,
    DEFAULT_FMRIPREP_VERSION,
    DEFAULT_QSIPREP_VERSION,
    DEFAULT_QSIRECON_VERSION,
    DEFAULT_MELDGRAPH_VERSION
)

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_available_datasets(rawdata_dir: str) -> List[str]:
    """Get list of available BIDS datasets in the rawdata directory."""
    return [name[:-8] for name in os.listdir(rawdata_dir) 
            if name.endswith("-rawdata")]

def read_processing_config(config_path: Path) -> pd.DataFrame:
    """Read processing configuration from TSV file.
    
    Args:
        config_path: Path to configuration TSV file
        
    Returns:
        DataFrame with dataset processing configuration
        
    Expected format:
        dataset    freesurfer    fmriprep
        dataset1   7.3.2         23.1.3
        dataset2                 25.1.4
        dataset3   7.4.0         
    """
    if not config_path.exists():
        logger.warning(f"Config file not found: {config_path}")
        return pd.DataFrame()
    
    try:
        config_df = pd.read_csv(config_path, sep='\t')
        if 'dataset' not in config_df.columns:
            raise ValueError("Config file must have a 'dataset' column")
        
        # Fill NaN values with empty strings
        config_df = config_df.fillna('')
        logger.info(f"Loaded config from {config_path}")
        return config_df
    except Exception as e:
        logger.error(f"Error reading config file {config_path}: {e}")
        return pd.DataFrame()

def get_datasets_to_process(config_df: pd.DataFrame, dataset_filter: Optional[str] = None) -> List[str]:
    """Get list of datasets to process based on config and filter.
    
    Args:
        config_df: Configuration DataFrame
        dataset_filter: Optional dataset name to filter by
        
    Returns:
        List of dataset names to process
    """
    if config_df.empty:
        # Fallback to all available datasets if no config
        available = get_available_datasets(DEFAULT_RAWDATA)
        if dataset_filter:
            return [dataset_filter] if dataset_filter in available else []
        return available
    
    datasets = config_df['dataset'].tolist()
    
    if dataset_filter:
        return [dataset_filter] if dataset_filter in datasets else []
    
    return datasets

def get_tools_for_dataset(config_df: pd.DataFrame, dataset: str) -> Dict[str, str]:
    """Get tools and versions to run for a specific dataset.
    
    Args:
        config_df: Configuration DataFrame
        dataset: Dataset name
        
    Returns:
        Dictionary mapping tool names to versions
    """
    if config_df.empty:
        # Fallback behavior - return default tools
        return {}
    
    dataset_row = config_df[config_df['dataset'] == dataset]
    if dataset_row.empty:
        logger.warning(f"Dataset {dataset} not found in config")
        return {}
    
    tools = {}
    row = dataset_row.iloc[0]
    
    # Check each column for tool specifications
    for col in config_df.columns:
        if col != 'dataset' and row[col] and str(row[col]).strip():
            tools[col] = str(row[col]).strip()
    
    return tools

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

def setup_directories(args) -> tuple[Path, Path, Path]:
    """Setup and validate directory structure for processing.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Tuple of (rawdata_dir, derivatives_dir, output_dir)
        
    Raises:
        FileNotFoundError: If dataset directory doesn't exist
    """
    dataset_rawdata = Path(DEFAULT_RAWDATA) / f"{args.dataset}-rawdata"
    if not dataset_rawdata.exists():
        available = get_available_datasets(DEFAULT_RAWDATA)
        datasets_str = "\n  - ".join(available) if available else "No datasets found"
        raise FileNotFoundError(
            f"Dataset '{args.dataset}' not found in {DEFAULT_RAWDATA}\n"
            f"Available datasets:\n  - {datasets_str}"
        )

    dataset_derivatives = Path(DEFAULT_DERIVATIVES) / f"{args.dataset}-derivatives"
    version = (DEFAULT_FS_VERSION if args.tool == 'freesurfer' 
              else DEFAULT_FMRIPREP_VERSION if args.tool == 'fmriprep'
              else DEFAULT_QSIPREP_VERSION if args.tool == 'qsiprep'
              else DEFAULT_QSIRECON_VERSION if args.tool == 'qsirecon'
              else DEFAULT_MELDGRAPH_VERSION if args.tool == 'meld_graph'
              else None)
    output_dir = dataset_derivatives / (args.output_label or 
                                      f"{args.tool}_{args.version or version}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    return dataset_rawdata, dataset_derivatives, output_dir

def process_freesurfer_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with FreeSurfer."""
    t1w_files = layout.get(
        subject=participant_label,
        scope="raw",
        suffix="T1w",
        extension=".nii.gz",
        return_type="filename"
    )
    
    if not t1w_files:
        logger.warning(f"No T1w images found for participant {participant_label}")
        return

    _ = get_flair_list(layout, participant_label)

    for t1w in t1w_files:
        process_single_t1w(
            t1w=t1w,
            layout=layout,
            participant_label=participant_label,
            args=args,
            dataset_rawdata=dataset_rawdata,
            dataset_derivatives=dataset_derivatives,
            apptainer_img=apptainer_img
        )

def process_single_t1w(
    t1w: str,
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single T1w image with FreeSurfer."""
    entities = layout.parse_file_entities(t1w)
    output_subdir = build_bids_subdir(
        participant_label, 
        entities.get('session'), 
        entities.get('run')
    )
    
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"freesurfer_{args.version or DEFAULT_FS_VERSION}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    # Get additional contrasts
    additional_contrasts = get_additional_contrasts(
        layout,
        participant_label,
        entities.get('session'),
        entities.get('run')
    )

    # Build FreeSurfer command options for additional contrasts
    fs_options = []
    if additional_contrasts['t2w']:
        logger.info(f"Found T2w image for {participant_label}")
        fs_options.append(f"-T2 {additional_contrasts['t2w']}")
        fs_options.append("-T2pial")  # Use T2 for pial surface
    
    if additional_contrasts['flair']:
        logger.info(f"Found FLAIR image for {participant_label}")
        fs_options.append(f"-FLAIR {additional_contrasts['flair']}")
        fs_options.append("-FLAIRpial")  # Use FLAIR for pial surface
        if additional_contrasts['t2w']:
            logger.info("Both T2w and FLAIR images found, using only FLAIR for pial surface")

    # Build and launch FreeSurfer command
    apptainer_cmd = build_apptainer_cmd(
        tool="freesurfer",
        fs_license=args.fs_license,
        rawdata=str(dataset_rawdata),
        derivatives=str(dataset_derivatives),
        participant_label=participant_label,
        t1w=t1w,
        apptainer_img=apptainer_img,
        output_label=args.output_label or f"freesurfer_{args.version or DEFAULT_FS_VERSION}",
        session=entities.get('session'),
        run=entities.get('run'),
        additional_options=" ".join(fs_options)
    )
    launch_apptainer(apptainer_cmd=apptainer_cmd)

def build_bids_subdir(
    participant_label: str,
    session: Optional[str] = None,
    run: Optional[str] = None
) -> str:
    """Build BIDS-compliant subject directory name."""
    parts = [f"sub-{participant_label}"]
    if session:
        parts.append(f"ses-{session}")
    if run:
        parts.append(f"run-{run}")
    return "_".join(parts)

def process_fmriprep_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with fMRIPrep."""
    # Check for required files
    t1w_files = layout.get(
        subject=participant_label,
        scope="raw",
        suffix="T1w",
        extension=".nii.gz",
        return_type="filename"
    )
    
    if not t1w_files:
        logger.warning(f"No T1w images found for participant {participant_label}")
        return

    # Check for functional data
    func_files = layout.get(
        subject=participant_label,
        scope="raw",
        suffix="bold",
        extension=".nii.gz",
        return_type="filename"
    )
    
    if not func_files:
        logger.warning(f"No functional data found for participant {participant_label}")
        return

    # Check for existing FreeSurfer output
    entities = layout.parse_file_entities(t1w_files[0])
    fs_output_dir = get_freesurfer_output(
        derivatives_dir=dataset_derivatives,
        participant_label=participant_label,
        version=DEFAULT_FS_VERSION,
        session=entities.get('session'),
        run=entities.get('run')
    )

    # Build output directory path
    output_subdir = build_bids_subdir(participant_label)
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"fmriprep_{args.version or DEFAULT_FMRIPREP_VERSION}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    # If FreeSurfer output exists, use it
    if fs_output_dir and not args.fs_no_reconall:
        logger.info(f"Using existing FreeSurfer output: {fs_output_dir}")
        fs_no_reconall = "--fs-no-reconall"
    else:
        logger.info("No existing FreeSurfer output found, will run reconstruction")
        fs_no_reconall = ""
        fs_output_dir = None

    # Build and launch fMRIPrep command
    output_dir = dataset_derivatives / (args.output_label or f"fmriprep_{args.version or DEFAULT_FMRIPREP_VERSION}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="fmriprep",
        fs_license=args.fs_license,
        rawdata=str(dataset_rawdata),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        fs_no_reconall=fs_no_reconall,
        output_spaces=getattr(args, 'output_spaces', "MNI152NLin2009cAsym:res-2"),
        nprocs=getattr(args, 'nprocs', 8),
        omp_nthreads=getattr(args, 'omp_nthreads', 8),
        fs_subjects_dir=fs_output_dir
    )
    launch_apptainer(apptainer_cmd=apptainer_cmd)

def process_qsiprep_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with QSIPrep.
    
    Args:
        layout: BIDSLayout object for BIDS dataset
        participant_label: Subject ID without 'sub-' prefix
        args: Parsed command line arguments
        dataset_rawdata: Path to BIDS rawdata directory
        dataset_derivatives: Path to derivatives directory
        apptainer_img: Path to Apptainer image
    """
    # Check for required DWI data
    dwi_files = layout.get(
        subject=participant_label,
        scope="raw",
        suffix="dwi",
        extension=".nii.gz",
        return_type="filename"
    )
    
    if not dwi_files:
        logger.warning(f"No DWI data found for participant {participant_label}")
        return

    # Check for anatomical data unless dwi-only is specified
    if not getattr(args, 'dwi_only', False):
        t1w_files = layout.get(
            subject=participant_label,
            scope="raw",
            suffix="T1w",
            extension=".nii.gz",
            return_type="filename"
        )
        
        if not t1w_files:
            logger.warning(f"No T1w images found for participant {participant_label}")
            if not getattr(args, 'anat_only', False):
                logger.info("Consider using --dwi-only flag for DWI-only processing")
                return

    # Build output directory path
    output_subdir = build_bids_subdir(participant_label)
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"qsiprep_{args.version or DEFAULT_QSIPREP_VERSION}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    # Validate required arguments for QSIPrep
    if not getattr(args, 'output_resolution', None):
        logger.error("--output-resolution is required for QSIPrep")
        return

    # Build and launch QSIPrep command
    output_dir = dataset_derivatives / (args.output_label or f"qsiprep_{args.version or DEFAULT_QSIPREP_VERSION}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="qsiprep",
        fs_license=args.fs_license,
        rawdata=str(dataset_rawdata),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        output_resolution=getattr(args, 'output_resolution'),
        denoise_method=getattr(args, 'denoise_method', 'dwidenoise'),
        dwi_only="--dwi-only" if getattr(args, 'dwi_only', False) else "",
        anat_only="--anat-only" if getattr(args, 'anat_only', False) else "",
        nprocs=getattr(args, 'nprocs', 8),
        omp_nthreads=getattr(args, 'omp_nthreads', 8)
    )
    launch_apptainer(apptainer_cmd=apptainer_cmd)

def process_qsirecon_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with QSIRecon for DWI reconstruction.
    
    Args:
        layout: BIDSLayout object for BIDS dataset
        participant_label: Subject ID without 'sub-' prefix
        args: Parsed command line arguments
        dataset_rawdata: Path to BIDS rawdata directory
        dataset_derivatives: Path to derivatives directory
        apptainer_img: Path to Apptainer image
    """
    # QSIRecon requires QSIPrep preprocessed data
    # Check for QSIPrep derivatives
    qsiprep_version = getattr(args, 'qsiprep_version', DEFAULT_QSIPREP_VERSION)
    qsiprep_dir = dataset_derivatives / f"qsiprep_{qsiprep_version}"
    
    if not qsiprep_dir.exists():
        logger.error(
            f"QSIPrep output not found at: {qsiprep_dir}\n"
            f"QSIRecon requires QSIPrep preprocessed data as input.\n"
            f"Please run QSIPrep first, or specify the correct QSIPrep version with --qsiprep-version.\n"
            f"Expected QSIPrep output directory: {qsiprep_dir}"
        )
        return
    
    # Check if participant exists in QSIPrep output
    participant_qsiprep_dir = qsiprep_dir / f"sub-{participant_label}"
    if not participant_qsiprep_dir.exists():
        logger.error(
            f"Participant {participant_label} not found in QSIPrep output at: {qsiprep_dir}\n"
            f"Please run QSIPrep for this participant first."
        )
        return

    # Build output directory path
    output_subdir = build_bids_subdir(participant_label)
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"qsirecon_{args.version or DEFAULT_QSIRECON_VERSION}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    logger.info(f"Using QSIPrep data from: {qsiprep_dir}")

    # Build and launch QSIRecon command
    output_dir = dataset_derivatives / (args.output_label or f"qsirecon_{args.version or DEFAULT_QSIRECON_VERSION}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="qsirecon",
        fs_license=args.fs_license,
        qsiprep_dir=str(qsiprep_dir),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        recon_spec=getattr(args, 'recon_spec', 'mrtrix_multishell_msmt_ACT-hsvs'),
        nprocs=getattr(args, 'nprocs', 8),
        omp_nthreads=getattr(args, 'omp_nthreads', 8),
        additional_options=getattr(args, 'additional_options', '')
    )
    launch_apptainer(apptainer_cmd=apptainer_cmd)

def process_meldgraph_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with MELD Graph for lesion detection.
    
    Args:
        layout: BIDSLayout object for BIDS dataset
        participant_label: Subject ID without 'sub-' prefix
        args: Parsed command line arguments
        dataset_rawdata: Path to BIDS rawdata directory
        dataset_derivatives: Path to derivatives directory
        apptainer_img: Path to Apptainer image
    """
    # MELD Graph requires FreeSurfer output
    # Check for FreeSurfer derivatives
    fs_version = getattr(args, 'fs_version', DEFAULT_FS_VERSION)
    fs_subjects_dir = get_freesurfer_output(
        dataset_derivatives,
        participant_label,
        fs_version
    )
    
    if not fs_subjects_dir:
        logger.warning(
            f"No FreeSurfer output found for participant {participant_label}. "
            f"MELD Graph requires FreeSurfer recon-all to be completed first."
        )
        return

    # Build output directory path
    output_subdir = build_bids_subdir(participant_label)
    meld_version = args.version or DEFAULT_MELDGRAPH_VERSION
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"meld_graph_{meld_version}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    # Build and launch MELD Graph command
    output_dir = dataset_derivatives / (args.output_label or f"meld_graph_{meld_version}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="meld_graph",
        rawdata=str(dataset_rawdata),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        fs_subjects_dir=fs_subjects_dir.parent,  # FreeSurfer subjects directory
        additional_options=getattr(args, 'additional_options', '')
    )
    launch_apptainer(apptainer_cmd=apptainer_cmd)

def main(args=None) -> None:
    """Main entry point for ln2t_tools."""
    if args is None:
        args = parse_args()
        setup_terminal_colors()

    try:
        # Initialize instance manager
        instance_manager = InstanceManager(max_instances=getattr(args, 'max_instances', 10))
        
        # Check for list operations that don't require instance lock
        if args.list_datasets:
            list_available_datasets()
            return
        
        if getattr(args, 'list_instances', False):
            instance_manager = InstanceManager()
            instance_manager.list_active_instances()
            return

        # Read processing configuration
        config_path = Path(DEFAULT_RAWDATA) / "processing_config.tsv"
        config_df = read_processing_config(config_path)
        
        # Get datasets to process based on config and arguments
        datasets_to_process = get_datasets_to_process(config_df, args.dataset)
        
        if not datasets_to_process:
            if args.dataset:
                logger.error(f"Dataset '{args.dataset}' not found in config or rawdata directory")
            else:
                logger.error("No datasets found to process")
            return

        # Collect all tools and participants for lock information
        all_tools = set()
        all_participants = set()
        
        for dataset in datasets_to_process:
            tools_to_run = get_tools_for_dataset(config_df, dataset)
            if not tools_to_run and hasattr(args, 'tool') and args.tool:
                tools_to_run = {args.tool: getattr(args, 'version', None)}
            all_tools.update(tools_to_run.keys() if tools_to_run else [])
            
            # Get participants for this dataset
            try:
                dataset_rawdata = Path(DEFAULT_RAWDATA) / f"{dataset}-rawdata"
                if dataset_rawdata.exists():
                    layout = BIDSLayout(dataset_rawdata)
                    participant_list = args.participant_label if args.participant_label else []
                    participant_list = check_participants_exist(layout, participant_list)
                    all_participants.update([f"sub-{p}" for p in participant_list])
            except:
                pass  # Skip if we can't determine participants yet

        # Try to acquire instance lock before processing with collected information
        dataset_str = ", ".join(datasets_to_process) if len(datasets_to_process) > 1 else datasets_to_process[0]
        tool_str = ", ".join(all_tools) if len(all_tools) > 1 else (list(all_tools)[0] if all_tools else "unknown")
        
        if not instance_manager.acquire_instance_lock(
            dataset=dataset_str,
            tool=tool_str,
            participants=list(all_participants)
        ):
            active_count = instance_manager.get_active_instances()
            logger.error(
                f"Cannot start new instance. "
                f"Maximum instances ({instance_manager.max_instances}) reached. "
                f"Currently running: {active_count} instances.\n"
                f"Please wait for other instances to complete or increase --max-instances."
            )
            return

        logger.info(f"Instance lock acquired. Active instances: {instance_manager.get_active_instances()}")
        logger.info(f"Processing datasets: {', '.join(datasets_to_process)}")

        # Track processing results
        successful_datasets = []
        failed_datasets = []

        # Process each dataset
        for dataset in datasets_to_process:
            logger.info(f"Processing dataset: {dataset}")
            
            # Get tools to run for this dataset from config
            tools_to_run = get_tools_for_dataset(config_df, dataset)
            
            if not tools_to_run:
                # Fallback to command line tool if no config
                if hasattr(args, 'tool') and args.tool:
                    default_version = DEFAULT_FS_VERSION if args.tool == 'freesurfer' else \
                                    DEFAULT_FMRIPREP_VERSION if args.tool == 'fmriprep' else \
                                    DEFAULT_QSIPREP_VERSION if args.tool == 'qsiprep' else \
                                    DEFAULT_QSIRECON_VERSION if args.tool == 'qsirecon' else \
                                    DEFAULT_MELDGRAPH_VERSION if args.tool == 'meld_graph' else None
                    tools_to_run = {args.tool: getattr(args, 'version', None) or default_version}
                else:
                    logger.warning(f"No tools specified for dataset {dataset}, skipping")
                    continue
            
            logger.info(f"Tools to run for {dataset}: {tools_to_run}")
            
            # Temporarily set the dataset for processing
            args.dataset = dataset
            
            try:
                dataset_rawdata, dataset_derivatives, _ = setup_directories(args)

                if args.list_missing:
                    # For list missing, use the first tool specified
                    first_tool = list(tools_to_run.keys())[0] if tools_to_run else 'freesurfer'
                    version = tools_to_run.get(first_tool, DEFAULT_FS_VERSION)
                    output_dir = dataset_derivatives / f"{first_tool}_{version}"
                    list_missing_subjects(dataset_rawdata, output_dir)
                    continue

                layout = BIDSLayout(dataset_rawdata)
                
                # Get participants to process
                participant_list = args.participant_label if args.participant_label else []
                participant_list = check_participants_exist(layout, participant_list)

                logger.info(f"Processing {len(participant_list)} participants in dataset {dataset}")

                # Track processing results for this dataset
                dataset_success = True

                # Process each tool for this dataset
                for tool, version in tools_to_run.items():
                    if tool not in ["freesurfer", "fmriprep", "qsiprep", "qsirecon", "meld_graph"]:
                        logger.warning(f"Unsupported tool {tool} for dataset {dataset}, skipping")
                        continue
                    
                    logger.info(f"Running {tool} version {version} for dataset {dataset}")
                    
                    # Set tool and version in args for this iteration
                    args.tool = tool
                    args.version = version
                    
                    try:
                        # Check tool requirements
                        check_apptainer_is_installed("/usr/bin/apptainer")
                        apptainer_img = ensure_image_exists(args.apptainer_dir, tool, version)
                        check_file_exists(args.fs_license)

                        # Process each participant with this tool
                        for participant_label in participant_list:
                            logger.info(f"Processing participant {participant_label} with {tool}")
                            
                            try:
                                if tool == "freesurfer":
                                    process_freesurfer_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                elif tool == "fmriprep":
                                    process_fmriprep_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                elif tool == "qsiprep":
                                    process_qsiprep_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                elif tool == "qsirecon":
                                    process_qsirecon_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                elif tool == "meld_graph":
                                    process_meldgraph_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                logger.info(f"Successfully processed participant {participant_label} with {tool}")
                            except Exception as e:
                                logger.error(f"Error processing participant {participant_label} with {tool}: {str(e)}")
                                dataset_success = False
                                # Continue with next participant
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error setting up {tool} for dataset {dataset}: {str(e)}")
                        dataset_success = False
                        # Continue with next tool
                        continue

                if dataset_success:
                    logger.info(f"Completed processing dataset: {dataset}")
                    successful_datasets.append(dataset)
                else:
                    logger.warning(f"Completed processing dataset: {dataset} (with some errors)")
                    failed_datasets.append(dataset)
                
            except Exception as e:
                logger.error(f"Error processing dataset {dataset}: {str(e)}")
                failed_datasets.append(dataset)
                # Continue with next dataset instead of failing completely
                continue

        # Report final results
        if len(datasets_to_process) == 1:
            # Single dataset case
            if successful_datasets:
                logger.info(f"Successfully processed dataset: {successful_datasets[0]}")
            else:
                logger.error(f"Failed to process dataset: {failed_datasets[0]}")
        else:
            # Multiple datasets case
            if successful_datasets and not failed_datasets:
                logger.info(f"Successfully processed all {len(successful_datasets)} datasets: {', '.join(successful_datasets)}")
            elif successful_datasets and failed_datasets:
                logger.warning(f"Processed {len(successful_datasets)}/{len(datasets_to_process)} datasets successfully")
                logger.info(f"Successful: {', '.join(successful_datasets)}")
                logger.error(f"Failed: {', '.join(failed_datasets)}")
            else:
                logger.error(f"Failed to process all {len(failed_datasets)} datasets: {', '.join(failed_datasets)}")

        # Exit with appropriate code
        if failed_datasets:
            exit(1)

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    finally:
        # Ensure instance lock is released
        try:
            if 'instance_manager' in locals():
                instance_manager.release_instance_lock()
        except:
            pass

if __name__ == "__main__":
    main()