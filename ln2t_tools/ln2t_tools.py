import os
import logging
import shutil
from typing import Optional, List, Dict
from pathlib import Path
import re
from datetime import datetime
from bids import BIDSLayout

from ln2t_tools.cli.cli import parse_args, setup_terminal_colors, configure_logging, log_minimal, MINIMAL
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
    get_freesurfer_output_with_fallback,
    InstanceManager,
    setup_meld_data_structure,
    create_meld_config_json,
    create_meld_dataset_description,
    prepare_meld_input_symlinks,
    download_meld_weights,
    get_dataset_initials
)
from ln2t_tools.utils.demographics import (
    create_meld_demographics_from_participants,
    validate_meld_demographics
)
from ln2t_tools.tools.cvrmap import CvrMapTool
from ln2t_tools.tools.bids_validator import BidsValidatorTool
from ln2t_tools.utils.hpc import (
    submit_hpc_job,
    submit_multiple_jobs,
    validate_hpc_config,
    check_required_data,
    print_download_command,
    check_apptainer_image_exists_on_hpc,
    get_hpc_image_build_command,
    prompt_apptainer_build,
    start_ssh_control_master,
    test_ssh_connection,
)
from ln2t_tools.utils.defaults import (
    DEFAULT_RAWDATA,
    DEFAULT_DERIVATIVES,
    DEFAULT_CODE,
    DEFAULT_SOURCEDATA,
    DEFAULT_FS_VERSION,
    DEFAULT_FASTSURFER_VERSION,
    DEFAULT_FMRIPREP_VERSION,
    DEFAULT_FMRIPREP_FS_VERSION,
    DEFAULT_QSIPREP_VERSION,
    DEFAULT_QSIRECON_VERSION,
    DEFAULT_MELDGRAPH_VERSION,
    DEFAULT_MELD_FS_VERSION,
    DEFAULT_CVRMAP_VERSION,
    DEFAULT_BIDS_VALIDATOR_VERSION,
    DEFAULT_MRI2PRINT_VERSION
)
from ln2t_tools.import_data import import_dicom, import_mrs, pre_import_mrs, import_physio, pre_import_physio, import_meg
from ln2t_tools.import_data.dicom import discover_participants_from_dicom_dir

# Setup initial logging (will be reconfigured based on --verbosity)
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_available_datasets(rawdata_dir: str) -> List[str]:
    """Get list of available BIDS datasets in the rawdata directory."""
    return [name[:-8] for name in os.listdir(rawdata_dir) 
            if name.endswith("-rawdata")]




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


def handle_hpc_status(args):
    """Handle HPC job status queries.
    
    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line arguments with hpc_status attribute
    """
    from ln2t_tools.utils.hpc_status import (
        load_all_jobs, get_jobs_for_dataset, get_jobs_for_tool,
        check_job_status, JobStatus
    )
    
    hpc_status_arg = getattr(args, 'hpc_status', None)
    
    # Determine what jobs to display
    jobs_to_check = []
    
    if hpc_status_arg is None or hpc_status_arg == 'recent':
        # Show recent jobs (last 20)
        all_jobs = load_all_jobs()
        # Sort by submit time, newest first
        sorted_jobs = sorted(
            all_jobs.values(),
            key=lambda j: j.submit_time,
            reverse=True
        )
        jobs_to_check = sorted_jobs[:20]
        
        if not jobs_to_check:
            logger.info("No HPC jobs found in history.")
            return
            
    elif hasattr(args, 'dataset') and getattr(args, 'dataset', None):
        # Filter by dataset
        jobs_to_check = get_jobs_for_dataset(args.dataset)
        if not jobs_to_check:
            logger.info(f"No HPC jobs found for dataset: {args.dataset}")
            return
            
    elif hasattr(args, 'tool') and getattr(args, 'tool', None) and args.tool != 'import':
        # Filter by tool
        jobs_to_check = get_jobs_for_tool(args.tool)
        if not jobs_to_check:
            logger.info(f"No HPC jobs found for tool: {args.tool}")
            return
            
    else:
        # Specific job ID provided
        all_jobs = load_all_jobs()
        if hpc_status_arg in all_jobs:
            jobs_to_check = [all_jobs[hpc_status_arg]]
        else:
            logger.warning(f"Job {hpc_status_arg} not found in local history")
            return
    
    if not jobs_to_check:
        logger.info("No jobs to check.")
        return
    
    # Display results
    logger.info("\n" + "="*70)
    logger.info("HPC Job Status Summary")
    logger.info("="*70 + "\n")
    
    # Organize by status
    pending_jobs = []
    running_jobs = []
    completed_jobs = []
    failed_jobs = []
    
    # Try to connect to HPC if we have credentials to query live status
    username = getattr(args, 'hpc_username', None)
    hostname = getattr(args, 'hpc_hostname', None)
    keyfile = getattr(args, 'hpc_keyfile', '~/.ssh/id_rsa')
    gateway = getattr(args, 'hpc_gateway', None)
    
    can_query = username and hostname
    
    for job_info in jobs_to_check:
        status = None
        details = {'state': job_info.state}
        
        # Try to get live status if we have HPC credentials
        if can_query:
            try:
                status, details = check_job_status(
                    job_info.job_id,
                    username,
                    hostname,
                    keyfile,
                    gateway
                )
            except Exception as e:
                logger.debug(f"Could not query live status for job {job_info.job_id}: {e}")
                status = None
        
        # Use local status if live query failed
        if status is None:
            status = job_info.status_category
        
        if status == JobStatus.PENDING:
            pending_jobs.append((job_info, status, details))
        elif status == JobStatus.RUNNING:
            running_jobs.append((job_info, status, details))
        elif status == JobStatus.COMPLETED:
            completed_jobs.append((job_info, status, details))
        else:
            failed_jobs.append((job_info, status, details))
    
    # Print by category
    if pending_jobs:
        logger.info("⏳ PENDING:")
        for job_info, status, details in pending_jobs:
            logger.info(f"  Job {job_info.job_id}: {job_info.tool} / {job_info.dataset} / sub-{job_info.participant}")
        logger.info("")
    
    if running_jobs:
        logger.info("▶️  RUNNING:")
        for job_info, status, details in running_jobs:
            logger.info(f"  Job {job_info.job_id}: {job_info.tool} / {job_info.dataset} / sub-{job_info.participant}")
        logger.info("")
    
    if completed_jobs:
        logger.info("✅ COMPLETED:")
        for job_info, status, details in completed_jobs:
            logger.info(f"  Job {job_info.job_id}: {job_info.tool} / {job_info.dataset} / sub-{job_info.participant}")
        logger.info("")
    
    if failed_jobs:
        logger.info("❌ FAILED/ERROR:")
        for job_info, status, details in failed_jobs:
            logger.info(f"  Job {job_info.job_id}: {status.value}")
            logger.info(f"    Tool: {job_info.tool}, Dataset: {job_info.dataset}, Sub: sub-{job_info.participant}")
            if details.get('reason'):
                logger.info(f"    Reason: {details['reason']}")
        logger.info("")
    
    # Summary
    logger.info("="*70)
    logger.info(f"Total: {len(jobs_to_check)} jobs")
    logger.info(f"  Pending: {len(pending_jobs)}")
    logger.info(f"  Running: {len(running_jobs)}")
    logger.info(f"  Completed: {len(completed_jobs)}")
    logger.info(f"  Failed: {len(failed_jobs)}")
    logger.info("="*70)
    
    if not can_query and (pending_jobs or running_jobs):
        logger.info("\nℹ️  Tip: For live status updates, provide HPC credentials:")
        logger.info("   ln2t_tools --hpc-status --hpc-username YOUR_USER --hpc-hostname YOUR_HPC")


def handle_import(args):
    """Handle import of source data to BIDS format.
    
    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line arguments
    """
    import subprocess
    
    # Display admin warning
    logger.warning("="*70)
    logger.warning("⚠️  ADMIN ONLY TOOL")
    logger.warning("="*70)
    logger.warning("This import tool requires:")
    logger.warning("  - READ access to sourcedata directory")
    logger.warning("  - WRITE access to rawdata directory")
    logger.warning("")
    logger.warning("Standard users should NOT use this tool.")
    logger.warning("Imported data will be provided by administrators.")
    logger.warning("="*70)
    logger.warning("")
    
    # Validate required arguments
    if not args.dataset:
        logger.error("--dataset is required for import")
        return
    
    # Note: --participant-label is optional for import
    # If not provided, participants will be auto-discovered from the dicom directory
    
    # Setup directories
    dataset = args.dataset
    sourcedata_dir = Path(DEFAULT_SOURCEDATA) / f"{dataset}-sourcedata"
    rawdata_dir = Path(DEFAULT_RAWDATA) / f"{dataset}-rawdata"
    
    # Resolve symlinks
    sourcedata_dir = sourcedata_dir.resolve()
    rawdata_dir = rawdata_dir.resolve()
    
    # Check sourcedata exists
    if not sourcedata_dir.exists():
        logger.error(f"Sourcedata directory not found: {sourcedata_dir}")
        logger.info(f"Expected location: {DEFAULT_SOURCEDATA}/{dataset}-sourcedata")
        return
    
    logger.info(f"Importing data for dataset: {dataset}")
    logger.info(f"Source: {sourcedata_dir}")
    logger.info(f"Target: {rawdata_dir}")
    logger.debug(f"PATHS CONFIG - DEFAULT_SOURCEDATA: {DEFAULT_SOURCEDATA}")
    logger.debug(f"PATHS CONFIG - DEFAULT_RAWDATA: {DEFAULT_RAWDATA}")
    logger.debug(f"PATHS CONFIG - DEFAULT_DERIVATIVES: {DEFAULT_DERIVATIVES}")
    if args.participant_label:
        logger.info(f"Participants: {', '.join(args.participant_label)}")
    else:
        logger.info("Participants: auto-discover from dicom directory")
    if getattr(args, 'session', None):
        logger.info(f"Session: {args.session}")
    
    # Get apptainer directory
    apptainer_dir = Path(getattr(args, 'apptainer_dir', '/opt/apptainer'))
    
    # Get virtual environment path
    venv_path = getattr(args, 'import_env', None)
    if venv_path:
        venv_path = Path(venv_path).resolve()
    
    # Handle --pre-import for MRS or physio data
    if getattr(args, 'pre_import', False):
        datatype_arg = getattr(args, 'datatype', None)
        
        # Require explicit datatype for pre-import
        if datatype_arg not in ['mrs', 'physio']:
            logger.error("--pre-import requires --datatype to be 'mrs' or 'physio'")
            logger.info("Example: ln2t_tools import --dataset DATASET --pre-import --datatype mrs")
            logger.info("Example: ln2t_tools import --dataset DATASET --pre-import --datatype physio")
            return
        
        # Auto-infer ds_initials from dataset name
        ds_initials = get_dataset_initials(dataset)
        if not ds_initials:
            logger.error(f"Could not infer dataset initials from '{dataset}'")
            logger.error("Dataset name should follow pattern: YYYY-Name_Parts-hexhash")
            return
        
        # For pre-import, auto-discover participants from DICOM directory if not specified
        participant_labels = args.participant_label
        if not participant_labels:
            dicom_dir = sourcedata_dir / "dicom"
            if not dicom_dir.exists():
                logger.error(f"DICOM directory not found: {dicom_dir}")
                logger.error("Cannot auto-discover participants without DICOM data")
                return
            
            logger.info("Auto-discovering participants from DICOM directory...")
            participant_labels = discover_participants_from_dicom_dir(dicom_dir, ds_initials)
            
            if not participant_labels:
                logger.error(f"No participants found in {dicom_dir} matching pattern {ds_initials}*")
                return
            
            logger.info(f"Discovered {len(participant_labels)} participants: {participant_labels}")
        
        if datatype_arg == 'mrs':
            logger.info(f"\n{'='*60}")
            logger.info("MRS PRE-IMPORT: Gathering P-files from scanner backup")
            logger.info(f"{'='*60}")
            logger.info(f"Using dataset initials: {ds_initials}")
            
            pre_import_success = pre_import_mrs(
                dataset=dataset,
                participant_labels=participant_labels,
                sourcedata_dir=sourcedata_dir,
                ds_initials=ds_initials,
                session=getattr(args, 'session', None),
                mrraw_dir=getattr(args, 'mrraw_dir', None),
                tmp_dir=getattr(args, 'mrs_tmp_dir', None),
                tolerance_hours=getattr(args, 'pre_import_tolerance_hours', None) or 1.0,
                dry_run=getattr(args, 'dry_run', False)
            )
            
            if pre_import_success:
                logger.info("✓ MRS pre-import completed successfully")
                
                # Print sample command to run the actual import
                logger.info(f"\n{'='*60}")
                logger.info("To run the MRS BIDS conversion, use:")
                logger.info(f"{'='*60}")
                
                cmd_parts = ["ln2t_tools import"]
                cmd_parts.append(f"--dataset {dataset}")
                cmd_parts.append("--datatype mrs")
                if getattr(args, 'session', None):
                    cmd_parts.append(f"--session {args.session}")
                
                logger.info(f"\n  {' '.join(cmd_parts)}\n")
            else:
                logger.error("✗ MRS pre-import failed")
        
        elif datatype_arg == 'physio':
            logger.info(f"\n{'='*60}")
            logger.info("PHYSIO PRE-IMPORT: Gathering physio files from scanner backup")
            logger.info(f"{'='*60}")
            logger.info(f"Using dataset initials: {ds_initials}")
            
            pre_import_success = pre_import_physio(
                dataset=dataset,
                participant_labels=participant_labels,
                sourcedata_dir=sourcedata_dir,
                ds_initials=ds_initials,
                session=getattr(args, 'session', None),
                backup_dir=getattr(args, 'physio_backup_dir', None),
                tolerance_hours=getattr(args, 'pre_import_tolerance_hours', None) or 1.0,
                dry_run=getattr(args, 'dry_run', False),
                physio_config=getattr(args, 'physio_config', None)
            )
            
            if pre_import_success:
                logger.info("✓ Physio pre-import completed successfully")
                
                # Print sample command to run the actual import
                logger.info(f"\n{'='*60}")
                logger.info("To run the physio BIDS conversion, use:")
                logger.info(f"{'='*60}")
                
                cmd_parts = ["ln2t_tools import"]
                cmd_parts.append(f"--dataset {dataset}")
                cmd_parts.append("--datatype physio")
                if getattr(args, 'session', None):
                    cmd_parts.append(f"--session {args.session}")
                
                logger.info(f"\n  {' '.join(cmd_parts)}\n")
            else:
                logger.error("✗ Physio pre-import failed")
        
        # Always exit after pre-import (don't automatically proceed with import)
        return
    
    # Determine which datatypes to import
    datatype_arg = getattr(args, 'datatype', 'all')
    datatypes = [datatype_arg] if datatype_arg != 'all' else ['dicom', 'mrs', 'physio', 'meg']
    
    import_success = {'dicom': False, 'mrs': False, 'physio': False, 'meg': False}
    
    # Check if overwrite is enabled
    overwrite = getattr(args, 'overwrite', False)
    
    for datatype in datatypes:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {datatype.upper()} data")
        logger.info(f"{'='*60}")
        
        if datatype == 'dicom':
            # Check if dicom directory exists
            if not (sourcedata_dir / "dicom").exists():
                logger.info(f"No dicom directory found in {sourcedata_dir}, skipping")
                continue
            
            # Compress source by default, unless --skip-source-compression is set
            compress_source = not getattr(args, 'skip_source_compression', False)
            
            import_success['dicom'] = import_dicom(
                dataset=dataset,
                participant_labels=args.participant_label,
                sourcedata_dir=sourcedata_dir,
                rawdata_dir=rawdata_dir,
                ds_initials=get_dataset_initials(dataset),
                session=getattr(args, 'session', None),
                compress_source=compress_source,
                deface=getattr(args, 'deface', False),
                venv_path=venv_path,
                keep_tmp_files=getattr(args, 'keep_tmp_files', False),
                overwrite=overwrite
            )
        
        elif datatype == 'mrs':
            # Check if mrs or pfiles directory exists
            if not (sourcedata_dir / "mrs").exists() and not (sourcedata_dir / "pfiles").exists():
                logger.info(f"No mrs/pfiles directory found in {sourcedata_dir}, skipping")
                continue
            
            # Compress source by default, unless --skip-source-compression is set
            compress_source = not getattr(args, 'skip_source_compression', False)
            
            import_success['mrs'] = import_mrs(
                dataset=dataset,
                participant_labels=args.participant_label,
                sourcedata_dir=sourcedata_dir,
                rawdata_dir=rawdata_dir,
                ds_initials=get_dataset_initials(dataset),
                session=getattr(args, 'session', None),
                compress_source=compress_source,
                venv_path=venv_path,
                overwrite=overwrite
            )
        
        elif datatype == 'physio':
            # Check if physio directory exists
            if not (sourcedata_dir / "physio").exists():
                logger.info(f"No physio directory found in {sourcedata_dir}, skipping")
                continue
            
            # Compress source by default, unless --skip-source-compression is set
            compress_source = not getattr(args, 'skip_source_compression', False)
            
            import_success['physio'] = import_physio(
                dataset=dataset,
                participant_labels=args.participant_label,
                sourcedata_dir=sourcedata_dir,
                rawdata_dir=rawdata_dir,
                ds_initials=get_dataset_initials(dataset),
                session=getattr(args, 'session', None),
                compress_source=compress_source,
                use_phys2bids=getattr(args, 'phys2bids', False),
                physio_config=getattr(args, 'physio_config', None),
                apptainer_dir=apptainer_dir,
                matching_tolerance_sec=getattr(args, 'matching_tolerance_sec', None),
                overwrite=overwrite
            )
        
        elif datatype == 'meg':
            # Check if meg directory exists
            if not (sourcedata_dir / "meg").exists():
                logger.info(f"No meg directory found in {sourcedata_dir}, skipping")
                continue
            
            # Get derivatives directory
            derivatives_dir = DEFAULT_DERIVATIVES / f"{dataset}-derivatives"
            
            import_success['meg'] = import_meg(
                dataset=dataset,
                participant_labels=args.participant_label,
                sourcedata_dir=sourcedata_dir,
                rawdata_dir=rawdata_dir,
                derivatives_dir=derivatives_dir,
                ds_initials=get_dataset_initials(dataset),
                session=getattr(args, 'session', None),
                overwrite=overwrite
            )
    
    # Final summary
    logger.info(f"\n{'='*60}")
    logger.info("IMPORT SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Dataset: {dataset}")
    if args.participant_label:
        logger.info(f"Participants: {len(args.participant_label)}")
    else:
        logger.info("Participants: auto-discovered")
    
    for dtype, success in import_success.items():
        if dtype in datatypes or datatype_arg == 'all':
            status = "✓ SUCCESS" if success else "✗ FAILED/SKIPPED"
            logger.info(f"  {dtype.upper()}: {status}")
    
    logger.info(f"{'='*60}\n")
    
    # Show tree of imported data (only if participants were specified)
    if args.participant_label:
        logger.info("Validating imported data structure...")
        for participant in args.participant_label:
            participant_id = participant.replace('sub-', '')
            subj_dir = rawdata_dir / f"sub-{participant_id}"
            if subj_dir.exists():
                # Try to run tree command
                try:
                    result = subprocess.run(
                        ['tree', str(subj_dir)],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result.returncode == 0:
                        logger.info(f"\n{result.stdout}")
                    else:
                        # Fallback: list directories
                        logger.info(f"\nStructure for sub-{participant_id}:")
                        for item in sorted(subj_dir.rglob("*")):
                            if item.is_file():
                                logger.info(f"  {item.relative_to(rawdata_dir)}")
                except (FileNotFoundError, subprocess.TimeoutExpired):
                    # tree command not available, just list top-level
                    logger.info(f"\nsub-{participant_id}:")
                    for item in sorted(subj_dir.iterdir()):
                        logger.info(f"  {item.name}")
            else:
                logger.warning(f"Subject directory not created: {subj_dir}")


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
    
    # Resolve symlinks to get actual filesystem paths for Apptainer bindings
    dataset_rawdata = dataset_rawdata.resolve()
    logger.debug(f"Resolved rawdata path: {dataset_rawdata}")

    dataset_derivatives = Path(DEFAULT_DERIVATIVES) / f"{args.dataset}-derivatives"
    # Resolve symlinks for derivatives as well
    dataset_derivatives = dataset_derivatives.resolve()
    logger.debug(f"Resolved derivatives path: {dataset_derivatives}")
    version = (DEFAULT_FS_VERSION if args.tool == 'freesurfer' 
              else DEFAULT_FASTSURFER_VERSION if args.tool == 'fastsurfer'
              else DEFAULT_FMRIPREP_VERSION if args.tool == 'fmriprep'
              else DEFAULT_QSIPREP_VERSION if args.tool == 'qsiprep'
              else DEFAULT_QSIRECON_VERSION if args.tool == 'qsirecon'
              else DEFAULT_MELDGRAPH_VERSION if args.tool == 'meld_graph'
              else DEFAULT_CVRMAP_VERSION if args.tool == 'cvrmap'
              else None)
    output_dir = dataset_derivatives / (args.output_label or 
                                      f"{args.tool}_{args.version or version}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    return dataset_rawdata, dataset_derivatives, output_dir


def launch_and_check(apptainer_cmd: str, tool_name: str, participant_label: str) -> None:
    """Launch Apptainer command and check for errors.
    
    Args:
        apptainer_cmd: Command to execute
        tool_name: Name of the tool for error messages
        participant_label: Subject ID for error messages
        
    Raises:
        RuntimeError: If the command fails
    """
    exit_code = launch_apptainer(apptainer_cmd=apptainer_cmd)
    if exit_code != 0:
        error_msg = f"{tool_name} failed for participant {participant_label} with exit code {exit_code}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


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
    # Convert paths to container paths (relative to /rawdata)
    fs_options = []
    
    if additional_contrasts['t2w']:
        logger.info(f"Found T2w image for {participant_label}")
        # Convert host path to container path
        t2w_host = Path(additional_contrasts['t2w'])
        try:
            t2w_relative = t2w_host.relative_to(dataset_rawdata)
            t2w_container = f"/rawdata/{t2w_relative}"
        except ValueError:
            t2w_container = str(t2w_host)
        fs_options.append(f"-T2 {t2w_container}")
        fs_options.append("-T2pial")  # Use T2 for pial surface
    
    if additional_contrasts['flair']:
        logger.info(f"Found FLAIR image for {participant_label}")
        # Convert host path to container path
        flair_host = Path(additional_contrasts['flair'])
        try:
            flair_relative = flair_host.relative_to(dataset_rawdata)
            flair_container = f"/rawdata/{flair_relative}"
        except ValueError:
            flair_container = str(flair_host)
        fs_options.append(f"-FLAIR {flair_container}")
        fs_options.append("-FLAIRpial")  # Use FLAIR for pial surface
        if additional_contrasts['t2w']:
            logger.info("Both T2w and FLAIR images found, using only FLAIR for pial surface")

    # Verify input files exist before launching
    logger.info("Verifying input files exist on host:")
    t1w_path = Path(t1w)
    logger.info(f"  T1w: {t1w_path}")
    if t1w_path.exists():
        logger.info(f"    ✓ File exists (size: {t1w_path.stat().st_size / (1024*1024):.2f} MB)")
    else:
        logger.error(f"    ✗ File NOT found!")
        raise FileNotFoundError(f"T1w file not found: {t1w_path}")
    
    if additional_contrasts['t2w']:
        t2w_path = Path(additional_contrasts['t2w'])
        logger.info(f"  T2w: {t2w_path}")
        if t2w_path.exists():
            logger.info(f"    ✓ File exists (size: {t2w_path.stat().st_size / (1024*1024):.2f} MB)")
        else:
            logger.warning(f"    ✗ File NOT found!")
    
    if additional_contrasts['flair']:
        flair_path = Path(additional_contrasts['flair'])
        logger.info(f"  FLAIR: {flair_path}")
        if flair_path.exists():
            logger.info(f"    ✓ File exists (size: {flair_path.stat().st_size / (1024*1024):.2f} MB)")
        else:
            logger.warning(f"    ✗ File NOT found!")
    
    logger.info(f"Binding directories:")
    logger.info(f"  Rawdata: {dataset_rawdata} -> /rawdata (read-only)")
    logger.info(f"  Derivatives: {dataset_derivatives} -> /derivatives")
    logger.info(f"  FreeSurfer license: {args.fs_license} -> /usr/local/freesurfer/.license")

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
    launch_and_check(apptainer_cmd, "FreeSurfer", participant_label)

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


def process_fastsurfer_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with FastSurfer.
    
    FastSurfer is a deep learning-based neuroimaging pipeline for fast
    whole-brain segmentation and cortical surface reconstruction.
    """
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

    for t1w in t1w_files:
        entities = layout.parse_file_entities(t1w)
        output_subdir = build_bids_subdir(
            participant_label, 
            entities.get('session'), 
            entities.get('run')
        )
        
        output_label = args.output_label or f"fastsurfer_{args.version or DEFAULT_FASTSURFER_VERSION}"
        output_participant_dir = dataset_derivatives / output_label / output_subdir

        if output_participant_dir.exists():
            logger.info(f"Output exists, skipping: {output_participant_dir}")
            continue

        # Verify input files exist before launching
        logger.info("Verifying input files exist on host:")
        t1w_path = Path(t1w)
        logger.info(f"  T1w: {t1w_path}")
        if t1w_path.exists():
            logger.info(f"    ✓ File exists (size: {t1w_path.stat().st_size / (1024*1024):.2f} MB)")
        else:
            logger.error(f"    ✗ File NOT found!")
            raise FileNotFoundError(f"T1w file not found: {t1w_path}")
        
        # Get optional T2 image for hypothalamus segmentation
        t2w_files = layout.get(
            subject=participant_label,
            session=entities.get('session'),
            scope="raw",
            suffix="T2w",
            extension=".nii.gz",
            return_type="filename"
        )
        t2_path = t2w_files[0] if t2w_files else None
        if t2_path:
            logger.info(f"  T2w: {t2_path}")
            if Path(t2_path).exists():
                logger.info(f"    ✓ File exists")
            else:
                logger.warning(f"    ✗ File NOT found, continuing without T2")
                t2_path = None

        logger.info(f"Binding directories:")
        logger.info(f"  Rawdata: {dataset_rawdata} -> /data (read-only)")
        logger.info(f"  Derivatives: {dataset_derivatives} -> /output")
        logger.info(f"  FreeSurfer license: {args.fs_license} -> /fs_license/license.txt")

        # Build FastSurfer command options
        options = {
            'fs_license': args.fs_license,
            'rawdata': str(dataset_rawdata),
            'derivatives': str(dataset_derivatives),
            'participant_label': participant_label,
            't1w': t1w,
            'apptainer_img': apptainer_img,
            'output_label': output_label,
            'session': entities.get('session'),
            'run': entities.get('run'),
            'seg_only': getattr(args, 'seg_only', False),
            'surf_only': getattr(args, 'surf_only', False),
            'three_tesla': getattr(args, 'three_tesla', False),
            'threads': getattr(args, 'threads', 4),
            'device': getattr(args, 'device', 'auto'),
            'vox_size': getattr(args, 'vox_size', 'min'),
            'no_cereb': getattr(args, 'no_cereb', False),
            'no_hypothal': getattr(args, 'no_hypothal', False),
            'no_biasfield': getattr(args, 'no_biasfield', False),
        }
        
        if t2_path:
            options['t2'] = t2_path

        # Build and launch FastSurfer command
        apptainer_cmd = build_apptainer_cmd(tool="fastsurfer", **options)
        launch_and_check(apptainer_cmd, "FastSurfer", participant_label)


def process_fmriprep_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with fMRIPrep.
    
    Handles multi-session datasets intelligently:
    - First tries to find FreeSurfer output matching the session of the anatomical data
    - If not found, falls back to any available FreeSurfer output for the participant
    - This allows processing sessions that only have functional data (no anatomical scan)
    """
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

    # Check for existing FreeSurfer output with fallback for multi-session datasets
    # This handles the case where session A has anat+func, session B has only func
    entities = layout.parse_file_entities(t1w_files[0])
    fs_output_dir, fallback_warning = get_freesurfer_output_with_fallback(
        derivatives_dir=dataset_derivatives,
        participant_label=participant_label,
        version=DEFAULT_FMRIPREP_FS_VERSION,
        requested_session=entities.get('session'),
        run=entities.get('run')
    )
    
    # Log fallback warning if we're using data from a different session
    if fallback_warning:
        logger.warning(fallback_warning)

    # Build output directory path
    output_subdir = build_bids_subdir(participant_label)
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"fmriprep_{args.version or DEFAULT_FMRIPREP_VERSION}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    # fMRIPrep now requires pre-computed FreeSurfer outputs by default
    # Users can override with --fmriprep-reconall to allow fMRIPrep to run FreeSurfer
    allow_fs_reconall = getattr(args, 'fmriprep_reconall', False)
    
    if fs_output_dir:
        logger.info(f"Using existing FreeSurfer output: {fs_output_dir}")
        fs_subjects_dir = fs_output_dir
    elif allow_fs_reconall:
        logger.info("No existing FreeSurfer output found, but --fmriprep-reconall enabled, will allow fMRIPrep to run reconstruction")
        fs_subjects_dir = None
    else:
        logger.error(
            f"No FreeSurfer output found for participant {participant_label}. "
            f"fMRIPrep now requires pre-computed FreeSurfer outputs by default. "
            f"Either run FreeSurfer first with 'ln2t_tools freesurfer' or use "
            f"'--fmriprep-reconall' to allow fMRIPrep to run FreeSurfer reconstruction."
        )
        return

    # Build and launch fMRIPrep command
    # Tool-specific options (--output-spaces, --nprocs, etc.) are passed via --tool-args
    output_dir = dataset_derivatives / (args.output_label or f"fmriprep_{args.version or DEFAULT_FMRIPREP_VERSION}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="fmriprep",
        fs_license=args.fs_license,
        rawdata=str(dataset_rawdata),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        fs_subjects_dir=fs_subjects_dir,
        allow_fs_reconall=allow_fs_reconall,
        tool_args=getattr(args, 'tool_args', '')
    )
    launch_and_check(apptainer_cmd, "fMRIPrep", participant_label)

def process_mri2print_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with mri2print.
    
    mri2print requires FreeSurfer outputs to already exist.
    It will look for FreeSurfer output in the derivatives directory
    and bind it into the container.
    """
    from ln2t_tools.utils.utils import build_apptainer_cmd
    
    # Check for existing FreeSurfer output (required for mri2print)
    # Parse entities from anatomical files to get session/run info
    anat_files = layout.get(
        subject=participant_label,
        scope="raw",
        suffix="T1w",
        extension=".nii.gz",
        return_type="filename"
    )
    
    if not anat_files:
        logger.warning(f"No anatomical images found for participant {participant_label}")
        return
    
    entities = layout.parse_file_entities(anat_files[0])
    
    # Get FreeSurfer output directory (mri2print just needs the outputs, no version specificity)
    fs_output_dir = get_freesurfer_output(
        derivatives_dir=dataset_derivatives,
        participant_label=participant_label,
        version=DEFAULT_FS_VERSION,
        session=entities.get('session'),
        run=entities.get('run')
    )
    
    if not fs_output_dir:
        logger.error(
            f"FreeSurfer output not found for participant {participant_label}. "
            f"Please run FreeSurfer first before using mri2print."
        )
        return
    
    # Build output directory path
    version = args.version or DEFAULT_MRI2PRINT_VERSION
    output_label = args.output_label or f"mri2print_{version}"
    output_dir = dataset_derivatives / output_label
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_subdir = build_bids_subdir(participant_label)
    output_participant_dir = output_dir / output_subdir
    
    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return
    
    # Build and launch mri2print command with FreeSurfer binding
    apptainer_cmd = build_apptainer_cmd(
        tool="mri2print",
        fs_license=args.fs_license,
        rawdata=str(dataset_rawdata),
        derivatives=str(output_dir),
        participant_label=participant_label,
        output_label=output_label,
        apptainer_img=apptainer_img,
        fs_subjects_dir=str(fs_output_dir),
        tool_args=getattr(args, 'tool_args', '')
    )
    launch_and_check(apptainer_cmd, "mri2print", participant_label)

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
        args: Parsed command line arguments (tool-specific options via --tool-args)
        dataset_rawdata: Path to BIDS rawdata directory
        dataset_derivatives: Path to derivatives directory
        apptainer_img: Path to Apptainer image
    
    Note:
        QSIPrep-specific options (--output-resolution, --denoise-method, etc.)
        should be passed via --tool-args. Example:
            --tool-args "--output-resolution 2.0 --denoise-method dwidenoise"
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

    # Build output directory path
    output_subdir = build_bids_subdir(participant_label)
    output_participant_dir = dataset_derivatives / (
        args.output_label or 
        f"qsiprep_{args.version or DEFAULT_QSIPREP_VERSION}"
    ) / output_subdir

    if output_participant_dir.exists():
        logger.info(f"Output exists, skipping: {output_participant_dir}")
        return

    # Build and launch QSIPrep command
    # Tool-specific options (--output-resolution, etc.) are passed via --tool-args
    output_dir = dataset_derivatives / (args.output_label or f"qsiprep_{args.version or DEFAULT_QSIPREP_VERSION}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="qsiprep",
        fs_license=args.fs_license,
        rawdata=str(dataset_rawdata),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        tool_args=getattr(args, 'tool_args', '')
    )
    launch_and_check(apptainer_cmd, "QSIPrep", participant_label)

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
        args: Parsed command line arguments (tool-specific options via --tool-args)
        dataset_rawdata: Path to BIDS rawdata directory
        dataset_derivatives: Path to derivatives directory
        apptainer_img: Path to Apptainer image
    
    Note:
        QSIRecon-specific options (--recon-spec, --nprocs, --omp-nthreads, etc.)
        should be passed via --tool-args. Example:
            --tool-args "--recon-spec dsi_studio_autotrack --nprocs 8"
    """
    # QSIRecon requires QSIPrep preprocessed data
    # Check for QSIPrep derivatives
    qsiprep_version = getattr(args, 'qsiprep_version', None) or DEFAULT_QSIPREP_VERSION
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
    # Tool-specific options (--recon-spec, --nprocs, etc.) are passed via --tool-args
    output_dir = dataset_derivatives / (args.output_label or f"qsirecon_{args.version or DEFAULT_QSIRECON_VERSION}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    apptainer_cmd = build_apptainer_cmd(
        tool="qsirecon",
        fs_license=args.fs_license,
        qsiprep_dir=str(qsiprep_dir),
        derivatives=str(output_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        tool_args=getattr(args, 'tool_args', '')
    )
    launch_and_check(apptainer_cmd, "QSIRecon", participant_label)

def process_meldgraph_subject(
    layout: BIDSLayout,
    participant_label: str,
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    dataset_code: Path,
    apptainer_img: str
) -> None:
    """Process a single subject with MELD Graph for lesion detection.
    
    MELD Graph has a specific directory structure and workflow:
    1. Setup MELD data structure in ~/code/{dataset}-code/meld_graph_{version}/
    2. Create symlinks to input data in MELD format
    3. Optionally use precomputed FreeSurfer outputs
    4. Run prediction with optional harmonization
    
    Args:
        layout: BIDSLayout object for BIDS dataset
        participant_label: Subject ID without 'sub-' prefix
        args: Parsed command line arguments
        dataset_rawdata: Path to BIDS rawdata directory
        dataset_derivatives: Path to derivatives directory
        dataset_code: Path to dataset code directory
        apptainer_img: Path to Apptainer image
    """
    # Check if HPC submission requested
    if getattr(args, 'hpc', False):
        logger.info(f"Submitting MELD Graph job for {participant_label} to HPC...")
        job_id = submit_hpc_job(
            tool="meld_graph",
            participant_label=participant_label,
            dataset=args.dataset,
            args=args
        )
        
        if job_id:
            logger.info(f"Job submitted successfully! Job ID: {job_id}")
            # Print download command
            print_download_command(
                tool="meld_graph",
                dataset=args.dataset,
                args=args
            )
        else:
            # HPC submission failed - raise error to stop processing
            raise RuntimeError(f"Failed to submit HPC job for participant {participant_label}")
        
        return  # Exit early - job is submitted to HPC
    
    meld_version = args.version or DEFAULT_MELDGRAPH_VERSION
    
    # Setup MELD-specific directory structure
    meld_data_dir, meld_config_dir, meld_output_dir = setup_meld_data_structure(
        dataset_derivatives,
        dataset_code,
        meld_version
    )
    
    # Create configuration files if they don't exist
    create_meld_config_json(meld_config_dir, use_bids=True)
    create_meld_dataset_description(meld_config_dir, args.dataset)
    
    # Check for FreeSurfer outputs if needed
    fs_derivatives_dir = None
    use_skip_feature_extraction = getattr(args, 'skip_feature_extraction', False)
    use_precomputed = getattr(args, 'use_precomputed_fs', False)
    
    # IMPORTANT: Only create input symlinks if NOT using precomputed FreeSurfer
    # When MELD finds input files, it tries to run FreeSurfer
    # When using precomputed FS, we only want MELD to see the FreeSurfer outputs
    if not use_precomputed:
        if not prepare_meld_input_symlinks(
            meld_data_dir / "input",
            layout,
            participant_label
        ):
            logger.error(f"Failed to prepare input data for {participant_label}")
            return
    else:
        logger.info("Skipping input symlink creation (using precomputed FreeSurfer)")
        logger.info("MELD will use existing FreeSurfer outputs for feature extraction")
    
    if use_precomputed:
        fs_version = getattr(args, 'fs_version', None) or DEFAULT_MELD_FS_VERSION
        fs_subject_dir = get_freesurfer_output(
            dataset_derivatives,
            participant_label,
            fs_version
        )
        
        if not fs_subject_dir:
            logger.warning(
                f"No FreeSurfer output found for participant {participant_label}. "
                f"MELD will run FreeSurfer segmentation.\n"
                f"Note: MELD Graph requires FreeSurfer 7.2.0 or earlier (current default: {DEFAULT_MELD_FS_VERSION})."
            )
        else:
            # Get the FreeSurfer derivatives directory (freesurfer_7.2.0/)
            fs_derivatives_dir = dataset_derivatives / f"freesurfer_{fs_version}"
            logger.info(f"Using precomputed FreeSurfer outputs from: {fs_derivatives_dir}")
            logger.info(f"FreeSurfer directory will be bound to /data/output/fs_outputs in container")
            
            # Verify the subject exists in FreeSurfer outputs
            fs_subject_dir = fs_derivatives_dir / f"sub-{participant_label}"
            if not fs_subject_dir.exists():
                logger.error(f"FreeSurfer output not found: {fs_subject_dir}")
                return
            
            # Check for and create completion marker if missing
            scripts_dir = fs_subject_dir / "scripts"
            if scripts_dir.exists():
                done_file = scripts_dir / "recon-all.done"
                if not done_file.exists():
                    logger.warning("recon-all.done marker not found - creating it")
                    try:
                        done_file.touch()
                        logger.info(f"Created completion marker: {done_file}")
                    except Exception as e:
                        logger.warning(f"Could not create completion marker: {e}")
            
            # Check if user explicitly wants to skip feature extraction
            # (only if features from a previous MELD run already exist)
            if not use_skip_feature_extraction:
                logger.info("MELD will run feature extraction to create .sm3.mgh files")
            
            # Keep fs_derivatives_dir to bind into container
            logger.info(f"Will bind FreeSurfer directory into container: {fs_derivatives_dir}")
    
    # Build and launch MELD Graph command
    apptainer_cmd = build_apptainer_cmd(
        tool="meld_graph",
        meld_data_dir=str(meld_data_dir),
        participant_label=participant_label,
        apptainer_img=apptainer_img,
        fs_license=str(args.fs_license),
        fs_subjects_dir=str(fs_derivatives_dir) if fs_derivatives_dir else None,
        harmo_code=getattr(args, 'harmo_code', None),
        demographics=None,  # Always auto-generated from participants.tsv
        skip_feature_extraction=use_skip_feature_extraction,
        harmonize=getattr(args, 'harmonize', False),
        use_gpu=not getattr(args, 'no_gpu', False),  # Enable GPU unless --no-gpu is set
        gpu_memory_limit=getattr(args, 'gpu_memory_limit', 128),  # GPU memory split size
        additional_options=getattr(args, 'additional_options', '')
    )
    
    launch_and_check(apptainer_cmd, "MELD Graph", participant_label)
    
    logger.info(f"MELD Graph processing complete for {participant_label}")
    logger.info(f"Results in: {meld_output_dir / 'predictions_reports' / f'sub-{participant_label}'}")


def process_meld_harmonization(
    layout: BIDSLayout,
    participant_labels: List[str],
    args,
    dataset_rawdata: Path,
    dataset_derivatives: Path,
    dataset_code: Path,
    apptainer_img: str
) -> None:
    """Compute harmonization parameters for MELD Graph.
    
    Harmonization requires:
    - At least 20 subjects from the same scanner
    - Harmonization code (e.g., H1, H2)
    - Demographics data from participants.tsv (age, sex, group)
    
    Demographics file is automatically created from participants.tsv in the BIDS dataset.
    
    Args:
        layout: BIDSLayout object
        participant_labels: List of subject IDs
        args: Parsed arguments
        dataset_rawdata: Path to rawdata
        dataset_derivatives: Path to derivatives
        dataset_code: Path to code directory
        apptainer_img: Path to container image
    """
    if len(participant_labels) < 20:
        logger.warning(
            f"Harmonization recommended with at least 20 subjects. "
            f"You have {len(participant_labels)} subjects."
        )
    
    if not getattr(args, 'harmo_code', None):
        logger.error("--harmo-code is required for harmonization")
        return
    
    meld_version = args.version or DEFAULT_MELDGRAPH_VERSION
    
    # Setup MELD structure
    meld_data_dir, meld_config_dir, meld_output_dir = setup_meld_data_structure(
        dataset_derivatives,
        dataset_code,
        meld_version
    )
    
    # Auto-generate demographics from participants.tsv
    participants_tsv = dataset_rawdata / "participants.tsv"
    
    if not participants_tsv.exists():
        logger.error(
            f"participants.tsv not found: {participants_tsv}\n"
            f"Please ensure your BIDS dataset has a participants.tsv file."
        )
        return
    
    logger.info("Creating demographics file from participants.tsv...")
    
    # Create demographics file in MELD data directory
    auto_demographics_file = meld_data_dir / f"demographics_{args.harmo_code}.csv"
    demographics_file = create_meld_demographics_from_participants(
        participants_tsv=participants_tsv,
        participant_labels=participant_labels,
        harmo_code=args.harmo_code,
        output_path=auto_demographics_file
    )
    
    if demographics_file is None:
        logger.error("Failed to create demographics file from participants.tsv")
        logger.error(
            "Please ensure participants.tsv contains required columns:\n"
            "  - participant_id (required)\n"
            "  - age or Age (required, numeric)\n"
            "  - sex or Sex or gender (required, M/F or male/female)\n"
            "  - group (optional, defaults to 'patient' if missing)"
        )
        return
    
    logger.info(f"Successfully created demographics file: {demographics_file}")
    
    # Validate demographics file
    if not validate_meld_demographics(demographics_file):
        logger.error("Demographics file validation failed")
        return
    
    # Create configuration files
    create_meld_config_json(meld_config_dir, use_bids=True)
    create_meld_dataset_description(meld_config_dir, args.dataset)
    
    # Prepare input for all participants
    for participant_label in participant_labels:
        prepare_meld_input_symlinks(
            meld_data_dir / "input",
            layout,
            participant_label
        )
    
    # Copy demographics file to MELD data directory (if not already there)
    demo_dest = meld_data_dir / demographics_file.name
    if demographics_file != demo_dest:
        shutil.copy(demographics_file, demo_dest)
        logger.info(f"Copied demographics to: {demo_dest}")
    
    # Create subjects list file
    subjects_list = meld_data_dir / "subjects_list.txt"
    with open(subjects_list, 'w') as f:
        for participant in participant_labels:
            f.write(f"sub-{participant}\n")
    logger.info(f"Created subjects list: {subjects_list}")
    
    # Handle precomputed FreeSurfer outputs
    fs_subjects_dir = None
    skip_feature_extraction = getattr(args, 'skip_feature_extraction', False)
    
    if getattr(args, 'use_precomputed_fs', False):
        fs_version = getattr(args, 'fs_version', DEFAULT_MELD_FS_VERSION)
        
        # Check FreeSurfer version compatibility
        if fs_version and float(fs_version.split('.')[0]) > 7 or (
            float(fs_version.split('.')[0]) == 7 and float(fs_version.split('.')[1]) > 2
        ):
            logger.error(
                f"MELD Graph requires FreeSurfer 7.2.0 or earlier. "
                f"Requested version: {fs_version}"
            )
            return
        
        # Get FreeSurfer subjects directory
        freesurfer_output_dir = dataset_derivatives / f"freesurfer_{fs_version}"
        
        if not freesurfer_output_dir.exists():
            logger.error(
                f"FreeSurfer output directory not found: {freesurfer_output_dir}\n"
                f"Cannot use --use-precomputed-fs without existing FreeSurfer outputs."
            )
            return
        
        # Verify all participants have FreeSurfer outputs
        missing_participants = []
        incomplete_participants = []
        for participant_label in participant_labels:
            fs_subject_dir = freesurfer_output_dir / f"sub-{participant_label}"
            if not fs_subject_dir.exists():
                missing_participants.append(participant_label)
            else:
                # Check if FreeSurfer processing completed successfully
                # Look for critical output files that indicate completion
                required_files = [
                    fs_subject_dir / "surf" / "lh.white",
                    fs_subject_dir / "surf" / "rh.white",
                    fs_subject_dir / "surf" / "lh.pial",
                    fs_subject_dir / "surf" / "rh.pial",
                ]
                
                if not all(f.exists() for f in required_files):
                    incomplete_participants.append(participant_label)
                    logger.warning(
                        f"FreeSurfer outputs for sub-{participant_label} appear incomplete. "
                        f"Missing critical surface files."
                    )
        
        if missing_participants:
            logger.error(
                f"FreeSurfer outputs not found for participants: {missing_participants}\n"
                f"Expected location: {freesurfer_output_dir}/sub-<ID>/"
            )
            return
        
        if incomplete_participants:
            logger.error(
                f"FreeSurfer outputs incomplete for participants: {incomplete_participants}\n"
                f"These subjects may not have completed recon-all successfully.\n"
                f"Please re-run FreeSurfer or exclude these subjects."
            )
            return
        
        fs_subjects_dir = str(freesurfer_output_dir)
        logger.info(f"Using precomputed FreeSurfer outputs from: {fs_subjects_dir}")
        logger.info(f"FreeSurfer directory will be bound to /data/output/fs_outputs in container")
        logger.info("MELD will detect existing FreeSurfer outputs and skip recon-all")
        logger.info("MELD will still run feature extraction to create .sm3.mgh files")
        
        # Check for and create completion markers for all participants
        for participant_label in participant_labels:
            fs_subject_dir = freesurfer_output_dir / f"sub-{participant_label}"
            scripts_dir = fs_subject_dir / "scripts"
            
            if scripts_dir.exists():
                done_file = scripts_dir / "recon-all.done"
                if not done_file.exists():
                    logger.warning(f"recon-all.done marker not found for sub-{participant_label} - creating it")
                    try:
                        done_file.touch()
                        logger.info(f"  Created {done_file}")
                    except Exception as e:
                        logger.error(f"  Failed to create completion marker: {e}")
        
        # Keep fs_subjects_dir to bind into container (don't set to None!)
    
    # Build command with --harmo_only flag
    apptainer_cmd = build_apptainer_cmd(
        tool="meld_graph",
        meld_data_dir=str(meld_data_dir),
        participant_label=participant_labels,  # Pass list for subjects_list.txt
        apptainer_img=apptainer_img,
        fs_license=str(args.fs_license),
        fs_subjects_dir=fs_subjects_dir if 'fs_subjects_dir' in locals() else None,
        harmo_code=args.harmo_code,
        demographics=str(demographics_file.name),
        harmonize=True,
        skip_feature_extraction=skip_feature_extraction,  # Only True if user explicitly set it
        use_gpu=not getattr(args, 'no_gpu', False),  # Enable GPU unless --no-gpu is set
        gpu_memory_limit=getattr(args, 'gpu_memory_limit', 128),  # GPU memory split size
        additional_options=getattr(args, 'additional_options', '')
    )
    
    logger.info(f"Computing harmonization parameters for {len(participant_labels)} subjects...")
    launch_and_check(apptainer_cmd, "MELD Harmonization", f"{len(participant_labels)} subjects")
    
    logger.info(f"Harmonization complete. Parameters saved in: {meld_output_dir / 'preprocessed_surf_data'}")


def main(args=None) -> None:
    """Main entry point for ln2t_tools."""
    if args is None:
        args = parse_args()
        setup_terminal_colors()
        
        # Configure logging based on verbosity level
        verbosity = getattr(args, 'verbosity', 'verbose')
        configure_logging(verbosity)

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
        
        # Check for list-missing operation (requires dataset but no processing)
        if getattr(args, 'list_missing', False):
            if not args.dataset:
                logger.error("--dataset is required with --list-missing")
                return
            
            # Import here to avoid circular imports
            from ln2t_tools.utils.utils import get_missing_participants, print_missing_participants_report
            
            tool_name = getattr(args, 'tool', 'unknown')
            tool_version = getattr(args, 'version', None)
            output_label = getattr(args, 'output_label', None)
            
            missing = get_missing_participants(
                dataset=args.dataset,
                tool=tool_name,
                tool_version=tool_version,
                tool_output_label=output_label
            )
            
            print_missing_participants_report(
                dataset=args.dataset,
                tool=tool_name,
                missing_participants=missing
            )
            return

        # Check for HPC status operation
        if getattr(args, 'hpc_status', None) is not None:
            handle_hpc_status(args)
            return

        # Handle import tool separately (doesn't follow the same pattern as processing tools)
        if hasattr(args, 'tool') and args.tool == 'import':
            handle_import(args)
            return

        # Require both --dataset and a tool for processing
        if not args.dataset:
            logger.error("--dataset is required. Please specify a dataset to process.")
            logger.info("Use 'ln2t_tools --list-datasets' to see available datasets.")
            return

        if not hasattr(args, 'tool') or not args.tool:
            logger.error("A tool must be specified (e.g., freesurfer, fmriprep, qsiprep, etc.).")
            return

        datasets_to_process = [args.dataset]

        # Collect all tools and participants for lock information
        all_tools = set()
        all_participants = set()
        
        for dataset in datasets_to_process:
            if hasattr(args, 'tool') and args.tool:
                tools_to_run = {args.tool: getattr(args, 'version', None)}
                all_tools.update(tools_to_run.keys())
            
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
            
            # Determine tool and version from command line arguments
            default_version = DEFAULT_FS_VERSION if args.tool == 'freesurfer' else \
                            DEFAULT_FASTSURFER_VERSION if args.tool == 'fastsurfer' else \
                            DEFAULT_FMRIPREP_VERSION if args.tool == 'fmriprep' else \
                            DEFAULT_QSIPREP_VERSION if args.tool == 'qsiprep' else \
                            DEFAULT_QSIRECON_VERSION if args.tool == 'qsirecon' else \
                            DEFAULT_MELDGRAPH_VERSION if args.tool == 'meld_graph' else \
                            DEFAULT_CVRMAP_VERSION if args.tool == 'cvrmap' else \
                            DEFAULT_BIDS_VALIDATOR_VERSION if args.tool == 'bids_validator' else \
                            DEFAULT_MRI2PRINT_VERSION if args.tool == 'mri2print' else None
            tools_to_run = {args.tool: getattr(args, 'version', None) or default_version}
            
            logger.info(f"Tools to run for {dataset}: {tools_to_run}")
            
            # Temporarily set the dataset for processing
            args.dataset = dataset
            
            try:
                dataset_rawdata, dataset_derivatives, _ = setup_directories(args)
                dataset_code = Path(DEFAULT_CODE) / f"{dataset}-code"
                dataset_code.mkdir(parents=True, exist_ok=True)
                # Resolve symlinks for code directory as well
                dataset_code = dataset_code.resolve()
                logger.debug(f"Resolved code path: {dataset_code}")

                # Handle MELD-specific operations that don't process individual participants
                if args.tool == "meld_graph":
                    # Download weights if requested
                    if getattr(args, 'download_weights', False):
                        meld_version = args.version or DEFAULT_MELDGRAPH_VERSION
                        meld_data_dir, _, _ = setup_meld_data_structure(
                            dataset_derivatives,
                            dataset_code,
                            meld_version
                        )
                        check_apptainer_is_installed()
                        apptainer_dir = Path(args.apptainer_dir)
                        apptainer_img = ensure_image_exists(apptainer_dir, args.tool, meld_version)
                        if download_meld_weights(
                            str(apptainer_img),
                            meld_data_dir,
                            str(args.fs_license)
                        ):
                            logger.info("MELD weights downloaded successfully")
                            successful_datasets.append(dataset)
                            continue
                        else:
                            logger.error("Failed to download MELD weights")
                            failed_datasets.append(dataset)
                            continue
                    
                    # Harmonization workflow
                    if getattr(args, 'harmonize', False):
                        # Get participant list from --participant-label arguments
                        participant_list = args.participant_label if args.participant_label else []
                        
                        layout = BIDSLayout(dataset_rawdata)
                        participant_list = check_participants_exist(layout, participant_list)
                        if not participant_list:
                            logger.error(
                                "No valid participants provided for harmonization. "
                                "Use --participant-label to specify participants, e.g.: "
                                "--participant-label 01 02 03"
                            )
                            failed_datasets.append(dataset)
                            continue
                        
                        # Determine or set harmo code
                        if not getattr(args, 'harmo_code', None):
                            preproc_dir = dataset_derivatives / f"meld_graph_{args.version or DEFAULT_MELDGRAPH_VERSION}" / "data" / "output" / "preprocessed_surf_data"
                            next_idx = 1
                            if preproc_dir.exists():
                                for p in preproc_dir.rglob("MELD_H*_combat_parameters.hdf5"):
                                    m = re.search(r"MELD_H(\d+)_combat_parameters\\.hdf5", p.name)
                                    if m:
                                        next_idx = max(next_idx, int(m.group(1)) + 1)
                            args.harmo_code = f"H{next_idx}"
                            logger.info(f"Auto-assigned harmonization code: {args.harmo_code}")
                        
                        check_apptainer_is_installed()
                        apptainer_dir = Path(args.apptainer_dir)
                        apptainer_img = ensure_image_exists(apptainer_dir, "meld_graph", args.version or DEFAULT_MELDGRAPH_VERSION)
                        
                        # Prepare inputs: config, subjects list, demographics
                        meld_data_dir, meld_config_dir, meld_output_dir = setup_meld_data_structure(
                            dataset_derivatives,
                            dataset_code,
                            args.version or DEFAULT_MELDGRAPH_VERSION
                        )
                        create_meld_config_json(meld_config_dir, use_bids=True)
                        create_meld_dataset_description(meld_config_dir, args.dataset)
                        
                        # Subjects list in MELD data root
                        subjects_list_path = meld_data_dir / "subjects_list.txt"
                        with open(subjects_list_path, 'w') as f:
                            for pid in participant_list:
                                f.write(f"sub-{pid}\n")
                        logger.info(f"Subjects list written: {subjects_list_path}")
                        
                        # Demographics CSV - always auto-generate from participants.tsv
                        participants_tsv = dataset_rawdata / "participants.tsv"
                        if not participants_tsv.exists():
                            logger.error(f"participants.tsv not found: {participants_tsv}")
                            failed_datasets.append(dataset)
                            continue
                        demographics_path = create_meld_demographics_from_participants(
                            participants_tsv=participants_tsv,
                            participant_labels=participant_list,
                            harmo_code=args.harmo_code,
                            output_path=meld_data_dir / f"demographics_{args.harmo_code}.csv"
                        )
                        if demographics_path is None:
                            logger.error("Failed to create demographics CSV for harmonization")
                            failed_datasets.append(dataset)
                            continue
                        
                        # Persist harmonization metadata
                        harmo_dir = dataset_derivatives / f"meld_graph_{args.version or DEFAULT_MELDGRAPH_VERSION}" / "harmonization"
                        harmo_dir.mkdir(parents=True, exist_ok=True)
                        meta_path = harmo_dir / f"harmonization_{args.harmo_code}.tsv"
                        with open(meta_path, 'w') as mf:
                            mf.write("ID\tHarmoCode\tTimestamp\n")
                            ts = datetime.now().isoformat(timespec='seconds')
                            for pid in participant_list:
                                mf.write(f"sub-{pid}\t{args.harmo_code}\t{ts}\n")
                        logger.info(f"Saved harmonization record: {meta_path}")
                        
                        if getattr(args, 'hpc', False):
                            # Submit HPC job using meld_graph in harmonize mode
                            job_id = submit_hpc_job(
                                tool="meld_graph",
                                participant_label=args.harmo_code,  # use code for job name label
                                dataset=dataset,
                                args=args
                            )
                            if job_id:
                                logger.info(f"Submitted harmonization job {job_id} for {len(participant_list)} subjects")
                                # Print download command
                                print_download_command(
                                    tool="meld_graph",
                                    dataset=dataset,
                                    args=args
                                )
                                successful_datasets.append(dataset)
                            else:
                                logger.error("Failed to submit harmonization job to HPC")
                                failed_datasets.append(dataset)
                            continue
                        else:
                            # Run locally via container
                            process_meld_harmonization(
                                layout=layout,
                                participant_labels=participant_list,
                                args=args,
                                dataset_rawdata=dataset_rawdata,
                                dataset_derivatives=dataset_derivatives,
                                dataset_code=dataset_code,
                                apptainer_img=str(apptainer_img)
                            )
                            successful_datasets.append(dataset)
                            continue

                if args.list_missing:
                    # For list missing, use the first tool specified
                    first_tool = list(tools_to_run.keys())[0] if tools_to_run else 'freesurfer'
                    version = tools_to_run.get(first_tool, DEFAULT_FS_VERSION)
                    output_dir = dataset_derivatives / f"{first_tool}_{version}"
                    list_missing_subjects(dataset_rawdata, output_dir)
                    continue

                layout = BIDSLayout(dataset_rawdata)
                
                # Get participants to process (use getattr for tools that don't have participant_label)
                participant_label_arg = getattr(args, 'participant_label', None)
                participant_list = participant_label_arg if participant_label_arg else []
                participant_list = check_participants_exist(layout, participant_list)

                logger.info(f"Processing {len(participant_list)} participants in dataset {dataset}")

                # Track processing results for this dataset
                dataset_success = True

                # Process each tool for this dataset
                for tool, version in tools_to_run.items():
                    if tool not in ["freesurfer", "fastsurfer", "fmriprep", "qsiprep", "qsirecon", "meld_graph", "cvrmap", "bids_validator", "mri2print"]:
                        logger.warning(f"Unsupported tool {tool} for dataset {dataset}, skipping")
                        continue
                    
                    log_minimal(logger, f"Running {tool} version {version} for dataset {dataset}")
                    
                    # Set tool and version in args for this iteration
                    args.tool = tool
                    args.version = version
                    
                    try:
                        # Check tool requirements
                        check_apptainer_is_installed("/usr/bin/apptainer")
                        
                        # Only check FreeSurfer license for tools that require it
                        # CVRmap does not use FreeSurfer
                        tools_requiring_fs_license = ["freesurfer", "fastsurfer", "fmriprep", "qsiprep", "qsirecon", "meld_graph"]
                        if tool in tools_requiring_fs_license:
                            check_file_exists(args.fs_license)

                        # If submitting to HPC, do not build a local image; instead
                        # ensure the required image exists on the HPC apptainer directory.
                        if getattr(args, 'hpc', False):
                            # Validate HPC configuration and set defaults (must be done first)
                            validate_hpc_config(args)
                            
                            username = args.hpc_username
                            hostname = args.hpc_hostname
                            keyfile = args.hpc_keyfile
                            gateway = getattr(args, 'hpc_gateway', None)
                            hpc_apptainer_dir = args.hpc_apptainer_dir  # Default set in validate_hpc_config

                            # Establish SSH ControlMaster for connection reuse (avoids rate limiting)
                            if not test_ssh_connection(username, hostname, keyfile, gateway):
                                logger.error("Cannot connect to HPC. Please check SSH configuration.")
                                dataset_success = False
                                continue

                            image_ok = check_apptainer_image_exists_on_hpc(
                                username=username,
                                hostname=hostname,
                                keyfile=keyfile,
                                gateway=gateway,
                                hpc_apptainer_dir=hpc_apptainer_dir,
                                tool=tool,
                                version=version
                            )

                            if not image_ok:
                                # Prompt user to build the image
                                build_submitted = prompt_apptainer_build(
                                    tool=tool,
                                    version=version,
                                    dataset=dataset,
                                    args=args
                                )
                                
                                # Whether user submitted a build job or saved script locally,
                                # we cannot proceed without the image
                                dataset_success = False
                                continue

                            # For HPC submission, there is no local image to reference
                            apptainer_img = None
                        else:
                            # Local execution: ensure local Apptainer image exists (build if needed)
                            apptainer_img = ensure_image_exists(args.apptainer_dir, tool, version)

                        # Check if HPC submission is requested
                        if getattr(args, 'hpc', False):
                            log_minimal(logger, f"Submitting {tool} jobs to HPC for {len(participant_list)} participants...")
                            
                            # Get HPC connection parameters (already validated earlier)
                            username = args.hpc_username
                            hostname = args.hpc_hostname
                            keyfile = args.hpc_keyfile
                            gateway = getattr(args, 'hpc_gateway', None)
                            hpc_rawdata = getattr(args, 'hpc_rawdata', None) or '$GLOBALSCRATCH/rawdata'
                            hpc_derivatives = getattr(args, 'hpc_derivatives', None) or '$GLOBALSCRATCH/derivatives'
                            
                            # Check required data on HPC for each participant
                            all_data_ready = True
                            for participant_label in participant_list:
                                data_ready = check_required_data(
                                    tool=tool,
                                    dataset=dataset,
                                    participant_label=participant_label,
                                    args=args,
                                    username=username,
                                    hostname=hostname,
                                    keyfile=keyfile,
                                    gateway=gateway,
                                    hpc_rawdata=hpc_rawdata,
                                    hpc_derivatives=hpc_derivatives
                                )
                                if not data_ready:
                                    all_data_ready = False
                                    break
                            
                            if not all_data_ready:
                                logger.error("Required data not available on HPC. Skipping this tool.")
                                dataset_success = False
                                continue
                            
                            # Submit jobs for all participants in parallel
                            job_ids = submit_multiple_jobs(
                                tool=tool,
                                participant_labels=participant_list,
                                dataset=dataset,
                                args=args
                            )
                            
                            if job_ids:
                                logger.info(f"Successfully submitted {len(job_ids)} jobs to HPC")
                                for i, job_id in enumerate(job_ids):
                                    logger.info(f"  Job {i+1}/{len(job_ids)}: {job_id}")
                                
                                # Print download command for retrieving results
                                print_download_command(
                                    tool=tool,
                                    dataset=dataset,
                                    args=args,
                                    job_ids=job_ids
                                )
                                
                                successful_datasets.append(dataset)
                            else:
                                logger.error("Failed to submit jobs to HPC")
                                dataset_success = False
                            
                            # Skip local processing - jobs are on HPC
                            continue

                        # Dataset-wide tools (like bids_validator) run once per dataset, not per participant
                        dataset_wide_tools = ["bids_validator"]
                        
                        if tool in dataset_wide_tools:
                            # Process dataset-wide tool once
                            log_minimal(logger, f"Running {tool} on entire dataset {dataset}")
                            try:
                                if tool == "bids_validator":
                                    BidsValidatorTool.process_subject(
                                        layout=layout,
                                        participant_label=None,  # No specific participant
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                log_minimal(logger, f"✓ Successfully ran {tool} on dataset {dataset}")
                            except Exception as e:
                                logger.error(f"Error running {tool} on dataset {dataset}: {str(e)}")
                                dataset_success = False
                            continue  # Move to next tool

                        # Process each participant with this tool
                        for participant_label in participant_list:
                            log_minimal(logger, f"Processing participant {participant_label} with {tool}")
                            
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
                                elif tool == "fastsurfer":
                                    process_fastsurfer_subject(
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
                                        dataset_code=dataset_code,
                                        apptainer_img=apptainer_img
                                    )
                                elif tool == "cvrmap":
                                    CvrMapTool.process_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                elif tool == "mri2print":
                                    process_mri2print_subject(
                                        layout=layout,
                                        participant_label=participant_label,
                                        args=args,
                                        dataset_rawdata=dataset_rawdata,
                                        dataset_derivatives=dataset_derivatives,
                                        apptainer_img=apptainer_img
                                    )
                                log_minimal(logger, f"✓ Successfully processed participant {participant_label} with {tool}")
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
                    log_minimal(logger, f"✓ Completed processing dataset: {dataset}")
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
                log_minimal(logger, f"✓ Successfully processed dataset: {successful_datasets[0]}")
            else:
                logger.error(f"✗ Failed to process dataset: {failed_datasets[0]}")
        else:
            # Multiple datasets case
            if successful_datasets and not failed_datasets:
                log_minimal(logger, f"✓ Successfully processed all {len(successful_datasets)} datasets: {', '.join(successful_datasets)}")
            elif successful_datasets and failed_datasets:
                logger.warning(f"Processed {len(successful_datasets)}/{len(datasets_to_process)} datasets successfully")
                log_minimal(logger, f"Successful: {', '.join(successful_datasets)}")
                logger.error(f"✗ Failed: {', '.join(failed_datasets)}")
            else:
                logger.error(f"✗ Failed to process all {len(failed_datasets)} datasets: {', '.join(failed_datasets)}")

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
        # Ensure SSH ControlMaster is stopped to avoid idle background processes
        try:
            from ln2t_tools.utils.hpc import stop_ssh_control_master
            stop_ssh_control_master()
        except Exception:
            pass

if __name__ == "__main__":
    main()