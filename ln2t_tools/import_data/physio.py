"""Physiological data to BIDS conversion.

By default uses in-house processing. Can optionally use phys2bids.
"""

import logging
import subprocess
import json
import re
import tarfile
import shutil
import os
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta
import nibabel as nib

logger = logging.getLogger(__name__)

# Default path for scanner physio backup
DEFAULT_PHYSIO_BACKUP_DIR = Path.home() / "PETMR/backup/auto/daily_backups/gating"


def import_physio(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = False,
    use_phys2bids: bool = False,
    physio_config: Optional[Path] = None,
    apptainer_dir: Path = Path("/opt/apptainer")
) -> bool:
    """Import physiological data to BIDS format.
    
    By default uses in-house processing. Set use_phys2bids=True to use phys2bids instead.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    participant_labels : List[str]
        List of participant IDs (without 'sub-' prefix)
    sourcedata_dir : Path
        Path to sourcedata directory
    rawdata_dir : Path
        Path to BIDS rawdata directory
    ds_initials : Optional[str]
        Dataset initials prefix
    session : Optional[str]
        Session label (without 'ses-' prefix)
    compress_source : bool
        Whether to compress source files after successful conversion
    use_phys2bids : bool
        If True, use phys2bids; otherwise use in-house processing (default: False)
    physio_config : Optional[Path]
        Path to physiological data configuration file (for in-house processing)
    apptainer_dir : Path
        Directory containing Apptainer images (for phys2bids only)
        
    Returns
    -------
    bool
        True if import successful, False otherwise
    """
    if use_phys2bids:
        logger.info("Using phys2bids for physiological data import")
        return import_physio_phys2bids(
            dataset=dataset,
            participant_labels=participant_labels,
            sourcedata_dir=sourcedata_dir,
            rawdata_dir=rawdata_dir,
            ds_initials=ds_initials,
            session=session,
            compress_source=compress_source,
            apptainer_dir=apptainer_dir
        )
    else:
        logger.info("Using in-house processing for physiological data import")
        from ln2t_tools.import_data.physio_inhouse import (
            import_physio_inhouse,
            load_physio_config
        )
        
        # Load configuration (auto-detects from sourcedata_dir if not provided)
        config = load_physio_config(physio_config, sourcedata_dir)
        
        return import_physio_inhouse(
            dataset=dataset,
            participant_labels=participant_labels,
            sourcedata_dir=sourcedata_dir,
            rawdata_dir=rawdata_dir,
            config=config,
            ds_initials=ds_initials,
            session=session
        )


def get_phys2bids_container(apptainer_dir: Path = Path("/opt/apptainer")) -> Path:
    """
    Get or build the phys2bids Apptainer container.
    
    Parameters
    ----------
    apptainer_dir : Path
        Directory containing Apptainer images
    
    Returns
    -------
    Path
        Path to the phys2bids container image
    """
    container_name = "phys2bids.phys2bids.latest.sif"
    container_path = apptainer_dir / container_name
    
    if not container_path.exists():
        logger.info(f"phys2bids container not found at {container_path}")
        logger.info("Building phys2bids container from recipe file...")
        apptainer_dir.mkdir(parents=True, exist_ok=True)
        
        # Find the recipe file
        recipe_file = Path(__file__).parent.parent.parent / "apptainer_recipes" / "phys2bids.def"
        
        if not recipe_file.exists():
            logger.error(f"Recipe file not found: {recipe_file}")
            logger.error("Please ensure the phys2bids.def recipe file is in the apptainer_recipes directory")
            raise FileNotFoundError(f"Recipe file not found: {recipe_file}")
        
        logger.info(f"Using recipe file: {recipe_file}")
        
        try:
            subprocess.run(
                ['apptainer', 'build', str(container_path), str(recipe_file)],
                check=True
            )
            logger.info(f"Successfully built phys2bids container at {container_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build phys2bids container: {e}")
            raise
    
    return container_path




def import_physio_phys2bids(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = False,
    apptainer_dir: Path = Path("/opt/apptainer")
) -> bool:
    """Import physiological data to BIDS format using phys2bids.
    
    Automatically matches GE physiological recordings (RESP, PPG) to fMRI runs
    based on timestamps and converts to BIDS format.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    participant_labels : List[str]
        List of participant IDs (without 'sub-' prefix)
    sourcedata_dir : Path
        Path to sourcedata directory
    rawdata_dir : Path
        Path to BIDS rawdata directory
    ds_initials : Optional[str]
        Dataset initials prefix
    session : Optional[str]
        Session label (without 'ses-' prefix)
    compress_source : bool
        Whether to compress source files after successful conversion
    apptainer_dir : Path
        Directory containing Apptainer images
        
    Returns
    -------
    bool
        True if import successful, False otherwise
    """
    # Get or build phys2bids container
    try:
        container_path = get_phys2bids_container(apptainer_dir)
    except Exception as e:
        logger.error(f"Failed to get phys2bids container: {e}")
        return False
    
    # Validate paths
    if not sourcedata_dir.exists():
        logger.error(f"Source data directory not found: {sourcedata_dir}")
        return False
    
    physio_dir = sourcedata_dir / "physio"
    if not physio_dir.exists():
        logger.error(f"Physio directory not found: {physio_dir}")
        return False
    
    logger.info(f"Found physio directory: {physio_dir}")
    
    # If ds_initials not provided, extract from dataset name
    if ds_initials is None:
        parts = dataset.split('-')
        if len(parts) >= 2:
            name_part = parts[1]
            words = name_part.replace('_', ' ').split()
            ds_initials = ''.join([w[0].upper() for w in words if w])
            logger.info(f"Inferred dataset initials: {ds_initials}")
        else:
            logger.warning(f"Could not infer dataset initials from '{dataset}'")
    
    # Process each participant
    success_count = 0
    failed_participants = []
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing physio data for participant {participant_id}")
        logger.info(f"{'='*60}")
        
        # Determine source directory using strict pattern
        if ds_initials:
            if session:
                source_name = f"{ds_initials}{participant_id}SES{session}"
            else:
                source_name = f"{ds_initials}{participant_id}"
            
            physio_source_dir = physio_dir / source_name
            
            if not physio_source_dir.exists():
                logger.error(f"Physio directory not found: {physio_source_dir}")
                logger.error(f"Expected naming: {source_name}")
                failed_participants.append(participant_id)
                continue
        else:
            # Fallback to flexible matching
            pattern = f"*{participant_id}*"
            if session:
                pattern = f"*{participant_id}*{session}*"
            
            matches = list(physio_dir.glob(pattern))
            if not matches:
                logger.error(f"No physio directory found matching {pattern}")
                failed_participants.append(participant_id)
                continue
            elif len(matches) > 1:
                logger.warning(f"Multiple matches: {[m.name for m in matches]}")
                logger.info(f"Using first match: {matches[0].name}")
            physio_source_dir = matches[0]
        
        logger.info(f"Physio source directory: {physio_source_dir}")
        
        # Find participant's fMRI data
        subj_dir = rawdata_dir / f"sub-{participant_id}"
        if session:
            func_dir = subj_dir / f"ses-{session}" / "func"
        else:
            func_dir = subj_dir / "func"
        
        if not func_dir.exists():
            logger.warning(f"fMRI directory not found: {func_dir}")
            logger.warning("Cannot match physio without fMRI data. Skipping.")
            failed_participants.append(participant_id)
            continue
        
        # Parse physio files
        physio_files = parse_physio_files(physio_source_dir)
        if not physio_files:
            logger.error(f"No valid physio files found in {physio_source_dir}")
            failed_participants.append(participant_id)
            continue
        
        logger.info(f"Found {len(physio_files)} physio data files (trigger files excluded):")
        for pf in physio_files:
            logger.info(f"  {pf['filename']}")
            logger.info(f"    Signal: {pf['signal_type']}, End time: {pf['end_time']}")
        
        # Match physio files to fMRI runs
        matches = match_physio_to_fmri(physio_files, func_dir, participant_id, session)
        
        if not matches:
            logger.error("No matches found between physio and fMRI")
            failed_participants.append(participant_id)
            continue
        
        # Create heuristic file
        heur_file = create_heuristic_file(physio_source_dir, matches)
        
        # Run phys2bids for each match
        match_success = 0
        for match in matches:
            success = run_phys2bids(
                physio_file=match['physio_file'],
                physio_dir=physio_source_dir,
                output_dir=rawdata_dir,
                sourcedata_dir=sourcedata_dir,
                dataset=dataset,
                participant_id=participant_id,
                session=session,
                heur_file=heur_file,
                tr=match['tr'],
                ntp=match['ntp'],
                container_path=container_path
            )
            if success:
                match_success += 1
        
        if match_success > 0:
            logger.info(f"✓ Successfully imported {match_success}/{len(matches)} physio files for {participant_id}")
            success_count += 1
            
            # Compress source files if requested
            if compress_source:
                compress_physio_source(physio_dir, physio_source_dir.name)
        else:
            logger.error(f"✗ Failed to import physio data for {participant_id}")
            failed_participants.append(participant_id)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Physio Import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    return success_count > 0


def parse_physio_files(physio_dir: Path) -> List[Dict]:
    """Parse GE physio filenames to extract metadata.
    
    GE physio files have format: {SIGNAL}{TYPE}_{SEQUENCE}_{TIMESTAMP}
    - SIGNAL: RESP (respiratory) or PPG (photoplethysmography)
    - TYPE: Data or Trig (trigger)
    - TIMESTAMP: End time of recording in format MMDDYYYYHH_MM_SS_MS
    
    Examples:
    - RESPData_epiRTphysio_1124202515_54_58_279 → Nov 24, 2025, 15:54:58
    - PPGData_epiRTphysio_1124202516_28_57_165 → Nov 24, 2025, 16:28:57
    - PPGTrig_epiRTphysio_1124202515_32_10_133 (ignored - trigger file)
    
    Parameters
    ----------
    physio_dir : Path
        Directory containing physio files
        
    Returns
    -------
    List[Dict]
        List of parsed physio file information
    """
    physio_files = []
    
    # Pattern: RESPData_epiRTphysio_1124202515_54_58_279
    # Format: {SIGNAL}{TYPE}_{SEQUENCE}_{MMDDYYYYHH_MM_SS_MS}
    pattern = re.compile(r'^(RESP|PPG)(Data|Trig)_([^_]+)_(\d{10})_(\d+)_(\d+)_(\d+)$')
    
    for file in physio_dir.iterdir():
        if not file.is_file():
            continue
        
        match = pattern.match(file.name)
        if not match:
            logger.debug(f"Skipping non-matching file: {file.name}")
            continue
        
        signal_type, data_type, sequence, datetime_str, minute, second, ms = match.groups()
        
        # Ignore trigger files
        if data_type == "Trig":
            logger.debug(f"Skipping trigger file: {file.name}")
            continue
        
        # Parse timestamp (end of recording)
        # Format: MMDDYYYYHH_MM_SS_MS where datetime_str = MMDDYYYYHH
        try:
            month = int(datetime_str[:2])
            day = int(datetime_str[2:4])
            year = int(datetime_str[4:8])
            hour = int(datetime_str[8:10])
            
            end_time = datetime(year, month, day, hour, int(minute), int(second))
            
            physio_files.append({
                'filename': file.name,
                'filepath': file,
                'signal_type': signal_type,  # RESP or PPG
                'data_type': data_type,      # Data or Trig
                'sequence': sequence,
                'end_time': end_time,
                'milliseconds': int(ms)      # Store for reference
            })
            
            logger.debug(f"Parsed {file.name}: {signal_type} ending at {end_time}")
            
        except ValueError as e:
            logger.warning(f"Could not parse timestamp for {file.name}: {e}")
            continue
    
    return physio_files


def match_physio_to_fmri(
    physio_files: List[Dict],
    func_dir: Path,
    participant_id: str,
    session: Optional[str] = None,
    tolerance_sec: float = 35.0
) -> List[Dict]:
    """Match physio files to fMRI runs based on timestamps.
    
    GE physio recordings start 30 seconds before fMRI acquisition,
    so we use a 35-second tolerance to account for this offset plus
    small timing variations.
    
    Parameters
    ----------
    physio_files : List[Dict]
        Parsed physio file information
    func_dir : Path
        Directory containing fMRI functional data
    participant_id : str
        Participant ID
    session : Optional[str]
        Session label
    tolerance_sec : float
        Time tolerance in seconds for matching (default: 35.0)
        Accounts for 30s pre-recording + 5s timing variation
        
    Returns
    -------
    List[Dict]
        List of matched physio-fMRI pairs
    """
    logger.info(f"\n{'='*60}")
    logger.info("Matching physio files to fMRI runs")
    logger.info(f"{'='*60}")
    
    # Find all fMRI files
    fmri_files = list(func_dir.glob("*.nii.gz"))
    if not fmri_files:
        logger.error(f"No fMRI files found in {func_dir}")
        return []
    
    matches = []
    
    for fmri_file in fmri_files:
        # Find corresponding JSON
        json_file = fmri_file.parent / f"{fmri_file.name.replace('.nii.gz', '')}.json"
        if not json_file.exists():
            logger.warning(f"No JSON sidecar for {fmri_file.name}, skipping")
            continue
        
        # Read fMRI metadata
        with open(json_file, 'r') as f:
            fmri_meta = json.load(f)
        
        acq_time_str = fmri_meta.get('AcquisitionTime')
        tr = fmri_meta.get('RepetitionTime')
        
        if not acq_time_str or not tr:
            logger.warning(f"Missing AcquisitionTime or TR in {json_file.name}, skipping")
            continue
        
        # Parse acquisition start time
        try:
            # AcquisitionTime format: "HH:MM:SS.ssssss"
            acq_time = datetime.strptime(acq_time_str.split('.')[0], "%H:%M:%S")
            # Note: we don't have the date from AcquisitionTime, will infer from physio
        except ValueError as e:
            logger.warning(f"Could not parse AcquisitionTime '{acq_time_str}': {e}")
            continue
        
        # Get number of timepoints from NIfTI
        try:
            nii = nib.load(fmri_file)
            ntp = nii.shape[3] if len(nii.shape) > 3 else 1
        except Exception as e:
            logger.warning(f"Could not read NIfTI {fmri_file.name}: {e}")
            continue
        
        # Compute fMRI end time
        fmri_duration = timedelta(seconds=tr * ntp)
        
        logger.info(f"\nfMRI file: {fmri_file.name}")
        logger.info(f"  Start time: {acq_time_str}")
        logger.info(f"  TR: {tr}s, Volumes: {ntp}")
        logger.info(f"  Duration: {fmri_duration.total_seconds():.1f}s")
        
        # Try to match with physio files (only Data files, ignore Trig)
        data_physio = [pf for pf in physio_files if pf['data_type'] == 'Data']
        
        best_match = None
        best_diff = float('inf')
        
        for pf in data_physio:
            # Combine date from physio with time from fMRI
            fmri_start = pf['end_time'].replace(
                hour=acq_time.hour,
                minute=acq_time.minute,
                second=acq_time.second
            )
            
            # If time appears to be from previous day (e.g., late night scan)
            if acq_time.hour < 12 and pf['end_time'].hour > 12:
                fmri_start += timedelta(days=1)
            
            fmri_end = fmri_start + fmri_duration
            
            # Compute time difference
            time_diff = abs((pf['end_time'] - fmri_end).total_seconds())
            
            logger.debug(f"  {pf['filename']}")
            logger.debug(f"    Physio end: {pf['end_time']}")
            logger.debug(f"    fMRI end:   {fmri_end}")
            logger.debug(f"    Difference: {time_diff:.1f}s")
            
            if time_diff < best_diff:
                best_diff = time_diff
                best_match = pf
        
        if best_match and best_diff <= tolerance_sec:
            logger.info(f"  ✓ MATCHED to {best_match['filename']}")
            logger.info(f"    Signal: {best_match['signal_type']}")
            logger.info(f"    Time difference: {best_diff:.1f}s (within tolerance)")
            
            # Extract task name from fMRI filename
            # Pattern: sub-XX_task-YYY_bold.nii.gz
            task_match = re.search(r'task-([^_]+)', fmri_file.name)
            task = task_match.group(1) if task_match else 'unknown'
            
            matches.append({
                'physio_file': best_match['filepath'],
                'physio_filename': best_match['filename'],
                'signal_type': best_match['signal_type'],
                'fmri_file': fmri_file,
                'task': task,
                'tr': tr,
                'ntp': ntp,
                'time_diff': best_diff
            })
        else:
            if best_match:
                logger.warning(f"  ✗ Closest match ({best_match['filename']}) exceeds tolerance")
                logger.warning(f"    Time difference: {best_diff:.1f}s > {tolerance_sec}s")
            else:
                logger.warning(f"  ✗ No matching physio file found")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Matching complete: {len(matches)} matches found")
    logger.info(f"{'='*60}\n")
    
    return matches


def create_heuristic_file(physio_dir: Path, matches: List[Dict]) -> Path:
    """Create phys2bids heuristic file.
    
    Parameters
    ----------
    physio_dir : Path
        Directory where heuristic file will be created
    matches : List[Dict]
        Matched physio-fMRI pairs
        
    Returns
    -------
    Path
        Path to created heuristic file
    """
    heur_file = physio_dir / "heur_ge_physio.py"
    
    # Get unique tasks
    tasks = {m['task'] for m in matches}
    
    heur_content = '''import fnmatch

def heur(physinfo, take=""):
    """
    Heuristic for GE physio files.
    Auto-generated by ln2t_tools.
    
    Parameters:
    -----------
    physinfo : str
        The filename of a physiological file to process.
    take : str
        Optional parameter for additional filtering.
    
    Returns:
    --------
    dict or None
        A dictionary containing BIDS keys.
        Returns None if the file should be ignored.
    """
    info = {}
    
'''
    
    # Add pattern matching for each signal type and task
    for task in sorted(tasks):
        task_matches = [m for m in matches if m['task'] == task]
        
        # Check which signal types are present for this task
        signals = {m['signal_type'] for m in task_matches}
        
        for signal in sorted(signals):
            signal_lower = signal.lower()
            heur_content += f'''    # {signal} signal for task {task}
    if fnmatch.fnmatchcase(physinfo, "*{signal}Data*"):
        info["task"] = "{task}"
    
'''
    
    heur_content += '''    # Ignore trigger files
    if "Trig" in physinfo:
        return None
    
    # Return None if no pattern matched
    if not info:
        return None
    
    return info
'''
    
    with open(heur_file, 'w') as f:
        f.write(heur_content)
    
    logger.info(f"Created heuristic file: {heur_file}")
    
    return heur_file


def run_phys2bids(
    physio_file: Path,
    physio_dir: Path,
    output_dir: Path,
    sourcedata_dir: Path,
    dataset: str,
    participant_id: str,
    session: Optional[str],
    heur_file: Path,
    tr: float,
    ntp: int,
    container_path: Path
) -> bool:
    """Run phys2bids for a single physio file using Apptainer container.
    
    Parameters
    ----------
    physio_file : Path
        Path to physio file
    physio_dir : Path
        Directory containing physio files
    output_dir : Path
        Output BIDS directory
    sourcedata_dir : Path
        Source data directory (for code/logs)
    dataset : str
        Dataset name
    participant_id : str
        Participant ID
    session : Optional[str]
        Session label
    heur_file : Path
        Path to heuristic file
    tr : float
        Repetition time
    ntp : int
        Number of timepoints
    container_path : Path
        Path to phys2bids Apptainer container
        
    Returns
    -------
    bool
        True if successful
    """
    logger.info(f"\nRunning phys2bids for {physio_file.name}")
    
    # Create temporary code directory in sourcedata
    code_dir = sourcedata_dir / "phys2bids_logs"
    code_dir.mkdir(parents=True, exist_ok=True)
    
    # Build apptainer command
    cmd = [
        'apptainer', 'exec',
        '-B', f'{physio_dir}:/data/input',
        '-B', f'{output_dir}:/data/output',
        '-B', f'{code_dir}:/data/output/code',  # Redirect code folder to sourcedata
        '-B', f'{heur_file.parent}:/data/heur',
        str(container_path),
        'phys2bids',
        '-in', f'/data/input/{physio_file.name}',
        '-indir', '/data/input',
        '-outdir', '/data/output',
        '-sub', participant_id,
    ]
    
    if session:
        cmd.extend(['-ses', session])
    
    cmd.extend([
        '-heur', f'/data/heur/{heur_file.name}',
        '-tr', str(tr),
        '-ntp', str(ntp)
    ])
    
    logger.debug(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"✓ phys2bids completed successfully")
        logger.info(f"  Logs saved to: {code_dir}")
        if result.stdout:
            logger.debug(f"Output: {result.stdout}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ phys2bids failed: {e.stderr}")
        return False


def compress_physio_source(physio_dir: Path, source_name: str) -> None:
    """Compress physio source directory.
    
    Parameters
    ----------
    physio_dir : Path
        Parent physio directory
    source_name : str
        Name of source directory to compress
    """
    source_path = physio_dir / source_name
    compressed_file = physio_dir / f"{source_name}.tar.gz"
    
    if compressed_file.exists():
        logger.debug(f"Compressed file already exists: {compressed_file.name}")
        return
    
    logger.info(f"Compressing {source_name}...")
    try:
        with tarfile.open(compressed_file, "w:gz") as tar:
            tar.add(source_path, arcname=source_name)
        logger.info(f"✓ Created {compressed_file.name}")
    except Exception as e:
        logger.error(f"Failed to compress {source_name}: {e}")


# =============================================================================
# Pre-import functions: Gather physio files from scanner backup locations
# =============================================================================

def parse_physio_filename(filename: str) -> Optional[Dict]:
    """Parse a physio filename to extract metadata including datetime.
    
    GE physio files have format: {SIGNAL}{TYPE}_{SEQUENCE}_{TIMESTAMP}
    - SIGNAL: RESP (respiratory) or PPG (photoplethysmography)
    - TYPE: Data or Trig (trigger)
    - TIMESTAMP: End time of recording in format MMDDYYYYHH_MM_SS_MS
    
    Examples:
    - RESPTrig_epi2_0621202109_35_43_785 → June 21, 2021, 09:35:43
    - PPGTrig_epiRTphysio_1204202516_10_48_787 → Dec 4, 2025, 16:10:48
    
    Parameters
    ----------
    filename : str
        Physio filename to parse
        
    Returns
    -------
    Optional[Dict]
        Dictionary with parsed info, or None if parsing failed
    """
    # Pattern: RESPData_epiRTphysio_1124202515_54_58_279
    # Format: {SIGNAL}{TYPE}_{SEQUENCE}_{MMDDYYYYHH_MM_SS_MS}
    pattern = re.compile(r'^(RESP|PPG)(Data|Trig)_([^_]+)_(\d{10})_(\d+)_(\d+)_(\d+)$')
    
    match = pattern.match(filename)
    if not match:
        return None
    
    signal_type, data_type, sequence, datetime_str, minute, second, ms = match.groups()
    
    # Parse timestamp (end of recording)
    # Format: MMDDYYYYHH_MM_SS_MS where datetime_str = MMDDYYYYHH
    try:
        month = int(datetime_str[:2])
        day = int(datetime_str[2:4])
        year = int(datetime_str[4:8])
        hour = int(datetime_str[8:10])
        
        end_time = datetime(year, month, day, hour, int(minute), int(second))
        
        return {
            'signal_type': signal_type,  # RESP or PPG
            'data_type': data_type,      # Data or Trig
            'sequence': sequence,
            'end_time': end_time,
            'milliseconds': int(ms)
        }
        
    except ValueError as e:
        logger.warning(f"Could not parse timestamp for {filename}: {e}")
        return None


def find_physio_files_by_datetime(
    backup_dir: Path,
    exam_datetime: datetime,
    tolerance_hours: float = 1.0
) -> List[Tuple[Path, Dict]]:
    """Find physio files in backup directory matching exam datetime.
    
    Scans the backup directory for physio files and returns those whose
    timestamp matches the exam datetime within the specified tolerance.
    
    Parameters
    ----------
    backup_dir : Path
        Path to the physio backup directory
    exam_datetime : datetime
        The exam datetime from DICOM metadata
    tolerance_hours : float
        Time tolerance in hours for matching files
        
    Returns
    -------
    List[Tuple[Path, Dict]]
        List of (file_path, parsed_info) tuples for matching files
    """
    if not backup_dir.exists():
        logger.warning(f"Physio backup directory not found: {backup_dir}")
        return []
    
    matching_files = []
    tolerance = timedelta(hours=tolerance_hours)
    
    for item in backup_dir.iterdir():
        if not item.is_file():
            continue
        
        # Parse the filename
        parsed = parse_physio_filename(item.name)
        if parsed is None:
            continue
        
        # Check if the physio end_time is within tolerance of exam_datetime
        # Note: exam_datetime is the start of the scan, physio end_time is when recording ended
        # We allow a window: exam_datetime - tolerance to exam_datetime + tolerance + ~30 min for scan duration
        # Simplified: just check if they're on the same day with similar times
        time_diff = abs((parsed['end_time'] - exam_datetime).total_seconds())
        time_diff_hours = time_diff / 3600.0
        
        if time_diff_hours <= tolerance_hours:
            matching_files.append((item, parsed))
            logger.debug(f"Found matching physio file: {item.name} (diff: {time_diff_hours:.2f}h)")
    
    return matching_files


def pre_import_physio(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    ds_initials: str,
    session: Optional[str] = None,
    backup_dir: Optional[Path] = None,
    tolerance_hours: float = 1.0,
    dry_run: bool = False
) -> bool:
    """Pre-import physio data: gather physio files from scanner backup location.
    
    This function:
    1. For each participant, finds their DICOM data to extract exam metadata
    2. Uses the exam datetime to find matching physio files in the backup directory
    3. Copies all found physio files to {sourcedata_dir}/physio/{ds_initials}{participant}
    
    Parameters
    ----------
    dataset : str
        Dataset name
    participant_labels : List[str]
        List of participant IDs (without 'sub-' prefix)
    sourcedata_dir : Path
        Path to sourcedata directory
    ds_initials : str
        Dataset initials prefix (e.g., 'CB', 'HP')
    session : Optional[str]
        Session label (without 'ses-' prefix)
    backup_dir : Optional[Path]
        Path to physio backup directory. Defaults to DEFAULT_PHYSIO_BACKUP_DIR
    tolerance_hours : float
        Time tolerance in hours for matching physio files by datetime
    dry_run : bool
        If True, only report what would be done without copying files
        
    Returns
    -------
    bool
        True if pre-import successful for at least one participant
    """
    # Import DICOM metadata functions from mrs module (avoid duplication)
    from ln2t_tools.import_data.mrs import get_dicom_metadata, find_dicom_for_participant
    
    # Set default path
    if backup_dir is None:
        backup_dir = DEFAULT_PHYSIO_BACKUP_DIR
    
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Physio Pre-import")
    logger.info(f"  Dataset: {dataset}")
    logger.info(f"  Participants: {participant_labels}")
    logger.info(f"  Backup dir: {backup_dir}")
    logger.info(f"  Tolerance: {tolerance_hours} hours")
    
    # Check backup directory exists
    if not backup_dir.exists():
        logger.error(f"Physio backup directory not found: {backup_dir}")
        logger.error("Please verify the path or use --physio-backup-dir to specify a different location")
        return False
    
    # Check DICOM source directory exists
    dicom_dir = sourcedata_dir / "dicom"
    if not dicom_dir.exists():
        logger.error(f"DICOM directory not found: {dicom_dir}")
        logger.error("Cannot extract exam metadata without DICOM files")
        return False
    
    # Create physio directory in sourcedata
    physio_output_dir = sourcedata_dir / "physio"
    if not dry_run:
        physio_output_dir.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    failed_participants = []
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing participant: {participant_id}")
        logger.info(f"{'='*60}")
        
        # Step 1: Find DICOM file and extract metadata
        dicom_file = find_dicom_for_participant(
            dicom_dir, participant_id, ds_initials, session
        )
        
        if dicom_file is None:
            logger.error(f"Could not find DICOM for {participant_id}")
            failed_participants.append(participant_id)
            continue
        
        metadata = get_dicom_metadata(dicom_file)
        
        logger.info(f"  DICOM metadata:")
        logger.info(f"    Exam Date: {metadata['exam_date']}")
        logger.info(f"    Exam Time: {metadata['exam_time']}")
        logger.info(f"    Patient ID: {metadata['patient_id']}")
        
        if metadata['exam_datetime'] is None:
            logger.error(f"Could not extract exam datetime for {participant_id}")
            failed_participants.append(participant_id)
            continue
        
        logger.info(f"    Exam DateTime: {metadata['exam_datetime']}")
        
        # Step 2: Find matching physio files from backup
        matching_files = find_physio_files_by_datetime(
            backup_dir, metadata['exam_datetime'], tolerance_hours
        )
        
        if not matching_files:
            logger.warning(f"No physio files found for {participant_id}")
            failed_participants.append(participant_id)
            continue
        
        logger.info(f"  Found {len(matching_files)} physio file(s):")
        for filepath, parsed in matching_files:
            time_diff = abs((parsed['end_time'] - metadata['exam_datetime']).total_seconds()) / 60
            logger.info(f"    - {filepath.name} ({parsed['signal_type']}{parsed['data_type']}, {time_diff:.1f} min diff)")
        
        # Step 3: Copy physio files to sourcedata/physio
        if session:
            output_dir = physio_output_dir / f"{ds_initials}{participant_id}SES{session}"
        else:
            output_dir = physio_output_dir / f"{ds_initials}{participant_id}"
        
        if dry_run:
            logger.info(f"  [DRY RUN] Would copy {len(matching_files)} file(s) to {output_dir}")
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            
            copied_count = 0
            for filepath, parsed in matching_files:
                dest = output_dir / filepath.name
                if dest.exists():
                    logger.info(f"  File already exists, skipping: {filepath.name}")
                    copied_count += 1
                    continue
                
                try:
                    shutil.copy2(filepath, dest)
                    logger.info(f"  ✓ Copied: {filepath.name}")
                    copied_count += 1
                except Exception as e:
                    logger.error(f"  ✗ Failed to copy {filepath.name}: {e}")
            
            if copied_count > 0:
                logger.info(f"  ✓ Copied {copied_count} file(s) for {participant_id}")
                success_count += 1
            else:
                logger.error(f"  ✗ Failed to copy any files for {participant_id}")
                failed_participants.append(participant_id)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Physio Pre-import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    return success_count > 0

