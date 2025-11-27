"""Physiological data to BIDS conversion using phys2bids."""

import logging
import subprocess
import json
import re
import tarfile
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta
import nibabel as nib

logger = logging.getLogger(__name__)


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


def import_physio(
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
    - TIMESTAMP: End time of recording in format DDMMYYYYHH_MM_SS_MS
    
    Examples:
    - RESPData_epiRTphysio_1124202515_54_58_279
    - PPGData_epiRTphysio_1124202516_28_57_165
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
    # Format: {SIGNAL}{TYPE}_{SEQUENCE}_{DDMMYYYYHH_MM_SS_MS}
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
        # Format: DDMMYYYYHH_MM_SS_MS where datetime_str = DDMMYYYYHH
        try:
            day = int(datetime_str[:2])
            month = int(datetime_str[2:4])
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
    tolerance_sec: float = 5.0
) -> List[Dict]:
    """Match physio files to fMRI runs based on timestamps.
    
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
        Time tolerance in seconds for matching (default: 5.0)
        
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
    
    # Build apptainer command
    cmd = [
        'apptainer', 'exec',
        '-B', f'{physio_dir}:/data/input',
        '-B', f'{output_dir}:/data/output',
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
