"""In-house physiological data processing for BIDS conversion."""

import logging
import json
import re
import gzip
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

# BIDS recording type mapping
SIGNAL_TYPE_MAPPING = {
    'RESP': 'respiratory',
    'PPG': 'cardiac'
}

# Sampling frequencies (Hz)
SAMPLING_FREQUENCIES = {
    'RESP': 25.0,   # Respiratory at 25Hz
    'PPG': 100.0    # PPG (cardiac) at 100Hz
}


def load_physio_config(config_path: Optional[Path] = None, sourcedata_dir: Optional[Path] = None) -> Dict:
    """Load physiological data configuration.
    
    Searches for config file in this order:
    1. Explicit config_path if provided
    2. sourcedata_dir/configs/physio.json
    3. sourcedata_dir/physio/config.json
    4. Default values (DummyVolumes=5)
    
    Parameters
    ----------
    config_path : Optional[Path]
        Path to configuration JSON file (overrides auto-detection)
    sourcedata_dir : Optional[Path]
        Path to sourcedata directory (for auto-detecting config file)
        
    Returns
    -------
    Dict
        Configuration dictionary with at least 'DummyVolumes' field
    """
    # Try explicit path first
    if config_path is not None:
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        logger.info(f"Using physio config: {config_path}")
    
    # Try auto-detection in sourcedata
    elif sourcedata_dir is not None:
        # Try preferred location: configs/physio.json
        config_path = sourcedata_dir / "configs" / "physio.json"
        if not config_path.exists():
            # Try legacy location: physio/config.json
            config_path = sourcedata_dir / "physio" / "config.json"
        
        if config_path.exists():
            logger.info(f"Using physio config: {config_path}")
        else:
            logger.warning(
                f"No physio config file found. Searched:\n"
                f"  {sourcedata_dir}/configs/physio.json\n"
                f"  {sourcedata_dir}/physio/config.json\n"
                f"Using default DummyVolumes=5"
            )
            return {'DummyVolumes': 5}
    
    # No path and no sourcedata_dir
    else:
        logger.warning("No physio config file provided, using default DummyVolumes=5")
        return {'DummyVolumes': 5}
    
    # Load the config file
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        if 'DummyVolumes' not in config:
            logger.warning("'DummyVolumes' not found in config, using default value 5")
            config['DummyVolumes'] = 5
        
        logger.info(f"Loaded config: DummyVolumes = {config['DummyVolumes']}")
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON config file: {e}")
        raise


def parse_physio_files(physio_dir: Path) -> List[Dict]:
    """Parse GE physio filenames to extract metadata.
    
    GE physio files have format: {SIGNAL}{TYPE}_{SEQUENCE}_{TIMESTAMP}
    - SIGNAL: RESP (respiratory) or PPG (photoplethysmography)
    - TYPE: Data or Trig (trigger)
    - TIMESTAMP: End time of recording in format MMDDYYYYHH_MM_SS_MS
    
    Only Data files are returned (Trig files are ignored).
    
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
                'data_type': data_type,      # Data
                'sequence': sequence,
                'end_time': end_time,
                'milliseconds': int(ms)
            })
            
            logger.debug(f"Parsed {file.name}: {signal_type} ending at {end_time}")
            
        except ValueError as e:
            logger.warning(f"Could not parse timestamp for {file.name}: {e}")
            continue
    
    return physio_files


def match_physio_to_fmri(
    physio_files: List[Dict],
    func_dir: Path,
    tolerance_sec: float = 35.0
) -> List[Dict]:
    """Match physio files to fMRI runs based on timestamps.
    
    GE physio recordings start 30 seconds before fMRI acquisition,
    so we use a tolerance to account for this offset plus timing variations.
    
    Parameters
    ----------
    physio_files : List[Dict]
        Parsed physio file information
    func_dir : Path
        Directory containing fMRI functional data
    tolerance_sec : float
        Time tolerance in seconds for matching (default: 35.0)
        
    Returns
    -------
    List[Dict]
        List of matched physio-fMRI pairs
    """
    from datetime import timedelta
    import nibabel as nib
    
    logger.info(f"\n{'='*60}")
    logger.info("Matching physio files to fMRI runs")
    logger.info(f"{'='*60}")
    
    # Find all fMRI files
    fmri_files = list(func_dir.glob("*_bold.nii.gz"))
    if not fmri_files:
        logger.error(f"No fMRI files found in {func_dir}")
        return []
    
    matches = []
    
    for fmri_file in fmri_files:
        # Find corresponding JSON
        json_file = fmri_file.parent / fmri_file.name.replace('_bold.nii.gz', '_bold.json')
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
            acq_time = datetime.strptime(acq_time_str.split('.')[0], "%H:%M:%S")
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
        
        # Compute fMRI duration
        fmri_duration = timedelta(seconds=tr * ntp)
        
        logger.info(f"\nfMRI file: {fmri_file.name}")
        logger.info(f"  Start time: {acq_time_str}")
        logger.info(f"  TR: {tr}s, Volumes: {ntp}")
        logger.info(f"  Duration: {fmri_duration.total_seconds():.1f}s")
        
        # Find all physio files that match within tolerance
        matched_physio = []
        
        for pf in physio_files:
            # Combine date from physio with time from fMRI
            fmri_start = pf['end_time'].replace(
                hour=acq_time.hour,
                minute=acq_time.minute,
                second=acq_time.second
            )
            
            # Handle day boundary
            if acq_time.hour < 12 and pf['end_time'].hour > 12:
                fmri_start += timedelta(days=1)
            
            fmri_end = fmri_start + fmri_duration
            
            # Compute time difference
            time_diff = abs((pf['end_time'] - fmri_end).total_seconds())
            
            # Collect all physio files within tolerance
            if time_diff <= tolerance_sec:
                matched_physio.append({
                    'physio_data': pf,
                    'time_diff': time_diff
                })
        
        if matched_physio:
            # Extract task and run from fMRI filename
            task_match = re.search(r'task-([^_]+)', fmri_file.name)
            run_match = re.search(r'run-(\d+)', fmri_file.name)
            
            task = task_match.group(1) if task_match else 'unknown'
            run = run_match.group(1) if run_match else None  # None if no run entity
            
            logger.info(f"  ✓ MATCHED {len(matched_physio)} physio file(s):")
            
            # Add each matched physio file
            for mp in matched_physio:
                pf = mp['physio_data']
                time_diff = mp['time_diff']
                
                logger.info(f"    - {pf['filename']} ({pf['signal_type']}, Δt={time_diff:.1f}s)")
                
                matches.append({
                    'physio_file': pf['filepath'],
                    'physio_filename': pf['filename'],
                    'signal_type': pf['signal_type'],
                    'fmri_file': fmri_file,
                    'task': task,
                    'run': run,  # Can be None
                    'tr': tr,
                    'ntp': ntp,
                    'time_diff': time_diff
                    })
        else:
            logger.warning(f"  ✗ No physio files found within tolerance ({tolerance_sec}s)")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Matching complete: {len(matches)} matches found")
    logger.info(f"{'='*60}\n")
    
    return matches


def process_physio_file(
    physio_file: Path,
    signal_type: str,
    tr: float,
    dummy_volumes: int,
    output_path: Path
) -> bool:
    """Process a single physiological data file to BIDS format.
    
    Parameters
    ----------
    physio_file : Path
        Path to input physio data file (single column of values)
    signal_type : str
        Signal type ('RESP' or 'PPG')
    tr : float
        Repetition time in seconds
    dummy_volumes : int
        Number of dummy volumes at scan start
    output_path : Path
        Output path (without extension) for BIDS files
        
    Returns
    -------
    bool
        True if successful
    """
    try:
        # Load data (single column of values)
        logger.info(f"Processing {physio_file.name}")
        data = np.loadtxt(physio_file)
        
        if data.ndim != 1:
            logger.error(f"Expected 1D data, got shape {data.shape}")
            return False
        
        logger.info(f"  Loaded {len(data)} samples")
        
        # Get sampling frequency
        sampling_freq = SAMPLING_FREQUENCIES.get(signal_type)
        if sampling_freq is None:
            logger.error(f"Unknown signal type: {signal_type}")
            return False
        
        # Calculate StartTime
        # StartTime = -(30s (GE pre-recording) + TR * DummyVolumes)
        # Negative because recording started BEFORE the first trigger
        start_time = -(30.0 + (tr * dummy_volumes))
        
        logger.info(f"  Signal type: {signal_type}")
        logger.info(f"  Sampling frequency: {sampling_freq} Hz")
        logger.info(f"  TR: {tr}s, Dummy volumes: {dummy_volumes}")
        logger.info(f"  StartTime: {start_time}s (-(30s + {tr}s × {dummy_volumes}))")
        
        # Get BIDS recording type
        recording_type = SIGNAL_TYPE_MAPPING.get(signal_type, signal_type.lower())
        
        # Create JSON sidecar
        json_data = {
            "SamplingFrequency": sampling_freq,
            "StartTime": round(start_time, 4),
            "Columns": [recording_type]
        }
        
        json_path = Path(str(output_path) + '_physio.json')
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=4)
        
        logger.info(f"  ✓ Created {json_path.name}")
        
        # Write data file (gzipped TSV, single column)
        tsv_path = Path(str(output_path) + '_physio.tsv.gz')
        with gzip.open(tsv_path, 'wt') as f:
            for value in data:
                f.write(f"{value:.6f}\n")
        
        logger.info(f"  ✓ Created {tsv_path.name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to process {physio_file.name}: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


def import_physio_inhouse(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    config: Dict,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None
) -> bool:
    """Import physiological data to BIDS format using in-house processing.
    
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
    config : Dict
        Configuration dictionary (must contain 'DummyVolumes')
    ds_initials : Optional[str]
        Dataset initials prefix
    session : Optional[str]
        Session label (without 'ses-' prefix)
        
    Returns
    -------
    bool
        True if import successful, False otherwise
    """
    # Validate paths
    if not sourcedata_dir.exists():
        logger.error(f"Source data directory not found: {sourcedata_dir}")
        return False
    
    physio_dir = sourcedata_dir / "physio"
    if not physio_dir.exists():
        logger.error(f"Physio directory not found: {physio_dir}")
        return False
    
    logger.info(f"Found physio directory: {physio_dir}")
    
    # Get dummy volumes from config
    dummy_volumes = config.get('DummyVolumes', 0)
    logger.info(f"Using DummyVolumes = {dummy_volumes}")
    
    # If ds_initials not provided, try to infer
    if ds_initials is None:
        parts = dataset.split('-')
        if len(parts) >= 2:
            name_part = parts[1]
            words = name_part.replace('_', ' ').split()
            ds_initials = ''.join([w[0].upper() for w in words if w])
            logger.info(f"Inferred dataset initials: {ds_initials}")
    
    # Process each participant
    success_count = 0
    failed_participants = []
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing physio data for participant {participant_id}")
        logger.info(f"{'='*60}")
        
        # Determine source directory
        if ds_initials:
            if session:
                source_name = f"{ds_initials}{participant_id}SES{session}"
            else:
                source_name = f"{ds_initials}{participant_id}"
            
            physio_source_dir = physio_dir / source_name
            
            if not physio_source_dir.exists():
                logger.error(f"Physio directory not found: {physio_source_dir}")
                failed_participants.append(participant_id)
                continue
        else:
            # Fallback to pattern matching
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
        
        logger.info(f"Found {len(physio_files)} physio data files:")
        for pf in physio_files:
            logger.info(f"  {pf['filename']} ({pf['signal_type']})")
        
        # Match physio files to fMRI runs
        matches = match_physio_to_fmri(physio_files, func_dir)
        
        if not matches:
            logger.error("No matches found between physio and fMRI")
            failed_participants.append(participant_id)
            continue
        
        # Process each match
        match_success = 0
        for match in matches:
            # Build output filename
            if session:
                output_name = f"sub-{participant_id}_ses-{session}_task-{match['task']}"
            else:
                output_name = f"sub-{participant_id}_task-{match['task']}"
            
            # Add run entity only if present in fMRI filename
            if match['run'] is not None:
                output_name += f"_run-{match['run']}"
            
            # Add recording entity
            output_name += f"_recording-{match['signal_type'].lower()}"
            
            output_path = func_dir / output_name
            
            success = process_physio_file(
                physio_file=match['physio_file'],
                signal_type=match['signal_type'],
                tr=match['tr'],
                dummy_volumes=dummy_volumes,
                output_path=output_path
            )
            
            if success:
                match_success += 1
        
        if match_success > 0:
            logger.info(f"✓ Successfully processed {match_success}/{len(matches)} physio files for {participant_id}")
            success_count += 1
        else:
            logger.error(f"✗ Failed to process physio data for {participant_id}")
            failed_participants.append(participant_id)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Physio Import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    return success_count > 0
