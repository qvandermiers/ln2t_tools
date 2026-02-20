"""MRS data to BIDS conversion using spec2bids/spec2nii."""

import logging
import subprocess
import shutil
import tarfile
import os
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

# Default paths for scanner MRS data locations
DEFAULT_MRRAW_DIR = Path("/home/ln2t-worker/PETMR/backup/auto/daily_backups/mrraw")
DEFAULT_TMP_DIR = Path("/home/ln2t-worker/PETMR/backup/auto/daily_backups/tmp")


# =============================================================================
# Pre-import functions: Gather P-files from scanner backup locations
# =============================================================================

def get_dicom_metadata(dicom_file: Path) -> Dict:
    """Extract relevant metadata from a DICOM file.
    
    Uses pydicom to read exam date/time and exam number from a DICOM file.
    
    Parameters
    ----------
    dicom_file : Path
        Path to a DICOM file
        
    Returns
    -------
    Dict
        Dictionary with keys: 'exam_date', 'exam_time', 'exam_datetime', 'exam_number'
        Values are None if not found
    """
    try:
        import pydicom
    except ImportError:
        logger.error("pydicom is required for MRS pre-import. Install with: pip install pydicom")
        raise
    
    metadata = {
        'exam_date': None,
        'exam_time': None,
        'exam_datetime': None,
        'exam_number': None,
        'patient_id': None,
    }
    
    try:
        ds = pydicom.dcmread(dicom_file, stop_before_pixels=True)
        
        # Get study date and time
        if hasattr(ds, 'StudyDate'):
            metadata['exam_date'] = ds.StudyDate  # Format: YYYYMMDD
        if hasattr(ds, 'StudyTime'):
            metadata['exam_time'] = ds.StudyTime  # Format: HHMMSS.ffffff
        
        # Parse into datetime object
        if metadata['exam_date']:
            try:
                date_str = metadata['exam_date']
                time_str = metadata['exam_time'] or '000000'
                # Handle time with or without fractional seconds
                time_str = time_str.split('.')[0][:6]  # Take only HHMMSS
                dt_str = f"{date_str}{time_str}"
                metadata['exam_datetime'] = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
            except ValueError as e:
                logger.warning(f"Could not parse datetime from {date_str} {time_str}: {e}")
        
        # Get exam number (StudyID or AccessionNumber depending on scanner)
        # GE scanners typically use StudyID
        if hasattr(ds, 'StudyID'):
            metadata['exam_number'] = ds.StudyID
        elif hasattr(ds, 'AccessionNumber'):
            metadata['exam_number'] = ds.AccessionNumber
        
        # Get patient ID for reference
        if hasattr(ds, 'PatientID'):
            metadata['patient_id'] = ds.PatientID
            
    except Exception as e:
        logger.error(f"Failed to read DICOM metadata from {dicom_file}: {e}")
    
    return metadata


def find_dicom_for_participant(
    dicom_dir: Path,
    participant_label: str,
    ds_initials: str,
    session: Optional[str] = None
) -> Optional[Path]:
    """Find a DICOM file for a given participant.
    
    Parameters
    ----------
    dicom_dir : Path
        Path to the DICOM source directory
    participant_label : str
        Participant label (without 'sub-' prefix)
    ds_initials : str
        Dataset initials (e.g., 'CB', 'HP')
    session : Optional[str]
        Session label (without 'ses-' prefix)
        
    Returns
    -------
    Optional[Path]
        Path to a DICOM file, or None if not found
    """
    # Build expected source directory name
    if session:
        source_name = f"{ds_initials}{participant_label}SES{session}"
    else:
        source_name = f"{ds_initials}{participant_label}"
    
    # Check for directory
    source_path = dicom_dir / source_name
    if not source_path.exists():
        # Try extracting from archive
        archive_path = dicom_dir / f"{source_name}.tar.gz"
        if archive_path.exists():
            logger.info(f"Extracting DICOM archive for metadata: {archive_path.name}")
            try:
                with tarfile.open(archive_path, 'r:gz') as tar:
                    tar.extractall(path=dicom_dir)
                if not source_path.exists():
                    logger.error(f"Extracted archive but {source_name} not found")
                    return None
            except Exception as e:
                logger.error(f"Failed to extract DICOM archive: {e}")
                return None
        else:
            logger.error(f"DICOM source not found: {source_name}")
            return None
    
    # Find a DICOM file (any will do, metadata should be consistent)
    for root, dirs, files in os.walk(source_path):
        for f in files:
            filepath = Path(root) / f
            # Skip hidden files and common non-DICOM files
            if f.startswith('.') or f.endswith(('.txt', '.json', '.csv', '.tar.gz')):
                continue
            # Check if it looks like a DICOM (no extension or .dcm)
            if not filepath.suffix or filepath.suffix.lower() == '.dcm':
                return filepath
    
    logger.error(f"No DICOM files found in {source_path}")
    return None


def find_pfiles_by_datetime(
    mrraw_dir: Path,
    exam_datetime: datetime,
    tolerance_hours: float = 1.0
) -> List[Path]:
    """Find P-files in mrraw directory matching exam datetime.
    
    P-files in mrraw are named like P98816.7 and we match based on file
    creation/modification time.
    
    Parameters
    ----------
    mrraw_dir : Path
        Path to the mrraw directory
    exam_datetime : datetime
        The exam datetime from DICOM metadata
    tolerance_hours : float
        Time tolerance in hours for matching files
        
    Returns
    -------
    List[Path]
        List of P-file paths matching the datetime criteria
    """
    if not mrraw_dir.exists():
        logger.warning(f"mrraw directory not found: {mrraw_dir}")
        return []
    
    matching_files = []
    tolerance = timedelta(hours=tolerance_hours)
    
    for item in mrraw_dir.iterdir():
        # P-files have names like P98816.7
        if not item.name.startswith('P'):
            continue
        
        # Skip symlinks
        if item.is_symlink():
            continue
        
        # Check if it's a file (not directory)
        if not item.is_file():
            continue
        
        # Get file modification time
        try:
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            
            # Check if within tolerance
            if abs(mtime - exam_datetime) <= tolerance:
                matching_files.append(item)
                logger.debug(f"Found matching P-file: {item.name} (mtime: {mtime})")
        except Exception as e:
            logger.warning(f"Could not check file time for {item}: {e}")
    
    return matching_files


def find_pfiles_by_exam_number(
    tmp_dir: Path,
    exam_number: str
) -> List[Path]:
    """Find P-files in tmp directory by exam number.
    
    The tmp directory structure is:
    tmp/{exam_number}/... (with P-files somewhere inside)
    
    Parameters
    ----------
    tmp_dir : Path
        Path to the tmp directory
    exam_number : str
        The exam number from DICOM metadata
        
    Returns
    -------
    List[Path]
        List of P-file paths found for this exam
    """
    if not tmp_dir.exists():
        logger.warning(f"tmp directory not found: {tmp_dir}")
        return []
    
    exam_dir = tmp_dir / str(exam_number)
    if not exam_dir.exists():
        logger.debug(f"No exam directory found: {exam_dir}")
        return []
    
    matching_files = []
    
    # Recursively search for P-files
    for root, dirs, files in os.walk(exam_dir):
        for f in files:
            # P-files have names like P98816.7
            if f.startswith('P') and '.' in f:
                # Basic validation: second part should be numeric-ish
                parts = f.split('.')
                if len(parts) == 2 and parts[0][1:].isdigit():
                    filepath = Path(root) / f
                    if filepath.is_file() and not filepath.is_symlink():
                        matching_files.append(filepath)
                        logger.debug(f"Found P-file in exam dir: {filepath}")
    
    return matching_files


def pre_import_mrs(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    ds_initials: str,
    session: Optional[str] = None,
    mrraw_dir: Optional[Path] = None,
    tmp_dir: Optional[Path] = None,
    tolerance_hours: float = 1.0,
    dry_run: bool = False
) -> bool:
    """Pre-import MRS data: gather P-files from scanner backup locations.
    
    This function:
    1. For each participant, finds their DICOM data to extract exam metadata
    2. Uses the exam datetime to find matching P-files in mrraw directory
    3. Uses the exam number to find P-files in the tmp directory
    4. Copies all found P-files to {sourcedata_dir}/mrs/{ds_initials}{participant}
    
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
    mrraw_dir : Optional[Path]
        Path to mrraw directory. Defaults to DEFAULT_MRRAW_DIR
    tmp_dir : Optional[Path]
        Path to tmp directory. Defaults to DEFAULT_TMP_DIR
    tolerance_hours : float
        Time tolerance in hours for matching P-files by datetime
    dry_run : bool
        If True, only report what would be done without copying files
        
    Returns
    -------
    bool
        True if pre-import successful for at least one participant
    """
    # Set default paths
    if mrraw_dir is None:
        mrraw_dir = DEFAULT_MRRAW_DIR
    if tmp_dir is None:
        tmp_dir = DEFAULT_TMP_DIR
    
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}MRS Pre-import")
    logger.info(f"  Dataset: {dataset}")
    logger.info(f"  Participants: {participant_labels}")
    logger.info(f"  mrraw dir: {mrraw_dir}")
    logger.info(f"  tmp dir: {tmp_dir}")
    logger.info(f"  Tolerance: {tolerance_hours} hours")
    
    # Check source directories exist
    dicom_dir = sourcedata_dir / "dicom"
    if not dicom_dir.exists():
        logger.error(f"DICOM directory not found: {dicom_dir}")
        logger.error("Cannot extract exam metadata without DICOM files")
        return False
    
    # Create mrs directory in sourcedata
    mrs_output_dir = sourcedata_dir / "mrs"
    if not dry_run:
        mrs_output_dir.mkdir(parents=True, exist_ok=True)
    
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
        logger.info(f"    Exam Number: {metadata['exam_number']}")
        logger.info(f"    Patient ID: {metadata['patient_id']}")
        
        if metadata['exam_datetime'] is None and metadata['exam_number'] is None:
            logger.error(f"Could not extract exam datetime or exam number for {participant_id}")
            failed_participants.append(participant_id)
            continue
        
        # Step 2: Find P-files from both locations
        pfiles = []
        
        # From mrraw by datetime
        if metadata['exam_datetime'] is not None:
            mrraw_pfiles = find_pfiles_by_datetime(
                mrraw_dir, metadata['exam_datetime'], tolerance_hours
            )
            if mrraw_pfiles:
                logger.info(f"  Found {len(mrraw_pfiles)} P-file(s) in mrraw by datetime")
                pfiles.extend(mrraw_pfiles)
            else:
                logger.info(f"  No P-files found in mrraw matching datetime")
        
        # From tmp by exam number
        if metadata['exam_number'] is not None:
            tmp_pfiles = find_pfiles_by_exam_number(tmp_dir, metadata['exam_number'])
            if tmp_pfiles:
                logger.info(f"  Found {len(tmp_pfiles)} P-file(s) in tmp for exam {metadata['exam_number']}")
                pfiles.extend(tmp_pfiles)
            else:
                logger.info(f"  No P-files found in tmp for exam number {metadata['exam_number']}")
        
        # Deduplicate (in case same file found via both methods)
        pfiles = list(set(pfiles))
        
        if not pfiles:
            logger.warning(f"No P-files found for {participant_id}")
            failed_participants.append(participant_id)
            continue
        
        logger.info(f"  Total P-files found: {len(pfiles)}")
        for pf in pfiles:
            logger.info(f"    - {pf}")
        
        # Step 3: Copy P-files to mrs directory
        if session:
            output_dir = mrs_output_dir / f"{ds_initials}{participant_id}SES{session}"
        else:
            output_dir = mrs_output_dir / f"{ds_initials}{participant_id}"
        
        if dry_run:
            logger.info(f"  [DRY RUN] Would copy {len(pfiles)} P-file(s) to {output_dir}")
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            
            copied_count = 0
            for pfile in pfiles:
                dest = output_dir / pfile.name
                if dest.exists():
                    logger.info(f"  P-file already exists, skipping: {pfile.name}")
                    copied_count += 1
                    continue
                
                try:
                    shutil.copy2(pfile, dest)
                    logger.info(f"  ✓ Copied: {pfile.name}")
                    copied_count += 1
                except Exception as e:
                    logger.error(f"  ✗ Failed to copy {pfile.name}: {e}")
            
            if copied_count > 0:
                logger.info(f"  ✓ Copied {copied_count} P-file(s) for {participant_id}")
                success_count += 1
            else:
                logger.error(f"  ✗ Failed to copy any P-files for {participant_id}")
                failed_participants.append(participant_id)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"MRS Pre-import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    return success_count > 0


# =============================================================================
# Discovery and archive utility functions
# =============================================================================

def discover_participants_from_mrs_dir(
    mrs_dir: Path,
    ds_initials: str
) -> List[str]:
    """Discover participant labels from MRS directory.
    
    Scans the mrs directory for folders and archives matching the dataset
    initials pattern and extracts participant labels.
    
    Parameters
    ----------
    mrs_dir : Path
        Path to mrs directory (e.g., sourcedata/mrs or sourcedata/pfiles)
    ds_initials : str
        Dataset initials prefix (e.g., 'CB', 'HP')
        
    Returns
    -------
    List[str]
        List of participant labels (without dataset initials or 'sub-' prefix)
    """
    participants = set()
    
    # Pattern: {ds_initials}* (e.g., CB001, CB002, HP042SES1)
    # Look for both directories and .tar.gz archives
    
    for item in mrs_dir.iterdir():
        name = item.name
        
        # Remove .tar.gz extension if present
        if name.endswith('.tar.gz'):
            name = name[:-7]  # Remove '.tar.gz'
        
        # Check if it starts with the dataset initials
        if name.startswith(ds_initials):
            # Extract participant ID by removing the initials
            participant_part = name[len(ds_initials):]
            
            # Handle session suffix (e.g., "042SES1" -> "042")
            if 'SES' in participant_part.upper():
                participant_part = participant_part.upper().split('SES')[0]
            
            if participant_part:
                participants.add(participant_part)
    
    result = sorted(list(participants))
    logger.info(f"Discovered {len(result)} participants from {mrs_dir}: {result}")
    return result


def verify_archive_integrity(archive_path: Path, extracted_dir: Path) -> bool:
    """Verify that an archive was extracted correctly by comparing file counts and sizes.
    
    Parameters
    ----------
    archive_path : Path
        Path to the .tar.gz archive
    extracted_dir : Path
        Path to the extracted directory
        
    Returns
    -------
    bool
        True if verification passes, False otherwise
    """
    try:
        # Get list of files in archive
        with tarfile.open(archive_path, 'r:gz') as tar:
            archive_members = tar.getmembers()
            archive_files = {m.name: m.size for m in archive_members if m.isfile()}
        
        # Get list of files in extracted directory
        extracted_files = {}
        for root, dirs, files in os.walk(extracted_dir):
            for f in files:
                filepath = Path(root) / f
                rel_path = filepath.relative_to(extracted_dir.parent)
                extracted_files[str(rel_path)] = filepath.stat().st_size
        
        # Compare
        if len(archive_files) != len(extracted_files):
            logger.error(f"File count mismatch: archive has {len(archive_files)}, extracted has {len(extracted_files)}")
            return False
        
        for name, size in archive_files.items():
            if name not in extracted_files:
                logger.error(f"Missing file after extraction: {name}")
                return False
            if extracted_files[name] != size:
                logger.error(f"Size mismatch for {name}: archive {size}, extracted {extracted_files[name]}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Archive verification failed: {e}")
        return False


def create_verified_archive(source_path: Path, archive_path: Path) -> bool:
    """Create a tar.gz archive with verification.
    
    Creates the archive and verifies it can be read back correctly before
    considering it successful.
    
    Parameters
    ----------
    source_path : Path
        Path to the directory to compress
    archive_path : Path
        Path for the output .tar.gz file
        
    Returns
    -------
    bool
        True if archive was created and verified successfully
    """
    temp_archive = archive_path.parent / f".{archive_path.name}.tmp"
    
    try:
        # Create archive
        logger.info(f"Creating archive: {archive_path.name}...")
        with tarfile.open(temp_archive, "w:gz") as tar:
            tar.add(source_path, arcname=source_path.name)
        
        # Verify archive by reading it back
        logger.info(f"Verifying archive integrity...")
        with tarfile.open(temp_archive, 'r:gz') as tar:
            members = tar.getmembers()
            archive_files = {m.name: m.size for m in members if m.isfile()}
        
        # Compare with source
        source_files = {}
        for root, dirs, files in os.walk(source_path):
            for f in files:
                filepath = Path(root) / f
                rel_path = filepath.relative_to(source_path.parent)
                source_files[str(rel_path)] = filepath.stat().st_size
        
        if len(archive_files) != len(source_files):
            logger.error(f"Archive verification failed: file count mismatch "
                        f"(source: {len(source_files)}, archive: {len(archive_files)})")
            temp_archive.unlink(missing_ok=True)
            return False
        
        for name, size in source_files.items():
            if name not in archive_files:
                logger.error(f"Archive verification failed: missing file {name}")
                temp_archive.unlink(missing_ok=True)
                return False
            if archive_files[name] != size:
                logger.error(f"Archive verification failed: size mismatch for {name}")
                temp_archive.unlink(missing_ok=True)
                return False
        
        # Move temp file to final location
        temp_archive.rename(archive_path)
        logger.info(f"✓ Archive created and verified: {archive_path.name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create archive: {e}")
        temp_archive.unlink(missing_ok=True)
        return False


def extract_archive_if_needed(
    mrs_dir: Path,
    source_name: str
) -> Tuple[Optional[Path], bool]:
    """Extract archive if directory doesn't exist but archive does.
    
    Parameters
    ----------
    mrs_dir : Path
        Path to mrs directory
    source_name : str
        Name of the source directory (e.g., 'CB042')
        
    Returns
    -------
    Tuple[Optional[Path], bool]
        (Path to source directory or None if not found, True if was extracted from archive)
    """
    source_path = mrs_dir / source_name
    archive_path = mrs_dir / f"{source_name}.tar.gz"
    
    # If directory exists, use it directly
    if source_path.exists():
        return source_path, False
    
    # If archive exists, extract it
    if archive_path.exists():
        logger.info(f"Directory {source_name} not found, extracting from archive...")
        try:
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(path=mrs_dir)
            
            if source_path.exists():
                logger.info(f"✓ Extracted {source_name} from archive")
                return source_path, True
            else:
                logger.error(f"Archive extracted but {source_name} directory not found")
                return None, False
                
        except Exception as e:
            logger.error(f"Failed to extract archive {archive_path.name}: {e}")
            return None, False
    
    # Neither directory nor archive exists
    return None, False


def import_mrs(
    dataset: str,
    participant_labels: Optional[List[str]],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = True,
    venv_path: Optional[Path] = None,
    overwrite: bool = False
) -> bool:
    """Import MRS data to BIDS format using spec2bids.
    
    Based on the spec2bids tool pattern: processes GE P-files and other
    MRS raw data formats into BIDS-compliant NIfTI format.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    participant_labels : Optional[List[str]]
        List of participant IDs (without 'sub-' prefix). If None, discovers
        participants automatically from the mrs directory.
    sourcedata_dir : Path
        Path to sourcedata directory
    rawdata_dir : Path
        Path to BIDS rawdata directory
    ds_initials : Optional[str]
        Dataset initials prefix (e.g., 'CB', 'HP')
    session : Optional[str]
        Session label (without 'ses-' prefix)
    compress_source : bool
        Whether to compress source files after successful conversion.
        If compression is successful, the original directory is deleted.
    venv_path : Optional[Path]
        Path to virtual environment containing spec2nii
    overwrite : bool
        If True, overwrite existing participant data. If False, skip existing participants.
        
    Returns
    -------
    bool
        True if import successful, False otherwise
    """
    # Validate paths
    if not sourcedata_dir.exists():
        logger.error(f"Source data directory not found: {sourcedata_dir}")
        return False
    
    # MRS data can be in 'mrs' or 'pfiles' directory
    mrs_dir = sourcedata_dir / "mrs"
    if not mrs_dir.exists():
        mrs_dir = sourcedata_dir / "pfiles"
        if not mrs_dir.exists():
            logger.error(f"MRS directory not found in {sourcedata_dir} (tried 'mrs' and 'pfiles')")
            return False
    
    # Check for spec2bids config
    config_file = sourcedata_dir / "spec2bids" / "config.json"
    if not config_file.exists():
        config_file = sourcedata_dir / "configs" / "spec2bids.json"
        if not config_file.exists():
            logger.error(
                f"spec2bids config not found at:\n"
                f"  {sourcedata_dir}/spec2bids/config.json\n"
                f"  {sourcedata_dir}/configs/spec2bids.json"
            )
            return False
    
    logger.info(f"Using spec2bids config: {config_file}")
    
    # Validate config structure
    try:
        with open(config_file) as f:
            config = json.load(f)
        if "manufacturer" not in config:
            logger.error("spec2bids config missing 'manufacturer' field")
            return False
        if "descriptions" not in config:
            logger.error("spec2bids config missing 'descriptions' field")
            return False
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        return False
    
    # If ds_initials not provided, extract from dataset name
    # Dataset format: 2024-Fantastic_Fox-123456789 -> FF
    if ds_initials is None:
        # Extract initials from dataset name (e.g., "Fantastic_Fox" -> "FF")
        # Split by '-', take the middle part (name), then get first letter of each word
        parts = dataset.split('-')
        if len(parts) >= 2:
            name_part = parts[1]  # e.g., "Fantastic_Fox"
            words = name_part.replace('_', ' ').split()
            ds_initials = ''.join([w[0].upper() for w in words if w])
            logger.info(f"Inferred dataset initials: {ds_initials}")
        else:
            logger.error(f"Could not infer dataset initials from '{dataset}'. "
                        f"Please provide --ds-initials explicitly.")
            return False
    
    # Discover participants if not provided
    if participant_labels is None or len(participant_labels) == 0:
        logger.info("No participant labels provided, discovering from MRS directory...")
        participant_labels = discover_participants_from_mrs_dir(mrs_dir, ds_initials)
        
        if not participant_labels:
            logger.error(f"No participants found in {mrs_dir} matching pattern {ds_initials}*")
            return False
    
    # Filter out existing participants unless overwrite is enabled
    if not overwrite:
        new_participants = []
        for participant in participant_labels:
            participant_id = participant.replace('sub-', '')
            # Check if MRS data already exists for this participant
            if session:
                mrs_dir = rawdata_dir / f"sub-{participant_id}" / f"ses-{session}" / "mrs"
            else:
                mrs_dir = rawdata_dir / f"sub-{participant_id}" / "mrs"
            
            if mrs_dir.exists():
                logger.info(f"Participant {participant_id} already has MRS data, skipping (use --overwrite to re-process)")
            else:
                new_participants.append(participant)
        
        if not new_participants:
            logger.info("All participants already have MRS data. Skipping MRS import.")
            return True
        
        participant_labels = new_participants
    
    # Check for spec2bids executable (priority order)
    spec2bids_path = None
    venv_cmd = ""
    
    # Priority 1: Check /opt/ln2t/spec2bids/venv/spec2bids
    opt_spec2bids = Path("/opt/ln2t/spec2bids/venv/spec2bids")
    if opt_spec2bids.exists() and opt_spec2bids.is_file():
        spec2bids_path = opt_spec2bids
        logger.info(f"Using spec2bids from priority path: {spec2bids_path}")
    else:
        # Priority 2: Setup virtual environment
        if venv_path is None:
            venv_path = Path.home() / "venvs" / "general_purpose_env"
        
        activate_script = venv_path / "bin" / "activate"
        if not activate_script.exists():
            logger.warning(f"Virtual environment not found at {venv_path}, will try system spec2bids")
            venv_cmd = ""
        else:
            venv_cmd = f". {activate_script} && "
        
        # Priority 3: Check if spec2bids is available via which
        check_cmd = f"{venv_cmd}which spec2bids"
        result = subprocess.run(
            check_cmd,
            shell=True,
            capture_output=True,
            text=True,
            executable='/bin/bash'
        )
        
        if result.returncode == 0:
            logger.info(f"Found spec2bids via which: {result.stdout.strip()}")
        else:
            logger.error(
                "spec2bids not found. Please install it:\n"
                "  1. Build at /opt/ln2t/spec2bids/venv/spec2bids (priority)\n"
                "  2. Install in virtual environment at ~/venvs/general_purpose_env\n"
                "  3. Clone from https://github.com/arovai/spec2bids or install via pip"
            )
            return False
    
    # Create rawdata directory if needed
    rawdata_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each participant
    success_count = 0
    failed_participants = []
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        
        # Determine source directory name using strict pattern
        # Use strict naming convention: AB042 or AB042SES4
        if session:
            source_name = f"{ds_initials}{participant_id}SES{session}"
        else:
            source_name = f"{ds_initials}{participant_id}"
        
        # Try to get source path (extract from archive if needed)
        source_path, was_extracted = extract_archive_if_needed(mrs_dir, source_name)
        
        if source_path is None:
            logger.error(f"Source MRS not found: {source_name} (checked directory and .tar.gz archive)")
            logger.error(f"Expected naming convention: {ds_initials}{participant_id}" + 
                       (f"SES{session}" if session else ""))
            failed_participants.append(participant_id)
            continue
        
        # Run spec2bids
        logger.info(f"Running spec2bids for {participant_id}...")
        
        # Build spec2bids command
        # Use the full path if we found it in the priority location, otherwise use the venv command
        if spec2bids_path:
            cmd = f"{spec2bids_path} -p {participant_id}"
        else:
            cmd = f"{venv_cmd}spec2bids -p {participant_id}"
        
        if session:
            cmd += f" -s {session}"
        cmd += f" -o {rawdata_dir} -d {source_path} -c {config_file}"
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                executable='/bin/bash'
            )
            logger.info(f"✓ Successfully imported MRS data for {participant_id}")
            if result.stdout:
                logger.debug(result.stdout)
            success_count += 1
            
            # Compress source data if requested (only after successful conversion)
            if compress_source and not was_extracted:
                # Don't compress if we just extracted from an archive
                compressed_file = mrs_dir / f"{source_path.name}.tar.gz"
                if not compressed_file.exists():
                    if create_verified_archive(source_path, compressed_file):
                        # Archive verified successfully, safe to delete original
                        logger.info(f"Deleting original directory after successful compression: {source_path.name}")
                        shutil.rmtree(source_path)
                        logger.info(f"✓ Deleted {source_path.name}")
                    else:
                        logger.warning(f"Archive creation/verification failed, keeping original directory: {source_path.name}")
                else:
                    logger.info(f"Archive already exists: {compressed_file.name}")
            
            # If we extracted from archive and conversion succeeded, clean up extracted directory
            if was_extracted:
                logger.info(f"Cleaning up extracted directory: {source_path.name}")
                shutil.rmtree(source_path)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"✗ Failed to import MRS data for {participant_id}: {e.stderr}")
            failed_participants.append(participant_id)
            
            # If we extracted from archive and conversion failed, still clean up
            if was_extracted and source_path.exists():
                logger.info(f"Cleaning up extracted directory after failed conversion: {source_path.name}")
                shutil.rmtree(source_path)
            continue
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"MRS Import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    return success_count > 0


def validate_mrs_import(
    rawdata_dir: Path,
    participant_labels: List[str],
    session: Optional[str] = None
) -> None:
    """Validate MRS import by checking for expected files.
    
    Parameters
    ----------
    rawdata_dir : Path
        Path to BIDS rawdata directory
    participant_labels : List[str]
        List of participant IDs
    session : Optional[str]
        Session label if applicable
    """
    logger.info("Validating MRS import...")
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        subj_dir = rawdata_dir / f"sub-{participant_id}"
        
        if not subj_dir.exists():
            logger.warning(f"Subject directory not found: {subj_dir}")
            continue
        
        if session:
            ses_dir = subj_dir / f"ses-{session}"
            if not ses_dir.exists():
                logger.warning(f"Session directory not found: {ses_dir}")
                continue
            search_dir = ses_dir
        else:
            search_dir = subj_dir
        
        # Look for MRS data in mrs/ datatype directory
        mrs_datatype_dir = search_dir / "mrs"
        if mrs_datatype_dir.exists():
            nii_files = list(mrs_datatype_dir.glob("*.nii*"))
            json_files = list(mrs_datatype_dir.glob("*.json"))
            logger.info(f"  sub-{participant_id}: {len(nii_files)} NIfTI, {len(json_files)} JSON")
        else:
            logger.warning(f"  sub-{participant_id}: No mrs/ directory found")
