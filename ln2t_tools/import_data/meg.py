"""MEG data to BIDS conversion using mne-bids.

Converts Neuromag/Elekta/MEGIN FIF files to BIDS format with automatic
MaxFilter derivative detection and calibration file handling.
"""

import logging
import json
import re
import shutil
import fnmatch
import os
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, Set
from collections import defaultdict
from datetime import datetime, date, timezone
import warnings

logger = logging.getLogger(__name__)

# Suppress verbose mne and mne_bids output
try:
    import mne
    from mne_bids import write_raw_bids, BIDSPath
    mne.set_log_level('ERROR')
    logging.getLogger('mne_bids').setLevel(logging.ERROR)
    logging.getLogger('mne').setLevel(logging.ERROR)
    warnings.filterwarnings('ignore')
    MNE_AVAILABLE = True
except ImportError:
    MNE_AVAILABLE = False
    logger.warning("MNE-Python and mne-bids not available. Install with: pip install mne mne-bids")


class ConversionStats:
    """Track conversion statistics for reporting."""
    
    def __init__(self):
        self.total_files = 0
        self.converted = 0
        self.skipped = 0
        self.failed = 0
        self.task_counts = defaultdict(int)
        self.failed_files = []
    
    def add_file(self, task: str, status: str, filename: str = ""):
        """Record a file conversion."""
        self.total_files += 1
        if status == 'converted':
            self.converted += 1
            self.task_counts[task] += 1
        elif status == 'skipped':
            self.skipped += 1
        elif status == 'failed':
            self.failed += 1
            if filename:
                self.failed_files.append(filename)


def load_meg_config(config_path: Optional[Path], sourcedata_dir: Path) -> Dict[str, Any]:
    """Load MEG conversion configuration from JSON file.
    
    If config_path is not provided, auto-detects from sourcedata/configs/meg2bids.json
    
    Parameters
    ----------
    config_path : Optional[Path]
        Path to meg2bids.json configuration file
    sourcedata_dir : Path
        Path to sourcedata directory for auto-detection
    
    Returns
    -------
    Dict[str, Any]
        Configuration dictionary
    """
    if config_path is None:
        # Auto-detect from sourcedata/configs/
        config_path = sourcedata_dir / "configs" / "meg2bids.json"
    
    if not config_path.exists():
        raise FileNotFoundError(f"MEG configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    if 'file_patterns' not in config:
        raise ValueError("Configuration must contain 'file_patterns' section")
    
    logger.info(f"Loaded MEG configuration from {config_path}")
    return config


def load_participants_mapping(participants_file: Path) -> Dict[str, str]:
    """Load participants_complete.tsv and create mapping of meg_id -> bids_subject.
    
    Parameters
    ----------
    participants_file : Path
        Path to participants_complete.tsv
    
    Returns
    -------
    Dict[str, str]
        Mapping of meg_id (4 digits) -> bids_subject (e.g., '1001' -> 'sub-01')
    """
    if not participants_file.exists():
        raise FileNotFoundError(f"Participants file not found: {participants_file}")
    
    mapping = {}
    with open(participants_file, 'r') as f:
        lines = f.readlines()
    
    if not lines:
        raise ValueError("Participants file is empty")
    
    # Parse header
    header = lines[0].strip().split('\t')
    try:
        participant_idx = header.index('participant_id')
        meg_id_idx = header.index('meg_id')
    except ValueError as e:
        raise ValueError(f"Missing required column in participants file: {e}")
    
    # Parse rows
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.strip().split('\t')
        if len(parts) <= max(participant_idx, meg_id_idx):
            continue
        
        bids_subject = parts[participant_idx].strip()
        meg_id_raw = parts[meg_id_idx].strip()
        
        # Extract 4 digits from meg_id (e.g., '1001' from 'meg_1001' or 'MEG-1001')
        digits = re.findall(r'\d{4}', meg_id_raw)
        if digits:
            meg_id = digits[0]
            mapping[meg_id] = bids_subject
            logger.debug(f"  Mapped meg_id {meg_id} ({meg_id_raw}) -> {bids_subject}")
    
    if not mapping:
        raise ValueError("No valid meg_id mappings found in participants file")
    
    logger.info(f"Loaded {len(mapping)} participant mappings")
    return mapping


def extract_derivative_info(filename: str) -> Optional[Tuple[str, str]]:
    """Detect and extract MaxFilter processing information from filename.
    
    Strips recognized suffixes and builds the base filename. Handles multiple
    suffixes (e.g., chessboard2_mc_ave.fif -> chessboard2.fif, "mc-ave").
    
    Parameters
    ----------
    filename : str
        FIF filename to check
    
    Returns
    -------
    Optional[Tuple[str, str]]
        (base_filename, proc_label) if derivative detected, None if raw file
    """
    stem = Path(filename).stem
    
    # Recognized MaxFilter suffixes in order of priority
    derivative_suffixes = [
        ('_tsss', 'tsss'),
        ('_sss', 'sss'),
        ('_mc', 'mc'),
        ('_quat', 'quat'),
        ('_trans', 'trans'),
        ('_ave', 'ave'),
        ('_av', 'ave'),
    ]
    
    current_stem = stem
    found_suffixes = []
    
    while True:
        found_match = False
        for suffix, label in derivative_suffixes:
            if current_stem.lower().endswith(suffix):
                found_suffixes.insert(0, label)
                current_stem = current_stem[:-len(suffix)]
                found_match = True
                break
        
        if not found_match:
            break
    
    if found_suffixes:
        # Combine multiple proc labels with hyphens, avoiding duplicates
        unique_labels = []
        for label in found_suffixes:
            if label not in unique_labels:
                unique_labels.append(label)
        proc_label = '-'.join(unique_labels)
        base_filename = current_stem + '.fif'
        return (base_filename, proc_label)
    
    return None


def detect_split_files(fif_files: List[Path]) -> Dict[Path, List[Path]]:
    """Detect and group multi-part FIFF files.
    
    Large FIF files automatically split into parts named: filename.fif,
    filename-1.fif, filename-2.fif, etc.
    
    Parameters
    ----------
    fif_files : List[Path]
        List of FIF file paths
    
    Returns
    -------
    Dict[Path, List[Path]]
        Mapping of primary file -> list of all parts in order
    """
    split_groups = {}
    processed = set()
    fif_files_set = set(fif_files)
    
    for fif_path in sorted(fif_files):
        if fif_path in processed:
            continue
        
        stem = fif_path.stem
        match = re.match(r'^(.+?)(?:-\d+)?$', stem)
        if not match:
            continue
        
        base_name = match.group(1)
        parent_dir = fif_path.parent
        base_file = parent_dir / f"{base_name}.fif"
        
        # Prefer the unsuffixed base file as primary
        if base_file in fif_files_set:
            primary = base_file
        else:
            primary = fif_path
        
        # Collect parts
        parts = [primary]
        idx = 1
        while True:
            next_part = parent_dir / f"{base_name}-{idx}.fif"
            if next_part in fif_files_set:
                parts.append(next_part)
                processed.add(next_part)
                idx += 1
            else:
                break
        
        if len(parts) > 1:
            split_groups[primary] = parts
            processed.update(parts)
            logger.debug(f"  Detected split file: {base_name} ({len(parts)} parts)")
    
    return split_groups


def detect_derivative_split_files(deriv_files: List[Path]) -> Dict[Path, List[Path]]:
    """Detect and group multi-part derivative FIFF files.
    
    For derivatives like NAP-1_tsss_mc.fif, NAP-2_tsss_mc.fif, we need to:
    1. Extract the base name and proc label (NAP, tsss-mc)
    2. Group by (base_name, proc_label)
    3. Detect split patterns within each group
    
    Handles both patterns:
    - NAP-1_tsss_mc.fif, NAP-2_tsss_mc.fif (split before proc)
    - NAP_tsss_mc-1.fif, NAP_tsss_mc-2.fif (split after proc)
    
    Parameters
    ----------
    deriv_files : List[Path]
        List of derivative FIF file paths
    
    Returns
    -------
    Dict[Path, List[Path]]
        Mapping of primary file -> list of all parts in order
    """
    split_groups = {}
    processed = set()
    deriv_files_set = set(deriv_files)
    
    # Group derivatives by (base_name, proc_label)
    deriv_groups = defaultdict(list)
    for deriv_file in deriv_files:
        deriv_info = extract_derivative_info(deriv_file.name)
        if deriv_info is None:
            continue
        base_filename, proc_label = deriv_info
        
        # Extract base name without split suffix
        # NAP.fif or NAP-1.fif -> NAP
        base_stem = Path(base_filename).stem
        base_match = re.match(r'^(.+?)(?:-\d+)?$', base_stem)
        if base_match:
            base_name = base_match.group(1)
        else:
            base_name = base_stem
        
        key = (base_name, proc_label)
        deriv_groups[key].append(deriv_file)
    
    # For each group, detect splits
    for (base_name, proc_label), files in deriv_groups.items():
        if len(files) <= 1:
            continue
        
        # Try pattern: NAP_proc.fif, NAP-1_proc.fif, NAP-2_proc.fif
        proc_suffix = proc_label.replace('-', '_')
        parent_dir = files[0].parent
        base_file = parent_dir / f"{base_name}_{proc_suffix}.fif"
        
        if base_file in deriv_files_set:
            primary = base_file
        else:
            # Use first numbered file as primary
            primary = parent_dir / f"{base_name}-1_{proc_suffix}.fif"
            if primary not in deriv_files_set:
                continue
        
        # Collect all parts
        parts = []
        if base_file in deriv_files_set:
            parts.append(base_file)
        
        idx = 1
        while True:
            next_part = parent_dir / f"{base_name}-{idx}_{proc_suffix}.fif"
            if next_part in deriv_files_set:
                parts.append(next_part)
                idx += 1
            else:
                break
        
        if len(parts) > 1:
            split_groups[parts[0]] = parts
            processed.update(parts)
            logger.debug(f"  Detected derivative split: {base_name}_{proc_suffix} ({len(parts)} parts)")
    
    return split_groups


def find_matching_raw_file(
    derivative_filename: str,
    raw_files: List[Path],
    split_file_groups: Dict[Path, List[Path]]
) -> Optional[Tuple[Path, Optional[int]]]:
    """Find the raw file that corresponds to a derivative file.
    
    Handles both split naming patterns:
    - file-1_mc.fif (split first, then processing)
    - file_mc-1.fif (processing first, then split)
    
    Parameters
    ----------
    derivative_filename : str
        Name of the derivative file (e.g., 'chessboard1_mc.fif', 'file_mc-1.fif')
    raw_files : List[Path]
        List of raw FIF file paths in the session
    split_file_groups : Dict[Path, List[Path]]
        Dict mapping primary file → list of all split parts
    
    Returns
    -------
    Optional[Tuple[Path, Optional[int]]]
        Tuple of (matching_raw_path, split_index) where:
        - matching_raw_path: Path to the raw file (primary file if split)
        - split_index: Index if derivative is a split part (0=primary, 1=first split, etc), None if not split
        Returns None if no match found
    """
    stem = Path(derivative_filename).stem
    
    # First, check if there's a split suffix at the end (e.g., file_mc-1 or file-1_mc)
    split_match = re.match(r'^(.+?)-(\d+)$', stem)
    split_num = None
    base_stem_with_proc = stem
    
    if split_match:
        # Has a split suffix: extract it
        base_stem_with_proc = split_match.group(1)  # e.g., "file_mc" or "file"
        split_num = int(split_match.group(2))        # e.g., 1, 2, 3
    
    # Now extract derivative info from the (possibly split-stripped) stem
    # Reconstruct filename for derivative extraction
    temp_filename = base_stem_with_proc + '.fif'
    deriv_info = extract_derivative_info(temp_filename)
    
    if not deriv_info:
        # Not a derivative file
        return None
    
    base_filename_stem, _ = deriv_info  # e.g., 'file.fif' → 'file'
    base_filename_stem = Path(base_filename_stem).stem
    
    # Now we have the base stem and possibly a split number
    if split_num is not None:
        # Derivative is for a split part (e.g., 'file_mc-1.fif' or 'file-1_mc.fif')
        primary_filename = f"{base_filename_stem}.fif"
        
        # Find the primary raw file
        for raw_path in raw_files:
            if raw_path.name == primary_filename:
                # Verify this is actually a split file group
                if raw_path in split_file_groups:
                    return (raw_path, split_num)  # split_num matches the -N suffix
                break
    else:
        # Derivative is for primary file (e.g., 'file_mc.fif')
        primary_filename = f"{base_filename_stem}.fif"
        for raw_path in raw_files:
            if raw_path.name == primary_filename:
                # Check if this raw file is part of a split group
                if raw_path in split_file_groups:
                    return (raw_path, 0)  # 0 = primary file
                else:
                    return (raw_path, None)  # Not a split file
    
    return None


def match_file_pattern(filename: str, patterns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Match a filename against configured patterns.
    
    Parameters
    ----------
    filename : str
        FIF filename to match
    patterns : List[Dict[str, Any]]
        List of pattern rules from configuration
    
    Returns
    -------
    Optional[Dict[str, Any]]
        Matched pattern rule dictionary, or None if no match
    """
    matches = []
    for idx, pattern_rule in enumerate(patterns):
        pattern = pattern_rule['pattern']
        if fnmatch.fnmatch(filename.lower(), pattern.lower()):
            matches.append((idx, pattern_rule))
    
    if len(matches) == 0:
        return None
    
    if len(matches) == 1:
        return matches[0][1]
    
    # Multiple matches - check if they all assign the same task
    tasks = set(rule.get('task', 'unknown') for _, rule in matches)
    
    if len(tasks) == 1:
        logger.debug(f"  {filename}: matches {len(matches)} patterns (all assign task={list(tasks)[0]})")
        return matches[0][1]
    
    # Multiple matches with different tasks - ambiguous!
    logger.error(f"  {filename}: matches multiple patterns with different tasks")
    for idx, (pattern_idx, rule) in enumerate(matches, 1):
        logger.error(f"    {idx}. Pattern: {rule.get('pattern')} -> task={rule.get('task')}")
    return None


def extract_run_from_filename(filename: str, extraction_method: str = "last_digits") -> Optional[int]:
    """Extract run number from filename.
    
    Excludes split file patterns (e.g., -1.fif, -2.fif).
    
    Parameters
    ----------
    filename : str
        FIF filename
    extraction_method : str
        Method to use: "last_digits" or "none"
    
    Returns
    -------
    Optional[int]
        Run number if found, None otherwise
    """
    if extraction_method == "none":
        return None
    
    stem = Path(filename).stem
    
    # Remove split suffix if present
    split_match = re.match(r'^(.+?)-(\d+)$', stem)
    if split_match:
        stem = split_match.group(1)
    
    matches = re.findall(r'\d+', stem)
    return int(matches[-1]) if matches else None


def find_meg_folder(meg_source_dir: Path, meg_id: str) -> Optional[Path]:
    """Find MEG folder for a subject, supporting multiple naming conventions.
    
    Supports both:
    - meg_XXXX (e.g., meg_1001)
    - XXXX_meg (e.g., 1001_meg)
    
    Parameters
    ----------
    meg_source_dir : Path
        Path to parent directory containing meg folders
    meg_id : str
        MEG ID (4 digits, e.g., '1001')
    
    Returns
    -------
    Optional[Path]
        Path to MEG folder if found, None otherwise
    """
    if not meg_source_dir.exists():
        return None
    
    # Try exact pattern first: meg_XXXX
    meg_folder = meg_source_dir / f"meg_{meg_id}"
    if meg_folder.exists() and meg_folder.is_dir():
        return meg_folder
    
    # Try reverse pattern: XXXX_meg
    meg_folder = meg_source_dir / f"{meg_id}_meg"
    if meg_folder.exists() and meg_folder.is_dir():
        return meg_folder
    
    # Fallback: search for any folder containing the meg_id
    for item in meg_source_dir.iterdir():
        if item.is_dir() and meg_id in item.name:
            return item
    
    return None


def auto_detect_sessions(source_dir: Path) -> List[Tuple[str, Optional[str]]]:
    """Auto-detect sessions from date-named folders.
    
    Parameters
    ----------
    source_dir : Path
        Path to subject's MEG directory (e.g., meg_1001/)
    
    Returns
    -------
    List[Tuple[str, Optional[str]]]
        List of (folder_name, session_id) tuples
    """
    session_folders = sorted([p for p in source_dir.iterdir() if p.is_dir()])
    
    if not session_folders:
        return []
    
    if len(session_folders) == 1:
        return [(session_folders[0].name, None)]
    
    sessions = []
    for idx, folder in enumerate(session_folders, start=1):
        session_id = f"{idx:02d}"
        sessions.append((folder.name, session_id))
    
    return sessions


def find_fine_calibration_file(
    meg_maxfilter_root: Path,
    session_date: Optional[str],
    calibration_system: str = 'triux'
) -> Optional[Path]:
    """Find appropriate fine-calibration file based on session date.
    
    Parameters
    ----------
    meg_maxfilter_root : Path
        Path to MEG/maxfilter directory
    session_date : Optional[str]
        Date string in format 'YYMMDD' from session folder name
    calibration_system : str
        'triux' or 'vectorview'
    
    Returns
    -------
    Optional[Path]
        Path to calibration file or None if not found
    """
    sss_dir = meg_maxfilter_root / 'sss'
    
    if not sss_dir.exists():
        logger.warning(f"  SSS directory not found: {sss_dir}")
        return None
    
    if calibration_system == 'vectorview':
        vectorview_file = sss_dir / 'sss_cal_vectorview.dat'
        if vectorview_file.exists():
            logger.info(f"  ✓ Fine-calibration: {vectorview_file.name} (VectorView)")
            return vectorview_file
        else:
            logger.warning(f"  VectorView calibration not found: {vectorview_file}")
            return None
    
    # Triux: find calibration file with date <= session date
    if not session_date:
        logger.error("  Could not extract session date - cannot match calibration file")
        return None
    
    try:
        # Parse YYMMDD format
        yy = int(session_date[:2])
        mm = int(session_date[2:4])
        dd = int(session_date[4:6])
        yyyy = 2000 + yy if yy <= 50 else 1900 + yy
        session_dt = datetime(yyyy, mm, dd)
    except (ValueError, IndexError) as e:
        logger.error(f"  Could not parse session date '{session_date}': {e}")
        return None
    
    # Find all sss_cal_XXXX_*.dat files
    cal_files = []
    for f in sss_dir.glob('sss_cal_3131_*.dat'):
        match = re.search(r'sss_cal_3131_(\d{6})\.dat', f.name)
        if match:
            date_str = match.group(1)
            try:
                yy = int(date_str[:2])
                mm = int(date_str[2:4])
                dd = int(date_str[4:6])
                yyyy = 2000 + yy if yy <= 50 else 1900 + yy
                cal_date = datetime(yyyy, mm, dd)
                cal_files.append((cal_date, f))
            except (ValueError, IndexError):
                continue
    
    if not cal_files:
        logger.error(f"  No fine-calibration files found matching pattern sss_cal_3131_*.dat")
        return None
    
    # Sort by date and find most recent <= session date
    cal_files.sort(key=lambda x: x[0])
    selected_file = None
    selected_date = None
    
    for cal_date, f in cal_files:
        if cal_date <= session_dt:
            selected_file = f
            selected_date = cal_date
    
    if selected_file:
        logger.info(f"  ✓ Fine-calibration: {selected_file.name} (cal date: {selected_date.strftime('%Y-%m-%d')}, session: {session_dt.strftime('%Y-%m-%d')})")
        return selected_file
    else:
        logger.error(f"  No calibration file with date <= {session_dt.strftime('%Y-%m-%d')} found")
        return None


def detect_calibration_files(
    source_dir: Path,
    session_folder: Optional[str],
    meg_maxfilter_root: Optional[Path],
    calibration_system: str = 'triux'
) -> Dict[str, Optional[Path]]:
    """Auto-detect Neuromag/Elekta/MEGIN calibration files.
    
    Parameters
    ----------
    source_dir : Path
        Path to subject's MEG directory
    session_folder : Optional[str]
        Session folder name for date extraction
    meg_maxfilter_root : Optional[Path]
        Path to MEG/maxfilter directory
    calibration_system : str
        'triux' or 'vectorview'
    
    Returns
    -------
    Dict[str, Optional[Path]]
        Dictionary with 'crosstalk' and 'calibration' file paths
    """
    calibration_files = {'crosstalk': None, 'calibration': None}
    
    if meg_maxfilter_root and meg_maxfilter_root.exists():
        ctc_dir = meg_maxfilter_root / 'ctc'
        
        if calibration_system == 'vectorview':
            crosstalk_file = ctc_dir / 'ct_sparse_vectorview.fif'
            if crosstalk_file.exists():
                calibration_files['crosstalk'] = crosstalk_file
                logger.debug(f"  Found cross-talk file (VectorView): {crosstalk_file.name}")
        else:
            crosstalk_file = ctc_dir / 'ct_sparse_triux2.fif'
            if crosstalk_file.exists():
                calibration_files['crosstalk'] = crosstalk_file
                logger.debug(f"  Found cross-talk file (Triux): {crosstalk_file.name}")
        
        # Extract session date from folder name
        session_date = None
        if session_folder:
            match = re.search(r'(\d{6})', session_folder)
            if match:
                session_date = match.group(1)
        
        calibration_files['calibration'] = find_fine_calibration_file(
            meg_maxfilter_root, session_date, calibration_system
        )
    
    # Fallback: search in session directory
    if not calibration_files['crosstalk'] or not calibration_files['calibration']:
        search_dir = source_dir / session_folder if session_folder else source_dir
        
        if not calibration_files['crosstalk']:
            for pattern in ['*crosstalk*.fif', '*cross_talk*.fif', '*sst*.fif']:
                matches = list(search_dir.glob(pattern))
                if matches:
                    calibration_files['crosstalk'] = matches[0]
                    logger.debug(f"  Found cross-talk file (fallback): {matches[0].name}")
                    break
        
        if not calibration_files['calibration']:
            for pattern in ['*calibration*.dat', '*sss*.dat']:
                matches = list(search_dir.glob(pattern))
                if matches:
                    calibration_files['calibration'] = matches[0]
                    logger.debug(f"  Found calibration file (fallback): {matches[0].name}")
                    break
    
    return calibration_files


def copy_calibration_files(
    source_calib_files: Dict[str, Optional[Path]],
    subject: str,
    session: Optional[str],
    bids_root: Path,
    datatype: str = 'meg'
) -> None:
    """Copy cross-talk and fine-calibration files to BIDS directory.
    
    Parameters
    ----------
    source_calib_files : Dict[str, Optional[Path]]
        Dictionary with 'crosstalk' and 'calibration' file paths
    subject : str
        Subject ID (without 'sub-' prefix)
    session : Optional[str]
        Session ID (without 'ses-' prefix)
    bids_root : Path
        BIDS root directory
    datatype : str
        Datatype (default: 'meg')
    """
    if session:
        target_dir = bids_root / f"sub-{subject}" / f"ses-{session}" / datatype
    else:
        target_dir = bids_root / f"sub-{subject}" / datatype
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    if source_calib_files['crosstalk']:
        if session:
            target_name = f"sub-{subject}_ses-{session}_acq-crosstalk_meg.fif"
        else:
            target_name = f"sub-{subject}_acq-crosstalk_meg.fif"
        
        target_path = target_dir / target_name
        shutil.copy2(source_calib_files['crosstalk'], target_path)
        logger.info(f"  ✓ Copied cross-talk file: {target_name}")
    
    if source_calib_files['calibration']:
        if session:
            target_name = f"sub-{subject}_ses-{session}_acq-calibration_meg.dat"
        else:
            target_name = f"sub-{subject}_acq-calibration_meg.dat"
        
        target_path = target_dir / target_name
        shutil.copy2(source_calib_files['calibration'], target_path)
        logger.info(f"  ✓ Copied calibration file: {target_name}")


def normalize_raw_info(raw: 'mne.io.BaseRaw') -> None:
    """Normalize raw.info fields for BIDS compatibility.
    
    Parameters
    ----------
    raw : mne.io.BaseRaw
        MNE raw object
    """
    md = raw.info.get('meas_date')
    if isinstance(md, date) and not isinstance(md, datetime):
        raw.set_meas_date(datetime(md.year, md.month, md.day, tzinfo=timezone.utc))
    elif isinstance(md, datetime) and md.tzinfo is None:
        raw.set_meas_date(md.replace(tzinfo=timezone.utc))
    
    si = raw.info.get('subject_info')
    if isinstance(si, dict) and 'birthday' in si:
        si['birthday'] = None


def convert_raw_file(
    fif_path: Path,
    subject: str,
    session: Optional[str],
    task: str,
    run: Optional[int],
    config: Dict[str, Any],
    bids_root: Path,
    split_parts: Optional[List[Path]] = None
) -> bool:
    """Convert a single raw FIF file to BIDS.
    
    Parameters
    ----------
    fif_path : Path
        Path to FIF file
    subject : str
        Subject ID (without 'sub-' prefix)
    session : Optional[str]
        Session ID (without 'ses-' prefix)
    task : str
        Task name
    run : Optional[int]
        Run number
    config : Dict[str, Any]
        Configuration dictionary
    bids_root : Path
        BIDS root directory
    split_parts : Optional[List[Path]]
        List of split file parts if applicable
    
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    datatype = config.get('dataset', {}).get('datatype', 'meg')
    allow_maxshield = config.get('options', {}).get('allow_maxshield', True)
    overwrite = config.get('options', {}).get('overwrite', True)
    
    run_str = f" (run {run})" if run is not None else ""
    split_str = f" ({len(split_parts)} parts)" if split_parts and len(split_parts) > 1 else ""
    logger.info(f"  ✓ Converting: {fif_path.name}{run_str}{split_str} -> task={task}")
    
    try:
        # MNE automatically handles split files
        raw = mne.io.read_raw_fif(fif_path, preload=False, allow_maxshield=allow_maxshield, verbose=False)
        normalize_raw_info(raw)
        
        bids_path = BIDSPath(
            subject=subject,
            session=session,
            task=task,
            run=run,
            datatype=datatype,
            root=bids_root
        )
        
        write_raw_bids(raw, bids_path, overwrite=overwrite, verbose=False)
        logger.debug(f"    -> Saved BIDS file: {bids_path.basename}")
        
        return True
        
    except Exception as err:
        logger.error(f"  ✗ Conversion failed for {fif_path.name}: {err}")
        return False


def copy_derivative_file(
    fif_path: Path,
    subject: str,
    session: Optional[str],
    task: str,
    run: Optional[int],
    derivative_info: Tuple[str, str],
    derivatives_root: Path,
    pipeline_name: str,
    pipeline_version: Optional[str] = None,
    split_parts: Optional[List[Path]] = None
) -> bool:
    """Copy a derivative FIF file to BIDS derivatives folder.
    
    Parameters
    ----------
    fif_path : Path
        Path to derivative FIF file (primary file if split)
    subject : str
        Subject ID (without 'sub-' prefix)
    session : Optional[str]
        Session ID (without 'ses-' prefix)
    task : str
        Task name
    run : Optional[int]
        Run number
    derivative_info : Tuple[str, str]
        (base_filename, proc_label) from extract_derivative_info
    derivatives_root : Path
        Base derivatives directory
    pipeline_name : str
        Pipeline name (e.g., 'maxfilter')
    pipeline_version : Optional[str]
        Pipeline version string
    split_parts : Optional[List[Path]]
        List of all split file parts (including primary)
    
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    base_filename, proc_label = derivative_info
    
    # Build pipeline directory name (use underscore to match maxfilter_version)
    if pipeline_version:
        pipeline_dir = f"{pipeline_name}_{pipeline_version}"
    else:
        pipeline_dir = pipeline_name
    
    # Create derivatives BIDS structure
    deriv_root = derivatives_root / pipeline_dir
    
    # Target directory
    if session:
        target_dir = deriv_root / f"sub-{subject}" / f"ses-{session}" / "meg"
    else:
        target_dir = deriv_root / f"sub-{subject}" / "meg"
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # If there are split parts, copy each with split entity
    if split_parts and len(split_parts) > 1:
        all_success = True
        for idx, split_file in enumerate(split_parts, start=1):
            # Target filename: sub-<label>[_ses-<label>]_task-<label>[_run-<index>]_split-<index>_proc-<label>_meg.fif
            fname_parts = [f"sub-{subject}"]
            if session:
                fname_parts.append(f"ses-{session}")
            fname_parts.append(f"task-{task}")
            if run is not None:
                fname_parts.append(f"run-{run:02d}")
            fname_parts.append(f"split-{idx:02d}")
            fname_parts.append(f"proc-{proc_label}")
            fname_parts.append("meg.fif")
            
            target_name = "_".join(fname_parts)
            target_path = target_dir / target_name
            
            try:
                shutil.copy2(split_file, target_path)
                logger.debug(f"    -> Saved split {idx}/{len(split_parts)}: {target_name}")
            except Exception as err:
                logger.error(f"  ✗ Failed to copy derivative split {split_file.name}: {err}")
                all_success = False
        
        return all_success
    
    else:
        # Single file (no splits)
        # Target filename: sub-<label>[_ses-<label>]_task-<label>[_run-<index>]_proc-<label>_meg.fif
        fname_parts = [f"sub-{subject}"]
        if session:
            fname_parts.append(f"ses-{session}")
        fname_parts.append(f"task-{task}")
        if run is not None:
            fname_parts.append(f"run-{run:02d}")
        fname_parts.append(f"proc-{proc_label}")
        fname_parts.append("meg.fif")
        
        target_name = "_".join(fname_parts)
        target_path = target_dir / target_name
        
        try:
            shutil.copy2(fif_path, target_path)
            logger.debug(f"    -> Saved to derivatives: {target_name}")
            return True
        except Exception as err:
            logger.error(f"  ✗ Failed to copy derivative {fif_path.name}: {err}")
            return False


def extract_bids_entities(filename: str) -> Dict[str, Optional[str]]:
    """Extract BIDS entities from a filename.
    
    Parses BIDS entities like sub, ses, task, run, etc.
    
    Parameters
    ----------
    filename : str
        BIDS filename (without path)
    
    Returns
    -------
    Dict[str, Optional[str]]
        Dictionary with extracted entities
    """
    entities = {
        'sub': None,
        'ses': None,
        'task': None,
        'run': None,
    }
    
    # Remove extension
    name_without_ext = filename.rsplit('.', 1)[0]
    
    # Split by underscore
    parts = name_without_ext.split('_')
    
    for part in parts:
        if part.startswith('sub-'):
            entities['sub'] = part[4:]
        elif part.startswith('ses-'):
            entities['ses'] = part[4:]
        elif part.startswith('task-'):
            entities['task'] = part[5:]
        elif part.startswith('run-'):
            entities['run'] = part[4:]
    
    return entities


def compare_tsv_files(file1: Path, file2: Path) -> bool:
    """Compare two TSV files for identical content.
    
    Parameters
    ----------
    file1 : Path
        First file path
    file2 : Path
        Second file path
    
    Returns
    -------
    bool
        True if files are identical, False otherwise
    """
    try:
        with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
            return f1.read() == f2.read()
    except Exception as err:
        logger.warning(f"Failed to compare {file1.name} and {file2.name}: {err}")
        return False


def create_most_general_name(filenames: List[str]) -> str:
    """Create the most general filename by removing run numbers.
    
    Only removes run entity, keeping sub, ses, and task.
    
    Parameters
    ----------
    filenames : List[str]
        List of identical filenames (should all have same task)
    
    Returns
    -------
    str
        The most general filename (with run removed)
    """
    if not filenames:
        return ""
    
    # Extract entities from first file (all should have same task)
    entities = extract_bids_entities(filenames[0])
    
    # Remove run number
    entities['run'] = None
    
    return reconstruct_filename(entities)


def reconstruct_filename(entities: Dict[str, Optional[str]], extension: str = ".tsv") -> str:
    """Reconstruct a BIDS filename from entities.
    
    Parameters
    ----------
    entities : Dict[str, Optional[str]]
        Dictionary with BIDS entities
    extension : str
        File extension (default: .tsv)
    
    Returns
    -------
    str
        Reconstructed filename
    """
    parts = []
    
    if entities.get('sub'):
        parts.append(f"sub-{entities['sub']}")
    if entities.get('ses'):
        parts.append(f"ses-{entities['ses']}")
    if entities.get('task'):
        parts.append(f"task-{entities['task']}")
    if entities.get('run'):
        parts.append(f"run-{entities['run']}")
    
    parts.append("channels")
    
    return "_".join(parts) + extension


def consolidate_identical_group(identical_files: List[Path]) -> Optional[Path]:
    """Consolidate a group of identical files by creating the most general name.
    
    Parameters
    ----------
    identical_files : List[Path]
        List of identical file paths
    
    Returns
    -------
    Optional[Path]
        Path to the retained file, None if consolidation failed
    """
    if len(identical_files) <= 1:
        return identical_files[0] if identical_files else None
    
    filenames = [f.name for f in identical_files]
    most_general_name = create_most_general_name(filenames)
    most_general_path = identical_files[0].parent / most_general_name
    
    # Check if the generalized file already exists among the identical files
    existing_general = None
    for f in identical_files:
        if f.name == most_general_name:
            existing_general = f
            break
    
    if existing_general:
        # The generalized name already exists, just delete the others
        most_general_path = existing_general
    else:
        # Create the new generalized file
        try:
            shutil.copy2(identical_files[0], most_general_path)
            logger.debug(f"    Created consolidated file: {most_general_path.name}")
        except Exception as err:
            logger.warning(f"Failed to create consolidated file {most_general_path.name}: {err}")
            return None
    
    # Delete all other files
    for file_path in identical_files:
        if file_path != most_general_path:
            try:
                file_path.unlink()
                logger.debug(f"    Deleted redundant file: {file_path.name}")
            except Exception as err:
                logger.warning(f"Failed to delete {file_path.name}: {err}")
    
    return most_general_path


def consolidate_channels_metadata(bids_root: Path, participant_labels: List[str], session: Optional[str] = None) -> None:
    """Consolidate identical *channels.tsv files in MEG datasets.
    
    Implements BIDS inheritance principle by removing redundant metadata files.
    Only removes run numbers when files of the same task are identical.
    Only processes files within each subject/session folder independently.
    
    Parameters
    ----------
    bids_root : Path
        BIDS root directory
    participant_labels : List[str]
        List of participant IDs (without 'sub-' prefix) to process
    session : Optional[str]
        Session label (without 'ses-' prefix), if None processes all sessions
    """
    # Build list of MEG directories to process based on participant labels
    meg_dirs = []
    for participant_id in participant_labels:
        if session:
            # Specific session
            meg_dir = bids_root / f"sub-{participant_id}" / f"ses-{session}" / "meg"
            if meg_dir.exists():
                meg_dirs.append(meg_dir)
        else:
            # Check for session structure
            subject_dir = bids_root / f"sub-{participant_id}"
            if subject_dir.exists():
                # Check if there are sessions
                session_dirs = list(subject_dir.glob("ses-*"))
                if session_dirs:
                    # Process each session
                    for ses_dir in session_dirs:
                        meg_dir = ses_dir / "meg"
                        if meg_dir.exists():
                            meg_dirs.append(meg_dir)
                else:
                    # No sessions, check for direct meg folder
                    meg_dir = subject_dir / "meg"
                    if meg_dir.exists():
                        meg_dirs.append(meg_dir)
    
    if not meg_dirs:
        logger.info("No MEG directories found for consolidation")
        return
    
    total_consolidated = 0
    total_deleted = 0
    
    # Process each MEG directory independently
    for meg_dir in meg_dirs:
        channels_files = list(meg_dir.glob("*channels.tsv"))
        
        if len(channels_files) <= 1:
            continue
        
        logger.debug(f"Processing {meg_dir.relative_to(bids_root)}: {len(channels_files)} channels.tsv file(s)")
        
        # Group files by task
        task_groups = defaultdict(list)
        for file_path in channels_files:
            entities = extract_bids_entities(file_path.name)
            task = entities.get('task', 'no-task')
            task_groups[task].append(file_path)
        
        # Within each task, find identical files
        for task, task_files in task_groups.items():
            if len(task_files) <= 1:
                continue
            
            logger.debug(f"  Checking task-{task}: {len(task_files)} file(s)")
            
            # Find all groups of identical files within this task
            processed = set()
            for i, file1 in enumerate(task_files):
                if file1 in processed:
                    continue
                
                # Find all files identical to file1
                identical_group = [file1]
                for file2 in task_files[i+1:]:
                    if file2 not in processed and compare_tsv_files(file1, file2):
                        identical_group.append(file2)
                
                # If we found identical files, consolidate them
                if len(identical_group) > 1:
                    logger.info(f"  Found {len(identical_group)} identical files for task-{task} in {meg_dir.relative_to(bids_root)}:")
                    for f in identical_group:
                        logger.info(f"    - {f.name}")
                    
                    retained = consolidate_identical_group(identical_group)
                    if retained:
                        logger.info(f"  → Consolidated to: {retained.name}")
                        total_consolidated += 1
                        total_deleted += len(identical_group) - 1
                        processed.update(identical_group)
    
    if total_consolidated > 0:
        logger.info(f"Metadata consolidation complete:")
        logger.info(f"  Groups consolidated: {total_consolidated}")
        logger.info(f"  Files deleted: {total_deleted}")
    else:
        logger.info("Metadata consolidation: No identical files found")


def import_meg(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    derivatives_dir: Optional[Path] = None,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None
) -> bool:
    """Import MEG data to BIDS format.
    
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
    derivatives_dir : Optional[Path]
        Path to derivatives directory (for MaxFilter outputs)
    ds_initials : Optional[str]
        Dataset initials prefix (not used for MEG, kept for API consistency)
    session : Optional[str]
        Session label (without 'ses-' prefix) - if None, auto-detects sessions
    
    Returns
    -------
    bool
        True if import successful for at least one participant, False otherwise
    """
    if not MNE_AVAILABLE:
        logger.error("MNE-Python and mne-bids are required for MEG import")
        logger.error("Install with: pip install mne mne-bids")
        return False
    
    logger.info(f"Starting MEG import for dataset: {dataset}")
    
    # Load configuration
    try:
        config = load_meg_config(None, sourcedata_dir)
    except Exception as e:
        logger.error(f"Failed to load MEG configuration: {e}")
        return False
    
    # Load participants mapping
    participants_file = sourcedata_dir / "participants_complete.tsv"
    try:
        participants_map = load_participants_mapping(participants_file)
    except Exception as e:
        logger.error(f"Failed to load participants mapping: {e}")
        return False
    
    # MEG source directory
    meg_source_dir = sourcedata_dir / "meg"
    if not meg_source_dir.exists():
        logger.error(f"MEG source directory not found: {meg_source_dir}")
        return False
    
    # Get configuration options
    file_patterns = config.get('file_patterns', [])
    calibration_system = config.get('calibration', {}).get('system', 'triux').lower()
    calibration_auto_detect = config.get('calibration', {}).get('auto_detect', True)
    meg_maxfilter_root = config.get('calibration', {}).get('maxfilter_root')
    if meg_maxfilter_root:
        # Expand env vars and user home (e.g., $HOME/MEG/maxfilter)
        meg_maxfilter_root = Path(os.path.expandvars(os.path.expanduser(str(meg_maxfilter_root)))).resolve()
        if not meg_maxfilter_root.exists():
            logger.warning(f"MaxFilter root not found: {meg_maxfilter_root}")
            meg_maxfilter_root = None
    
    # Track success/failure
    success_count = 0
    failed_participants = []
    
    # Process each participant
    for participant_label in participant_labels:
        participant_id = participant_label.replace('sub-', '')
        bids_subject = f"sub-{participant_id}"
        
        logger.info(f"{'='*60}")
        logger.info(f"Processing participant: {bids_subject}")
        logger.info(f"{'='*60}")
        
        # Find meg_id for this participant
        meg_id = None
        for mid, subj in participants_map.items():
            if subj == bids_subject or subj == participant_id:
                meg_id = mid
                break
        
        if not meg_id:
            logger.error(f"  ✗ No meg_id found for {bids_subject} in participants file")
            failed_participants.append(participant_id)
            continue
        
        # Find MEG folder (supports meg_XXXX or XXXX_meg naming)
        meg_folder = find_meg_folder(meg_source_dir, meg_id)
        if not meg_folder:
            logger.error(f"  ✗ MEG folder not found for meg_id {meg_id} (tried meg_{meg_id}, {meg_id}_meg, or folder containing {meg_id})")
            failed_participants.append(participant_id)
            continue
        
        logger.info(f"  MEG folder: {meg_folder.name}")
        
        # Auto-detect sessions within this MEG folder
        sessions = auto_detect_sessions(meg_folder)
        if not sessions:
            logger.warning(f"  ⚠ No session directories found in {meg_folder}")
            failed_participants.append(participant_id)
            continue
        
        participant_success = False
        stats = ConversionStats()
        
        # Process each session
        for folder_name, session_id in sessions:
            # If user specified a session, only process that one
            if session and session_id and session != session_id:
                continue
            if session and not session_id:
                # Single session case - process it
                pass
            
            sess_dir = meg_folder / folder_name
            
            logger.info(f"{'-'*60}")
            if session_id:
                logger.info(f"SESSION: {folder_name} (ses-{session_id})")
            else:
                logger.info(f"SESSION: {folder_name}")
            logger.info(f"{'-'*60}")
            
            # Find all FIF files
            all_fif_files = sorted(sess_dir.glob("*.fif"))
            if not all_fif_files:
                logger.warning("  ⚠ No FIF files found in this session")
                continue
            
            # Separate raw from derivatives
            raw_files = []
            derivative_files = []
            for fif_file in all_fif_files:
                if extract_derivative_info(fif_file.name) is None:
                    raw_files.append(fif_file)
                else:
                    derivative_files.append(fif_file)
            
            logger.info(f"Found {len(all_fif_files)} FIF file(s) ({len(raw_files)} raw, {len(derivative_files)} derivatives)")
            
            # Detect calibration files
            if calibration_auto_detect:
                logger.info("Detecting calibration files...")
                calib_files = detect_calibration_files(
                    meg_folder, folder_name, meg_maxfilter_root, calibration_system
                )
                
                if calib_files['crosstalk'] or calib_files['calibration']:
                    copy_calibration_files(
                        calib_files, participant_id, session_id, rawdata_dir
                    )
                else:
                    logger.info("  ℹ No calibration files found")
            
            # Detect split files
            split_file_groups = detect_split_files(raw_files)
            if split_file_groups:
                logger.info(f"Detected {len(split_file_groups)} split file group(s)")
            
            # Get primary files only (exclude split parts)
            split_parts = set()
            for primary_file, parts in split_file_groups.items():
                split_parts.update(parts[1:])
            primary_raw_files = [f for f in raw_files if f not in split_parts]
            
            # Match files to patterns and assign run numbers
            logger.info("Matching files to patterns...")
            file_mapping = {}
            for fif_file in primary_raw_files:
                pattern_rule = match_file_pattern(fif_file.name, file_patterns)
                if pattern_rule:
                    task = pattern_rule.get('task', 'unknown')
                    run_extraction = pattern_rule.get('run_extraction', 'last_digits')
                    run = extract_run_from_filename(fif_file.name, run_extraction)
                    file_mapping[fif_file] = (task, run, pattern_rule)
                    logger.debug(f"  {fif_file.name} -> task={task}, run={run}")
                else:
                    logger.warning(f"  ⊘ No matching pattern for: {fif_file.name}")
            
            # Group by task and assign run numbers if needed
            task_files = defaultdict(list)
            for fif_path, (task, run, pattern_rule) in file_mapping.items():
                task_files[task].append((fif_path, run))
            
            # Reassign run numbers if multiple files per task
            final_mapping = {}
            for task, files in task_files.items():
                if len(files) == 1:
                    fif_path, _ = files[0]
                    final_mapping[fif_path] = (task, None)
                else:
                    # Sort by run number from filename, then by filename
                    sorted_files = sorted(files, key=lambda x: (x[1] if x[1] is not None else float('inf'), x[0].name))
                    for idx, (fif_path, _) in enumerate(sorted_files, start=1):
                        final_mapping[fif_path] = (task, idx)
            
            # Convert files
            logger.info("Converting files...")
            for fif_path in primary_raw_files:
                if fif_path not in final_mapping:
                    stats.add_file('unknown', 'skipped', fif_path.name)
                    continue
                
                task, run = final_mapping[fif_path]
                split_parts_for_file = split_file_groups.get(fif_path, None)
                
                success = convert_raw_file(
                    fif_path, participant_id, session_id, task, run,
                    config, rawdata_dir, split_parts_for_file
                )
                
                if success:
                    stats.add_file(task, 'converted', fif_path.name)
                    participant_success = True
                else:
                    stats.add_file(task, 'failed', fif_path.name)
            
            # Process derivative files if configured
            if derivative_files and derivatives_dir:
                pipeline_name = config.get('derivatives', {}).get('pipeline_name', 'maxfilter')
                pipeline_version = config.get('derivatives', {}).get('maxfilter_version')
                
                if pipeline_name and pipeline_name.lower() != 'none':
                    logger.info(f"Converting {len(derivative_files)} derivative file(s)...")
                    
                    # Detect split files in derivatives (special handling for proc suffixes)
                    deriv_split_groups = detect_derivative_split_files(derivative_files)
                    if deriv_split_groups:
                        logger.info(f"Detected {len(deriv_split_groups)} derivative split file group(s)")
                    
                    deriv_split_parts = set()
                    for primary_file, parts in deriv_split_groups.items():
                        deriv_split_parts.update(parts[1:])
                    primary_deriv_files = [f for f in derivative_files if f not in deriv_split_parts]
                    
                    # Build mapping of task -> raw file organization (splits or runs)
                    task_organization = {}  # task -> {'type': 'splits'/'runs', 'base_names': set()}
                    for raw_path, (task, run) in final_mapping.items():
                        if task not in task_organization:
                            task_organization[task] = {'base_names': set(), 'has_splits': False, 'has_runs': False}
                        
                        # Check if this raw file was part of a split group
                        if raw_path in split_file_groups:
                            task_organization[task]['has_splits'] = True
                            # Extract base name without extension or split suffix
                            base_name = raw_path.stem.split('-')[0]  # NAP from NAP.fif or NAP-1.fif
                            task_organization[task]['base_names'].add(base_name)
                        elif run is not None:
                            task_organization[task]['has_runs'] = True
                    
                    # Process each primary derivative file
                    for deriv_file in primary_deriv_files:
                        deriv_info = extract_derivative_info(deriv_file.name)
                        if deriv_info is None:
                            continue
                        
                        base_filename, proc_label = deriv_info
                        
                        # Try to match derivative to a raw file task/run
                        pattern_rule = match_file_pattern(deriv_file.name, file_patterns)
                        if pattern_rule:
                            task = pattern_rule.get('task', 'unknown')
                            run_extraction = pattern_rule.get('run_extraction', 'last_digits')
                            
                            # Determine if this derivative is a split or a run based on raw file organization
                            run = None
                            is_split = False
                            split_parts_for_deriv = deriv_split_groups.get(deriv_file, None)
                            
                            if task in task_organization:
                                # Extract base name from derivative by removing hyphen-suffix (splits only use hyphens)
                                # NAP from NAP_tsss_mc.fif or NAP-1_tsss_mc.fif
                                # RS_1 from RS_1_tsss_mc.fif (keep underscore, that's a run indicator)
                                deriv_base = base_filename.split('-')[0]
                                
                                # Check if corresponding raw file was a split
                                if (task_organization[task]['has_splits'] and 
                                    deriv_base in task_organization[task]['base_names']):
                                    # This is a split file group, not different runs
                                    is_split = True
                                    run = None
                                else:
                                    # This is a run-based organization
                                    run = extract_run_from_filename(deriv_file.name, run_extraction)
                            else:
                                # No corresponding raw file info, use default extraction
                                run = extract_run_from_filename(deriv_file.name, run_extraction)
                            
                            # Format log message
                            if is_split and split_parts_for_deriv:
                                num_parts = len(split_parts_for_deriv)
                                run_str = f" ({num_parts} parts)"
                            elif run is not None:
                                run_str = f" (run {run})"
                            else:
                                run_str = ""
                            
                            logger.info(f"  ✓ Converting: {deriv_file.name}{run_str} -> task={task} (proc-{proc_label})")
                            
                            success = copy_derivative_file(
                                deriv_file, participant_id, session_id, task, run,
                                deriv_info, derivatives_dir, pipeline_name, pipeline_version,
                                split_parts=split_parts_for_deriv
                            )
                            
                            if not success:
                                logger.warning(f"    ✗ Copy failed for {deriv_file.name}")
                        else:
                            logger.warning(f"  ⊘ No pattern match for derivative: {deriv_file.name}")
        
        # Summary for this participant
        logger.info(f"{'-'*60}")
        logger.info(f"Summary for {bids_subject}:")
        logger.info(f"  Total files: {stats.total_files}")
        logger.info(f"  ✓ Converted: {stats.converted}")
        logger.info(f"  ⊘ Skipped:   {stats.skipped}")
        logger.info(f"  ✗ Failed:    {stats.failed}")
        if stats.task_counts:
            logger.info("  Files by task:")
            for task in sorted(stats.task_counts.keys()):
                logger.info(f"    • task-{task}: {stats.task_counts[task]} file(s)")
        logger.info(f"{'-'*60}")
        
        if participant_success:
            success_count += 1
        else:
            failed_participants.append(participant_id)
    
    # Overall summary
    logger.info(f"{'='*60}")
    logger.info("MEG Import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    # Consolidate identical channels.tsv files using BIDS inheritance principle
    logger.info("Consolidating metadata files using BIDS inheritance principle...")
    consolidate_channels_metadata(rawdata_dir, participant_labels, session)
    
    return success_count > 0
