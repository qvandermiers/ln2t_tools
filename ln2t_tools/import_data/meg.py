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
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, Set
from collections import defaultdict
from datetime import datetime, date, timezone
import warnings

logger = logging.getLogger(__name__)

# Suppress verbose mne and mne_bids output
try:
    import mne  # type: ignore
    from mne_bids import write_raw_bids, BIDSPath  # type: ignore
    import mne_bids.write  # type: ignore  # For patching FIFF split size
    mne.set_log_level('ERROR')
    logging.getLogger('mne_bids').setLevel(logging.ERROR)
    logging.getLogger('mne').setLevel(logging.ERROR)
    warnings.filterwarnings('ignore', category=DeprecationWarning, module='mne')
    warnings.filterwarnings('ignore', message='.*headshape.*', category=UserWarning)
    warnings.filterwarnings('ignore', message='.*does not conform to MNE naming conventions.*')
    warnings.filterwarnings('ignore', message='.*raw Internal Active Shielding data.*')
    warnings.filterwarnings('ignore', message='.*No events found or provided.*')
    MNE_AVAILABLE = True
except ImportError:
    MNE_AVAILABLE = False
    logger.warning("MNE-Python and mne-bids not available. Install with: pip install mne mne-bids")


def prompt_user_for_duplicate(files: List[Path]) -> Path:
    """Prompt user to choose between duplicate files.
    
    Only prompts if stdin is a TTY (interactive environment).
    Falls back silently to first file if not interactive.
    
    Parameters
    ----------
    files : List[Path]
        List of duplicate file paths (should have 2+ files)
    
    Returns
    -------
    Path
        The file chosen by the user, or first file if non-interactive
    """
    # Check if stdin is a TTY (interactive terminal)
    if not sys.stdin.isatty():
        logger.debug(f"Non-interactive environment: silently choosing {files[0].name}")
        return files[0]
    
    # Display options
    print(f"\n⚠️  Duplicate files with same preference level found:")
    for idx, fpath in enumerate(files, 1):
        print(f"  {idx}. {fpath.name}")
    
    # Loop until valid selection
    while True:
        try:
            response = input(f"\nWhich file to keep? Enter 1-{len(files)}: ").strip()
            choice = int(response)
            if 1 <= choice <= len(files):
                selected = files[choice - 1]
                print(f"✓ Selected: {selected.name}\n")
                return selected
            else:
                print(f"Invalid choice. Please enter 1-{len(files)}.")
        except ValueError:
            print(f"Invalid input. Please enter a number 1-{len(files)}.")


def get_fif_header_info(file_path: Path) -> Optional[Dict[str, Any]]:
    """Extract metadata from FIF file header without loading full data.
    
    Reads only the FIF header to extract:
    - file_id: Machine/system identifier for split file detection
    - meas_date: Recording timestamp
    - n_samples: Total samples in file
    - sfreq: Sampling frequency
    - n_channels: Number of channels
    
    This is fast (header only) and helps identify genuine split files vs duplicates.
    
    Parameters
    ----------
    file_path : Path
        Path to FIF file
    
    Returns
    -------
    Optional[Dict[str, Any]]
        Dictionary with header info, or None if read failed
    """
    if not MNE_AVAILABLE:
        return None
    
    try:
        raw = mne.io.read_raw_fif(file_path, preload=False, allow_maxshield=True, verbose=False)
        
        file_id = raw.info.get('file_id', {})
        meas_date = raw.info.get('meas_date')
        
        # Convert datetime to string for hashing
        meas_date_str = None
        if meas_date:
            if isinstance(meas_date, datetime):
                meas_date_str = meas_date.isoformat()
            elif isinstance(meas_date, date):
                meas_date_str = meas_date.isoformat()
        
        # Extract split structure info (first_samps and last_samps)
        first_samps = getattr(raw, '_first_samps', None)
        last_samps = getattr(raw, '_last_samps', None)
        
        return {
            'file_id': file_id,
            'meas_date': meas_date_str,
            'n_samples': raw.n_times,
            'sfreq': raw.info.get('sfreq'),
            'n_channels': len(raw.ch_names),
            'duration_sec': raw.times[-1] if len(raw.times) > 0 else 0,
            'first_samps': first_samps,
            'last_samps': last_samps,
            'is_primary': first_samps is not None and len(first_samps) > 1,
            'n_parts': len(first_samps) if first_samps is not None else 1,
        }
    except Exception as e:
        logger.warning(f"Failed to read FIF header from {file_path.name}: {e}")
        return None


def inspect_fif_header(fif_path: Path, verbose: bool = True) -> Optional[Dict[str, Any]]:
    """Inspect and display detailed FIF header information for debugging.
    
    Shows all relevant metadata for understanding split files and duplicates:
    - file_id fields (version, machine_id, secs, usecs) - SAME for split parts
    - Duration and samples - DIFFERENT for each split part
    - Whether file is detected as part of split series
    
    Parameters
    ----------
    fif_path : Path
        Path to FIF file
    verbose : bool
        If True, print detailed information to logger
    
    Returns
    -------
    Optional[Dict[str, Any]]
        Dictionary with detailed header information, or None if read failed
    """
    if not MNE_AVAILABLE:
        logger.warning(f"MNE not available - cannot inspect {fif_path.name}")
        return None
    
    try:
        raw = mne.io.read_raw_fif(fif_path, preload=False, allow_maxshield=True, verbose=False)
        
        file_id = raw.info.get('file_id', {})
        meas_date = raw.info.get('meas_date')
        
        result = {
            'filename': fif_path.name,
            'meas_date': str(meas_date) if meas_date else None,
            'duration_sec': raw.times[-1] if len(raw.times) > 0 else 0,
            'n_samples': raw.n_times,
            'n_channels': len(raw.ch_names),
            'sfreq': raw.info.get('sfreq'),
            'file_id': file_id,
        }
        
        if verbose:
            logger.info(f"FIF Header: {fif_path.name}")
            logger.info(f"  Meas date: {result['meas_date']}")
            logger.info(f"  Duration: {result['duration_sec']:.1f}s")
            logger.info(f"  Samples: {result['n_samples']:,}")
            logger.info(f"  Channels: {result['n_channels']}")
            logger.info(f"  Sfreq: {result['sfreq']} Hz")
            
            if file_id:
                logger.info(f"  File ID:")
                for key, value in file_id.items():
                    logger.info(f"    {key}: {value}")
            
            # Check for split part information stored by MNE
            if hasattr(raw, '_first_samps') and hasattr(raw, '_last_samps'):
                logger.info(f"  Split info (internal):")
                logger.info(f"    First samples: {raw._first_samps}")
                logger.info(f"    Last samples: {raw._last_samps}")
        
        return result
        
    except Exception as e:
        if verbose:
            logger.warning(f"Failed to inspect FIF header from {fif_path.name}: {e}")
        return None


def identify_primary_files(fif_files: List[Path], interactive: bool = False) -> Tuple[List[Path], int]:
    """Identify and filter FIF files by separating PRIMARY, SECONDARY, and STANDALONE.
    
    Algorithm - Three-phase approach:
    
    PHASE 1: Classify files by split structure
    - PRIMARY: len(first_samps) > 1 (split group headers)
    - OTHER: len(first_samps) == 1 (SECONDARY or STANDALONE)
    
    PHASE 2: Process PRIMARY files
    - Group by fingerprint (meas_date, first_samps_tuple)
    - For each group: keep preferred suffix variant (underscore > dash > none)
    - SECONDARY files linked to excluded PRIMARY are also excluded
    
    PHASE 3: Process STANDALONE files (not linked to any PRIMARY)
    - Group by fingerprint (meas_date, first_samps_tuple)
    - For each group: keep preferred suffix variant (underscore > dash > none)
    
    Result: PRIMARY (kept) + STANDALONE (kept) + SECONDARY (all kept PRIMARY's parts)
    
    Parameters
    ----------
    fif_files : List[Path]
        List of FIF file paths
    interactive : bool
        If True, prompt user when duplicate files have same preference level
    List[Path]
        List of files to process (duplicates removed, relationships preserved)
    """
    if not MNE_AVAILABLE:
        logger.warning("MNE not available, cannot identify primary files intelligently. Keeping all files.")
        return list(fif_files), 0
    
    # Read headers for all files
    file_headers = {}
    for fif_file in fif_files:
        header_info = get_fif_header_info(fif_file)
        if header_info:
            file_headers[fif_file] = header_info
        else:
            # If we can't read header, keep the file
            file_headers[fif_file] = None
    
    # Helper: Extract suffix type from filename
    def get_filename_suffix_type(filename: str) -> str:
        """Return suffix type: 'underscore', 'dash', or 'none'."""
        stem = filename[:-4] if filename.endswith('.fif') else filename
        
        # Match underscore suffix: base_N
        if re.search(r'^.+_\d+$', stem):
            return 'underscore'
        
        # Match dash suffix: base-N
        if re.search(r'^.+?-\d+$', stem):
            return 'dash'
        
        # No numeric suffix
        return 'none'
    
    # Helper: Create fingerprint from file metadata
    def get_fingerprint(file_path: Path) -> tuple:
        """Fingerprint = (meas_date, first_samps_tuple)."""
        hdr = file_headers.get(file_path)
        if not hdr:
            return ('unknown', file_path.name)
        
        first_samps = hdr['first_samps']
        first_samp_tuple = tuple(int(s) for s in first_samps) if first_samps is not None else ()
        meas_date = hdr['meas_date'] or 'unknown'
        
        return (meas_date, first_samp_tuple)
    
    # Helper: Get preference level for a file
    def get_preference_level(file_path: Path) -> int:
        """Return preference level: 0 (underscore) > 1 (dash) > 2 (none)."""
        suffix_type = get_filename_suffix_type(file_path.name)
        if suffix_type == 'underscore':
            return 0
        elif suffix_type == 'dash':
            return 1
        else:
            return 2
    
    # Helper: Process a group of files with same fingerprint
    def process_duplicate_group(files_in_group: List[Path], interactive: bool = False) -> Path:
        """Select best file from duplicates (underscore > dash > none).
        
        If interactive=True and files have same preference level, prompts user to choose.
        """
        if len(files_in_group) == 1:
            return files_in_group[0]
        
        files_sorted = sorted(
            files_in_group,
            key=lambda f: (get_preference_level(f), f.name)
        )
        
        # Check if top 2 files have same preference level
        if len(files_sorted) >= 2:
            level_1 = get_preference_level(files_sorted[0])
            level_2 = get_preference_level(files_sorted[1])
            
            # If same preference level and interactive mode, ask user
            if level_1 == level_2 and interactive:
                canonical = prompt_user_for_duplicate(files_sorted)
                logger.info(f"  → (user selected) {canonical.name} from {len(files_sorted)} duplicate(s)")
                return canonical
        
        canonical = files_sorted[0]
        
        # Log duplicates in clean format: kept <-> excluded
        if len(files_sorted) > 1:
            kept = files_sorted[0].name
            for excluded_file in files_sorted[1:]:
                logger.info(f"  → {kept} <-> {excluded_file.name}")
        
        return canonical
    
    # Helper: Parse filename to extract base name, number, and separator
    def parse_filename_with_number(filename: str) -> tuple:
        """
        Parse filename like 'base_1.fif' or 'base-2.fif'
        Returns: (base_name, number, separator) or (None, None, None) if no number found
        E.g., 'MEG_4157_RestEyesClosed-2.fif' -> ('MEG_4157_RestEyesClosed', 2, '-')
        """
        stem = filename[:-4] if filename.endswith('.fif') else filename
        
        # Try matching underscore: base_N
        match = re.search(r'^(.+)_(\d+)$', stem)
        if match:
            return (match.group(1), int(match.group(2)), '_')
        
        # Try matching dash: base-N
        match = re.search(r'^(.+?)-([\d]+)$', stem)
        if match:
            return (match.group(1), int(match.group(2)), '-')
        
        return (None, None, None)
    
    # Helper: Check if a file matches an excluded PRIMARY by filename pattern
    def matches_excluded_primary(file_path: Path, excluded_primary_path: Path) -> bool:
        """
        Check if file matches excluded PRIMARY by filename pattern.
        E.g., if excluding 'base-2.fif', files 'base-1.fif', 'base-3.fif', etc. match if same base/separator.
        Also validate: same meas_date as excluded PRIMARY.
        """
        base_excl, num_excl, sep_excl = parse_filename_with_number(excluded_primary_path.name)
        base_file, num_file, sep_file = parse_filename_with_number(file_path.name)
        
        # Must have parsed successfully
        if base_excl is None or base_file is None:
            return False
        
        # Must have same base name and separator
        if base_excl != base_file or sep_excl != sep_file:
            return False
        
        # Check meas_date match
        hdr_excl = file_headers.get(excluded_primary_path)
        hdr_file = file_headers.get(file_path)
        
        if not hdr_excl or not hdr_file:
            return False
        
        # Must have same measurement date
        if hdr_excl['meas_date'] != hdr_file['meas_date']:
            return False
        
        # Match any numbered file with same base/separator/date
        # (all parts of the same split group)
        return True
    
    logger.info("Identifying duplicate files...")
    
    # ========== PHASE 1: Classify files by split structure ==========
    primary_files = []
    other_files = []
    
    for fif_file in sorted(fif_files):
        hdr = file_headers.get(fif_file)
        if hdr and hdr['first_samps'] is not None and len(hdr['first_samps']) > 1:
            primary_files.append(fif_file)
        else:
            other_files.append(fif_file)
    
    # ========== PHASE 2: Process PRIMARY files ==========
    # For PRIMARY files, duplicates are identified by:
    # - Same meas_date AND
    # - Same first_samps[0] (the main recording start point)
    # We DON'T compare the entire first_samps array because split structure may vary
    primary_by_fp = defaultdict(list)
    for fif_file in primary_files:
        hdr = file_headers.get(fif_file)
        if hdr and hdr['first_samps'] is not None and len(hdr['first_samps']) > 0 and hdr['meas_date']:
            # For PRIMARY, fingerprint = (meas_date, first_samps[0] only)
            first_samp = int(hdr['first_samps'][0])
            meas_date = hdr['meas_date']
            fp = (meas_date, first_samp)  # Only first element for PRIMARY
        else:
            # No valid header, use filename as fallback
            fp = ('unknown', fif_file.name)
        primary_by_fp[fp].append(fif_file)
    
    kept_primary_files = set()  # Use set to track by path
    excluded_primary_files = set()  # Use set to track excluded PRIMARY files
    
    for fp, files_in_group in primary_by_fp.items():
        canonical = process_duplicate_group(files_in_group, interactive=interactive)
        kept_primary_files.add(canonical)
        # Track files that were excluded from this group
        for fif_file in files_in_group:
            if fif_file != canonical:
                excluded_primary_files.add(fif_file)
    

    
    # ========== PHASE 3: Link SECONDARY files to KEPT PRIMARY ==========
    # Kept PRIMARY files are form: base_X.fif (underscore)
    # Their SECONDARY files are form: base_X-N.fif (dash pattern)
    # SECONDARY candidate = len(first_samps) == 1 with valid meas_date
    # - If matches kept PRIMARY (same base, dash pattern, same date) → KEPT
    # - If duplicate of kept SECONDARY (same first_samps[0] + meas_date) → EXCLUDED
    # - Otherwise → STANDALONE
    # Non-SECONDARY files → STANDALONE
    
    # Build map of kept PRIMARY files: base_stem -> meas_date
    kept_primary_map = {}
    for kept_primary in kept_primary_files:
        hdr = file_headers.get(kept_primary)
        if hdr and hdr['meas_date']:
            stem = kept_primary.name[:-4]  # Remove .fif extension
            kept_primary_map[stem] = hdr['meas_date']
    
    secondary_files = []
    standalone_files = []
    excluded_secondary_files = []
    
    # First pass: identify SECONDARY files linked to kept PRIMARY
    for fif_file in other_files:
        hdr = file_headers.get(fif_file)
        
        # Check if this is a SECONDARY candidate
        is_secondary_candidate = (hdr and hdr['meas_date'] and 
                                 hdr['first_samps'] is not None and 
                                 len(hdr['first_samps']) == 1)
        
        if not is_secondary_candidate:
            # Not a SECONDARY candidate, treat as STANDALONE
            standalone_files.append(fif_file)
            continue
        
        # It's a SECONDARY candidate - check if matches a kept PRIMARY
        matched_to_kept = False
        
        # Parse filename to check for dash pattern
        base_file, num_file, sep_file = parse_filename_with_number(fif_file.name)
        
        if base_file and sep_file == '-':
            for primary_stem, primary_meas_date in kept_primary_map.items():
                # Parse the kept primary's stem
                primary_base, primary_num, primary_sep = parse_filename_with_number(primary_stem + ".fif")
                
                # Match if: same base + primary has underscore + file has dash + same meas_date
                if (base_file == primary_base and 
                    primary_sep == '_' and 
                    hdr and hdr['meas_date'] == primary_meas_date):
                    secondary_files.append(fif_file)
                    logger.debug(f"  SECONDARY (kept): {fif_file.name} -> linked to {primary_stem}.fif")
                    matched_to_kept = True
                    break
        
        # If not linked to a kept PRIMARY, it will be checked for duplicates in second pass
        if not matched_to_kept:
            standalone_files.append(fif_file)  # Tentatively treat as standalone
    
    # Second pass: among tentatively-standalone SECONDARY candidates, find duplicates of kept SECONDARY files
    # A duplicate SECONDARY has same first_samps[0] and meas_date as a kept SECONDARY
    for fif_file in list(standalone_files):
        hdr = file_headers.get(fif_file)
        
        # Check if this is a SECONDARY candidate
        is_secondary_candidate = (hdr and hdr['meas_date'] and 
                                 hdr['first_samps'] is not None and 
                                 len(hdr['first_samps']) == 1)
        
        if not is_secondary_candidate or not hdr:
            continue  # Not a SECONDARY candidate, keep as standalone
        
        # Check if it's a duplicate of any kept SECONDARY
        first_samp = int(hdr['first_samps'][0])
        meas_date = hdr['meas_date']
        candidate_fp = (first_samp, meas_date)
        
        is_duplicate = False
        for kept_sec in secondary_files:
            kept_hdr = file_headers.get(kept_sec)
            if kept_hdr and kept_hdr['first_samps'] is not None and len(kept_hdr['first_samps']) == 1:
                kept_first_samp = int(kept_hdr['first_samps'][0])
                kept_meas_date = kept_hdr['meas_date']
                kept_fp = (kept_first_samp, kept_meas_date)
                
                if candidate_fp == kept_fp:
                    is_duplicate = True
                    break
        
        if is_duplicate:
            # Move from standalone to excluded_secondary
            standalone_files.remove(fif_file)
            excluded_secondary_files.append(fif_file)
            logger.debug(f"  SECONDARY (excluded - duplicate): {fif_file.name}")
    

    
    # ========== PHASE 4: Process STANDALONE files ==========
    standalone_by_fp = defaultdict(list)
    for fif_file in standalone_files:
        fp = get_fingerprint(fif_file)
        standalone_by_fp[fp].append(fif_file)
    
    kept_standalone_files = []
    
    for fp, files_in_group in standalone_by_fp.items():
        canonical = process_duplicate_group(files_in_group, interactive=interactive)
        kept_standalone_files.append(canonical)
    

    
    # ========== PHASE 5: Combine results ==========
    # Result = PRIMARY (kept) + STANDALONE (kept)
    # SECONDARY files are NOT converted (only used for linked detection)
    # EXCLUDED: PRIMARY (duplicates), SECONDARY (all), STANDALONE (duplicates)
    files_to_keep = list(kept_primary_files) + kept_standalone_files
    
    # Count split groups based on first_samps metadata (kept PRIMARY files only)
    split_group_count = sum(1 for kp in kept_primary_files 
                           if (file_headers.get(kp, {}).get('first_samps') is not None 
                           and len(file_headers.get(kp, {}).get('first_samps', [])) > 1))
    
    return files_to_keep, split_group_count


class ConversionStats:
    """Track conversion statistics for reporting."""
    
    def __init__(self):
        self.total_files = 0
        self.converted = 0
        self.skipped = 0
        self.excluded = 0
        self.failed = 0
        self.task_counts = defaultdict(int)
        self.failed_files = []
    
    def add_file(self, task: str, status: str, filename: str = ""):
        """Record a file conversion.
        
        Parameters
        ----------
        task : str
            Task name associated with the file
        status : str
            File status: 'converted', 'skipped', 'excluded', or 'failed'
        filename : str
            Optional filename for logging/tracking
        """
        self.total_files += 1
        if status == 'converted':
            self.converted += 1
            self.task_counts[task] += 1
        elif status == 'skipped':
            self.skipped += 1
        elif status == 'excluded':
            self.excluded += 1
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
        Configuration dictionary with optional 'exclude_patterns' field
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
    
    # Ensure optional exclude_patterns is a list (defaults to empty if missing)
    if 'exclude_patterns' not in config:
        config['exclude_patterns'] = []
    elif not isinstance(config['exclude_patterns'], list):
        raise ValueError("'exclude_patterns' must be a list of wildcard patterns")
    
    if config['exclude_patterns']:
        logger.info(f"Loaded MEG configuration from {config_path} (exclude_patterns: {len(config['exclude_patterns'])} pattern(s))")
    else:
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


def _extract_base_name_and_suffix(filename: str, with_proc: bool = False) -> Tuple[str, Optional[str]]:
    """Extract base name and optional proc suffix from a filename.
    
    Used by split detection to group files by their base name and processing label.
    
    Parameters
    ----------
    filename : str
        FIF filename
    with_proc : bool
        If True, also extract proc label (e.g., "tsss-mc"). If False, ignore proc labels.
    
    Returns
    -------
    Tuple[str, Optional[str]]
        (base_name, proc_label) where proc_label is None if with_proc=False
    """
    stem = Path(filename).stem
    
    # FIRST: Remove trailing split suffix if present (e.g., _sss-2)
    # This is for Pattern 2: base_sss-2.fif
    split_after_proc = re.match(r'^(.+)-\d+$', stem)
    if split_after_proc:
        stem = split_after_proc.group(1)
    
    if not with_proc:
        # Also remove leading split suffix if present (e.g., -1_sss becomes just sss part)
        # This is for Pattern 1: base-1_sss.fif
        split_before_proc = re.match(r'^(.+?)-\d+(.*)$', stem)
        if split_before_proc:
            stem = split_before_proc.group(1) + split_before_proc.group(2)
        return (stem, None)
    
    # Extract proc label
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
    
    # FINALLY: Remove any remaining leading split suffix (e.g., -1 or -2)
    # This is for Pattern 1: base-1_sss.fif where base_name is extracted after removing _sss
    split_before_proc = re.match(r'^(.+?)-\d+$', current_stem)
    if split_before_proc:
        current_stem = split_before_proc.group(1)
    
    if found_suffixes:
        unique_labels = []
        for label in found_suffixes:
            if label not in unique_labels:
                unique_labels.append(label)
        proc_label = '-'.join(unique_labels)
        return (current_stem, proc_label)
    
    return (stem, None)


def extract_derivative_info(filename: str) -> Optional[Tuple[str, str]]:
    """Detect and extract MaxFilter processing information from filename.
    
    Strips recognized suffixes and builds the base filename. Handles multiple
    suffixes (e.g., chessboard2_mc_ave.fif -> chessboard2.fif, "mc-ave").
    Also handles split file suffixes (e.g., file_tsss_mc-1.fif, file-1_tsss_mc.fif).
    
    Parameters
    ----------
    filename : str
        FIF filename to check
    
    Returns
    -------
    Optional[Tuple[str, str]]
        (base_filename, proc_label) if derivative detected, None if raw file
    """
    base_name, proc_label = _extract_base_name_and_suffix(filename, with_proc=True)
    
    if proc_label is None:
        return None
    
    base_filename = base_name + '.fif'
    return (base_filename, proc_label)


def should_exclude_file(filename: str, exclude_patterns: List[str]) -> Optional[str]:
    """Check if a file should be excluded based on configured patterns.
    
    Performs case-insensitive wildcard matching against exclude patterns.
    
    Parameters
    ----------
    filename : str
        FIF filename to check
    exclude_patterns : List[str]
        List of wildcard patterns (e.g., '*test*', '*demo*')
    
    Returns
    -------
    Optional[str]
        Matched pattern if file should be excluded, None otherwise
    """
    if not exclude_patterns:
        return None
    
    filename_lower = filename.lower()
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(filename_lower, pattern.lower()):
            return pattern
    
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
        
        base_name, _ = _extract_base_name_and_suffix(fif_path.name, with_proc=False)
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


def detect_derivative_split_files(deriv_files: List[Path]) -> Tuple[Dict[Path, List[Path]], set]:
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
    Tuple[Dict[Path, List[Path]], set]
        (mapping of primary file -> list of all parts in order, set of all files in split groups)
    """
    split_groups = {}
    processed = set()
    deriv_files_set = set(deriv_files)
    
    # Group derivatives by (base_name, proc_label)
    deriv_groups = defaultdict(list)
    for deriv_file in deriv_files:
        base_name, proc_label = _extract_base_name_and_suffix(deriv_file.name, with_proc=True)
        
        if proc_label is None:
            continue
        
        key = (base_name, proc_label)
        deriv_groups[key].append(deriv_file)
    
    # For each group, detect splits
    for (base_name, proc_label), files in deriv_groups.items():
        if len(files) <= 1:
            continue
        
        proc_suffix = proc_label.replace('-', '_')
        parent_dir = files[0].parent
        
        parts = []
        
        # Try pattern 1: base_proc.fif, base-1_proc.fif, base-2_proc.fif (split BEFORE proc)
        # This is the preferred pattern. Can have base file or just -1, -2, etc.
        base_file = parent_dir / f"{base_name}_{proc_suffix}.fif"
        
        pattern1_parts = []
        if base_file in deriv_files_set:
            pattern1_parts.append(base_file)
        
        # Look for split parts (-1, -2, etc.) regardless of whether base exists
        idx = 1
        while True:
            next_part = parent_dir / f"{base_name}-{idx}_{proc_suffix}.fif"
            if next_part in deriv_files_set:
                pattern1_parts.append(next_part)
                idx += 1
            else:
                break
        
        if len(pattern1_parts) > 1:
            parts = pattern1_parts
        
        # If pattern 1 didn't find splits, try pattern 2: base_proc-1.fif, base_proc-2.fif (split AFTER proc)
        if not parts:
            base_file_p2 = parent_dir / f"{base_name}_{proc_suffix}-1.fif"
            
            if base_file_p2 in deriv_files_set:
                pattern2_parts = []
                
                # Check if there's also a base file without split suffix
                base_without_split = parent_dir / f"{base_name}_{proc_suffix}.fif"
                if base_without_split in deriv_files_set:
                    pattern2_parts.append(base_without_split)
                
                idx = 1
                while True:
                    next_part = parent_dir / f"{base_name}_{proc_suffix}-{idx}.fif"
                    if next_part in deriv_files_set:
                        pattern2_parts.append(next_part)
                        idx += 1
                    else:
                        break
                
                if len(pattern2_parts) > 1:
                    parts = pattern2_parts
        
        # Create split group if we found parts
        if len(parts) > 1:
            split_groups[parts[0]] = parts
            # CRITICAL: Exclude ALL files in this (base_name, proc_label) group from primary processing
            processed.update(files)
            
            # Check if there are alternative pattern files in the group
            alternative_files = [f for f in files if f not in parts]
            if alternative_files:
                logger.info(f"Consolidated: {base_name}_{proc_label} - multiple naming patterns detected")
    
    return split_groups, processed



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


def extract_run_from_filename(filename: str, extraction_method: str = "last_digits", meg_id: Optional[str] = None) -> Optional[int]:
    """Extract run number from filename.
    
    Excludes split file patterns (e.g., -1.fif, -2.fif).
    Rejects if the extracted number matches the meg_id.
    
    Parameters
    ----------
    filename : str
        FIF filename
    extraction_method : str
        Method to use: "last_digits", "first_digits", or "none"
    meg_id : Optional[str]
        MEG ID to exclude from run number extraction
    
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
    if not matches:
        return None
    
    if extraction_method == "first_digits":
        candidate = int(matches[0])
    else:  # "last_digits" or default
        candidate = int(matches[-1])
    
    # Reject if it matches the meg_id
    if meg_id is not None:
        try:
            meg_id_num = int(meg_id)
            if candidate == meg_id_num:
                return None
        except (ValueError, TypeError):
            pass
    
    return candidate


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
    
    if selected_file and selected_date:
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
    calibration_files: Dict[str, Optional[Path]] = {'crosstalk': None, 'calibration': None}
    
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


def parse_task_spec(task_spec: str) -> Dict[str, Optional[str]]:
    """Parse a config task specification into BIDS entities.

    Supports simple task labels (e.g., ``"rest"``) and composite labels with
    extra entities separated by underscores (e.g., ``"noise_acq-supine"``).

    Parameters
    ----------
    task_spec : str
        Task specification from meg2bids.json pattern rule.

    Returns
    -------
    Dict[str, Optional[str]]
        Dictionary with BIDS entities extracted from the task specification.
        Always contains a ``task`` key.
    """
    entities: Dict[str, Optional[str]] = {
        'task': None,
        'acq': None,
        'ce': None,
        'dir': None,
        'rec': None,
    }

    if not task_spec:
        entities['task'] = 'unknown'
        return entities

    tokens = [token for token in str(task_spec).split('_') if token]
    if not tokens:
        entities['task'] = 'unknown'
        return entities

    # First token is always task label
    entities['task'] = tokens[0]

    # Optional extra entities (e.g., acq-supine)
    for token in tokens[1:]:
        if '-' not in token:
            # Keep backward-compatible behavior for non-entity suffixes
            entities['task'] = f"{entities['task']}-{token}"
            continue

        entity_name, entity_value = token.split('-', 1)
        if entity_name in entities and entity_value:
            entities[entity_name] = entity_value
        else:
            # Unknown token: append to task to avoid dropping user input
            entities['task'] = f"{entities['task']}-{token}"

    return entities





def add_associated_empty_room_to_session(
    meg_dir: Path,
    subject: str,
    session: Optional[str] = None
) -> None:
    """Add AssociatedEmptyRoom field to all MEG JSON files in a session.
    
    Finds the task-noise MEG file and adds its filename to all other MEG JSON files.
    According to BIDS specification, AssociatedEmptyRoom should reference
    the empty room FIF file used for noise characterization.
    
    Reference: https://bids-specification.readthedocs.io/en/stable/glossary.html#associatedemptyroom-metadata
    
    Parameters
    ----------
    meg_dir : Path
        Path to the MEG directory containing the converted files
    subject : str
        Subject ID (without 'sub-' prefix)
    session : Optional[str]
        Session ID (without 'ses-' prefix)
    """
    if not meg_dir.exists():
        return
    
    # Find the noise file
    noise_files = list(meg_dir.glob("*task-noise*_meg.fif"))
    if not noise_files:
        logger.debug(f"No task-noise file found in {meg_dir}")
        return
    
    noise_filename = noise_files[0].name
    logger.info(f"  Adding AssociatedEmptyRoom reference to: {noise_filename}")
    
    # Find all MEG JSON files (except the noise file itself)
    meg_json_files = [f for f in meg_dir.glob("*_meg.json") 
                      if 'task-noise' not in f.name]
    
    if not meg_json_files:
        logger.debug("No MEG JSON files to update")
        return
    
    # Add AssociatedEmptyRoom to each JSON file
    updated_count = 0
    for json_path in meg_json_files:
        try:
            # Read existing JSON
            with open(json_path, 'r') as f:
                metadata = json.load(f)
            
            # Add the AssociatedEmptyRoom field
            metadata['AssociatedEmptyRoom'] = noise_filename
            
            # Write updated JSON
            with open(json_path, 'w') as f:
                json.dump(metadata, f, indent=4)
            
            updated_count += 1
            logger.debug(f"  Updated {json_path.name}")
            
        except Exception as err:
            logger.warning(f"Failed to update {json_path.name}: {err}")
    
    if updated_count > 0:
        logger.info(f"  ✓ Added AssociatedEmptyRoom to {updated_count} file(s)")


def consolidate_coordsystem_metadata(
    meg_dir: Path,
    subject: str,
    session: Optional[str] = None
) -> None:
    """Keep only one coordsystem JSON file per subject/session.

    Removes redundant acquisition-specific files such as
    ``sub-<id>[_ses-<id>]_acq-<label>_coordsystem.json`` and retains a single
    ``sub-<id>[_ses-<id>]_coordsystem.json`` file.

    Parameters
    ----------
    meg_dir : Path
        Path to the MEG directory containing converted files
    subject : str
        Subject ID (without 'sub-' prefix)
    session : Optional[str]
        Session ID (without 'ses-' prefix)
    """
    if not meg_dir.exists():
        return

    coordsystem_files = sorted(meg_dir.glob("*_coordsystem.json"))
    if len(coordsystem_files) <= 1:
        return

    if session:
        canonical_name = f"sub-{subject}_ses-{session}_coordsystem.json"
    else:
        canonical_name = f"sub-{subject}_coordsystem.json"
    canonical_path = meg_dir / canonical_name

    acq_files = [f for f in coordsystem_files if "_acq-" in f.name]
    if not acq_files:
        return

    # Ensure canonical coordsystem file exists
    if not canonical_path.exists():
        try:
            shutil.copy2(acq_files[0], canonical_path)
            logger.info(f"  ✓ Created canonical coordsystem file: {canonical_name}")
        except Exception as err:
            logger.warning(f"Failed to create canonical coordsystem file {canonical_name}: {err}")
            return

    removed = 0
    for acq_file in acq_files:
        try:
            acq_file.unlink()
            removed += 1
        except Exception as err:
            logger.warning(f"Failed to delete redundant coordsystem file {acq_file.name}: {err}")

    if removed > 0:
        logger.info(f"  ✓ Removed {removed} redundant acq-specific coordsystem file(s)")


def extract_and_write_headshape(
    raw: 'mne.io.BaseRaw',
    subject: str,
    session: Optional[str],
    bids_root: Path,
    datatype: str = 'meg'
) -> Optional[Path]:
    """Extract digitized head points from raw FIF and write to *_headshape.pos file.
    
    Extracts all digitized points (head surface, fiducials, etc.) from the raw.info['dig']
    and writes them to a Polhemus .pos format file. This file is shared across all tasks/runs
    in a session since the head shape doesn't change.
    
    Parameters
    ----------
    raw : mne.io.BaseRaw
        MNE raw object containing digitized point information
    subject : str
        Subject ID (without 'sub-' prefix)
    session : Optional[str]
        Session ID (without 'ses-' prefix)
    bids_root : Path
        BIDS root directory
    datatype : str
        BIDS datatype folder (default: 'meg')

    Returns
    -------
    Optional[Path]
        Path to created headshape file, None if no digitized points found
    """
    # Check if there are digitized points
    dig = raw.info.get('dig')
    if not dig or len(dig) == 0:
        logger.debug("No digitized points found in raw data")
        return None

    try:
        # Create target directory
        if session:
            target_dir = bids_root / f"sub-{subject}" / f"ses-{session}" / datatype
        else:
            target_dir = bids_root / f"sub-{subject}" / datatype

        target_dir.mkdir(parents=True, exist_ok=True)

        # Create headshape filename (session-specific, shared across all tasks/runs)
        fname_parts = [f"sub-{subject}"]
        if session:
            fname_parts.append(f"ses-{session}")
        fname_parts.append("headshape.pos")

        headshape_filename = "_".join(fname_parts)
        headshape_path = target_dir / headshape_filename

        # Check if file already exists (don't overwrite)
        if headshape_path.exists():
            logger.debug(f"Headshape file already exists: {headshape_filename}")
            return headshape_path

        # Extract and write digitized points in Polhemus .pos format
        # Format: plain text with one point per line: x y z (in meters, space-separated)
        with open(headshape_path, 'w') as f:
            for point in dig:
                coords = point.get('r')  # Get 3D coordinates [x, y, z]
                if coords is not None:
                    # Write coordinates with high precision (7 decimal places typical for Polhemus)
                    f.write(f"{coords[0]:.7f} {coords[1]:.7f} {coords[2]:.7f}\n")

        logger.info(f"  ✓ Created headshape file: {headshape_filename} ({len(dig)} digitized points)")
        return headshape_path

    except Exception as err:
        logger.warning(f"Failed to extract and write headshape points: {err}")
        return None


def convert_raw_file(
    fif_path: Path,
    subject: str,
    session: Optional[str],
    task: str,
    run: Optional[int],
    config: Dict[str, Any],
    bids_root: Path,
    split_parts: Optional[List[Path]] = None,
    task_entities: Optional[Dict[str, Optional[str]]] = None
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
    task_entities : Optional[Dict[str, Optional[str]]]
        Parsed BIDS entities from task specification (task/acq/rec/dir/ce)
    
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    datatype = config.get('dataset', {}).get('datatype', 'meg')
    allow_maxshield = config.get('options', {}).get('allow_maxshield', True)
    overwrite = config.get('options', {}).get('overwrite', True)
    
    # Build info string with run and parts count (in separate parens before arrow)
    info_parts = []
    if run is not None:
        info_parts.append(f"run {run}")
    
    info_str = f" ({', '.join(info_parts)})" if info_parts else ""
    
    # Check if file is part of a split group based on first_samps
    try:
        raw_temp = mne.io.read_raw_fif(fif_path, preload=False, allow_maxshield=allow_maxshield, verbose=False)
        first_samps = getattr(raw_temp, '_first_samps', None)
        if first_samps is not None and len(first_samps) > 1:
            info_str += f" ({len(first_samps)} parts)"
    except:
        pass
    
    # Build suffix string with task and acq (after arrow)
    suffix_parts = [f"task={task}"]
    if task_entities and task_entities.get('acq'):
        suffix_parts.append(f"acq={task_entities['acq']}")
    suffix_str = ", ".join(suffix_parts)
    
    logger.info(f"  ✓ Converting: {fif_path.name}{info_str} -> {suffix_str}")
    
    try:
        # MNE automatically handles split files
        raw = mne.io.read_raw_fif(fif_path, preload=False, allow_maxshield=allow_maxshield, verbose=False)
        
        normalize_raw_info(raw)

        if task_entities is None:
            task_entities = parse_task_spec(task)

        task_label = task_entities.get('task') or task
        
        bids_path = BIDSPath(
            subject=subject,
            session=session,
            task=task_label,
            acquisition=task_entities.get('acq'),
            run=run,
            datatype=datatype,
            root=bids_root
        )
        
        # Patch mne-bids split size to 1900MB for maxfilter compatibility
        # (maxfilter has strict 2GB limit, default MNE uses ~2.1GB)
        mne_bids.write._FIFF_SPLIT_SIZE = "1900MB"
        
        write_raw_bids(raw, bids_path, overwrite=overwrite, verbose=False)
        
        # Clean up temporary FIF read if it was created
        try:
            raw_temp.close()
        except:
            pass
        
        # Extract headshape file (no-op if already created)
        extract_and_write_headshape(raw, subject, session, bids_root, datatype)
        
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
    split_parts: Optional[List[Path]] = None,
    acq: Optional[str] = None
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
    acq : Optional[str]
        Acquisition label (optional)
    
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
        for split_file in split_parts:
            # Determine split index from source filename suffix
            # Examples:
            # - MEG_3818_RestEyesClosed_sss.fif (no suffix) -> split-01
            # - MEG_3818_RestEyesClosed-1_sss.fif (suffix -1) -> split-02
            # - MEG_3818_RestEyesClosed_sss-1.fif (suffix -1 after proc) -> split-02
            # Extract split suffix from source filename
            stem = split_file.stem
            
            # First check for split before proc pattern: base-N_proc.fif
            split_before_match = re.search(r'^(.+)-(\d+)_[a-z]', stem)
            if split_before_match:
                split_num = int(split_before_match.group(2))
                split_idx = split_num + 1  # -1 becomes split-02, -2 becomes split-03
            else:
                # Check for split after proc pattern: base_proc-N.fif
                split_after_match = re.search(r'_[a-z]+(?:_[a-z]+)*-(\d+)$', stem)
                if split_after_match:
                    split_num = int(split_after_match.group(1))
                    split_idx = split_num + 1  # -1 becomes split-02, -2 becomes split-03
                else:
                    # No split suffix - use position in parts list
                    split_idx = split_parts.index(split_file) + 1
            
            # Target filename: sub-<label>[_ses-<label>]_task-<label>[_acq-<label>][_run-<index>]_split-<index>_proc-<label>_meg.fif
            fname_parts = [f"sub-{subject}"]
            if session:
                fname_parts.append(f"ses-{session}")
            fname_parts.append(f"task-{task}")
            if acq is not None:
                fname_parts.append(f"acq-{acq}")
            if run is not None:
                fname_parts.append(f"run-{run:02d}")
            fname_parts.append(f"split-{split_idx:02d}")
            fname_parts.append(f"proc-{proc_label}")
            fname_parts.append("meg.fif")
            
            target_name = "_".join(fname_parts)
            target_path = target_dir / target_name
            
            try:
                shutil.copy2(split_file, target_path)
                logger.debug(f"    -> Saved split {split_idx}/{len(split_parts)}: {target_name}")
            except Exception as err:
                logger.error(f"  ✗ Failed to copy derivative split {split_file.name}: {err}")
                all_success = False
        
        return all_success
    
    else:
        # Single file (no splits)
        # Target filename: sub-<label>[_ses-<label>]_task-<label>[_acq-<label>][_run-<index>]_proc-<label>_meg.fif
        fname_parts = [f"sub-{subject}"]
        if session:
            fname_parts.append(f"ses-{session}")
        fname_parts.append(f"task-{task}")
        if acq is not None:
            fname_parts.append(f"acq-{acq}")
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
    """Extract all BIDS entities from a filename.
    
    Parses BIDS entities including sub, ses, task, run, acq, rec, split, proc, dir, ce
    and other standard entities. Handles both standard and extended entities.
    
    Parameters
    ----------
    filename : str
        BIDS filename (without path)
    
    Returns
    -------
    Dict[str, Optional[str]]
        Dictionary with extracted entities (all values default to None)
    """
    entities: Dict[str, Optional[str]] = {
        'sub': None,
        'ses': None,
        'task': None,
        'acq': None,
        'ce': None,
        'rec': None,
        'dir': None,
        'run': None,
        'mod': None,
        'echo': None,
        'flip': None,
        'inv': None,
        'mt': None,
        'part': None,
        'proc': None,
        'hemi': None,
        'space': None,
        'split': None,
        'desc': None,
    }
    
    # Remove extension(s) - handle cases like .fif, .json, .nii.gz
    name = filename
    for ext in ['.nii.gz', '.tsv.gz', '.json', '.nii', '.tsv', '.fif', '.pos', '.dat']:
        if name.lower().endswith(ext):
            name = name[:-len(ext)]
            break
    
    # Split by underscore
    parts = name.split('_')
    
    for part in parts:
        if '-' not in part:
            continue
        
        entity_name, entity_value = part.split('-', 1)
        if entity_name in entities and entity_value:
            entities[entity_name] = entity_value
    
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
    session: Optional[str] = None,
    overwrite: bool = False,
    interactive_deduplication: bool = True
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
    overwrite : bool
        If True, overwrite existing participant data. If False, skip existing participants.
    interactive_deduplication : bool
        If True and in interactive environment, prompt user when duplicate files 
        have same preference level. Default: True (interactive mode enabled)
    
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
    
    # Filter out existing participants unless overwrite is enabled
    if not overwrite:
        new_participants = []
        for participant in participant_labels:
            participant_id = participant.replace('sub-', '')
            # Check if MEG data already exists for this participant
            if session:
                meg_dir = rawdata_dir / f"sub-{participant_id}" / f"ses-{session}" / "meg"
            else:
                meg_dir = rawdata_dir / f"sub-{participant_id}" / "meg"
            
            if meg_dir.exists():
                logger.info(f"Participant {participant_id} already has MEG data, skipping (use --overwrite to re-process)")
            else:
                new_participants.append(participant)
        
        if not new_participants:
            logger.info("All participants already have MEG data. Skipping MEG import.")
            return True
        
        participant_labels = new_participants
    
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
            
            # Log derivative files found for debugging
            if derivative_files:
                logger.debug(f"  Derivative files detected:")
                for df in derivative_files:
                    deriv_info = extract_derivative_info(df.name)
                    if deriv_info:
                        base, proc = deriv_info
                        logger.debug(f"    • {df.name} -> base={base}, proc={proc}")
            
            # Apply file exclusion patterns if configured
            exclude_patterns = config.get('exclude_patterns', [])
            if exclude_patterns:
                logger.info(f"Applying {len(exclude_patterns)} exclusion pattern(s)...")
                excluded_raw = []
                excluded_deriv = []
                
                # Check raw files
                for fif_file in raw_files:
                    matched_pattern = should_exclude_file(fif_file.name, exclude_patterns)
                    if matched_pattern:
                        logger.info(f"  ⊗ Excluded (pattern: {matched_pattern}): {fif_file.name}")
                        excluded_raw.append(fif_file)
                        stats.add_file('excluded', 'excluded', fif_file.name)
                
                # Check derivative files
                for fif_file in derivative_files:
                    matched_pattern = should_exclude_file(fif_file.name, exclude_patterns)
                    if matched_pattern:
                        logger.info(f"  ⊗ Excluded (pattern: {matched_pattern}): {fif_file.name}")
                        excluded_deriv.append(fif_file)
                        stats.add_file('excluded', 'excluded', fif_file.name)
                
                # Remove excluded files from processing lists
                raw_files = [f for f in raw_files if f not in excluded_raw]
                derivative_files = [f for f in derivative_files if f not in excluded_deriv]
                
                if excluded_raw or excluded_deriv:
                    total_excluded = len(excluded_raw) + len(excluded_deriv)
                    logger.info(f"  Excluded {total_excluded} file(s)")
            
            logger.info(f"Processing {len(raw_files)} raw file(s), {len(derivative_files)} derivative file(s)")
            
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
            
            # Identify primary files (remove duplicates) - returns (files, split_group_count)
            raw_files, split_group_count = identify_primary_files(raw_files, interactive=interactive_deduplication)
            
            # Detect split files
            split_file_groups = detect_split_files(raw_files)
            
            # Log processing summary with split group info (counted from first_samps metadata)
            split_info = ""
            if split_group_count > 0:
                split_info = f" (+ {split_group_count} split group{'s' if split_group_count != 1 else ''})"
            logger.info(f"Processing {len(raw_files)} raw file(s){split_info}, {len(derivative_files)} derivative file(s)")
            
            # Get primary files only (exclude split parts)
            # Get primary files only (exclude split parts)
            split_parts = set()
            for primary_file, parts in split_file_groups.items():
                split_parts.update(parts[1:])
            primary_raw_files = [f for f in raw_files if f not in split_parts]
            
            # Match files to patterns and extract run and acq
            logger.info("Matching files to patterns...")
            file_mapping = {}
            for fif_file in primary_raw_files:
                pattern_rule = match_file_pattern(fif_file.name, file_patterns)
                if pattern_rule:
                    task = pattern_rule.get('task', 'unknown')
                    task_entities = parse_task_spec(task)
                    run_extraction = pattern_rule.get('run_extraction', 'last_digits')
                    run = extract_run_from_filename(fif_file.name, run_extraction, meg_id)
                    
                    # Extract acquisition label if configured
                    acq = None
                    acq_config = pattern_rule.get('acq')
                    if acq_config:
                        # Check if acq_config is an extraction method (last_digits, first_digits) or static text
                        if acq_config in ('last_digits', 'first_digits'):
                            # Extract number from filename
                            acq = extract_run_from_filename(fif_file.name, acq_config, meg_id)
                            # Convert to string for BIDS
                            if acq is not None:
                                acq = str(acq)
                        else:
                            # Use as static text
                            acq = str(acq_config)
                    
                    file_mapping[fif_file] = (task, task_entities, run, acq, pattern_rule)
                    acq_str = f", acq={acq}" if acq is not None else ""
                    logger.debug(f"  {fif_file.name} -> task={task}, run={run}{acq_str}")
                else:
                    logger.warning(f"  ⊘ No matching pattern for: {fif_file.name}")
            
            # Group by task and assign run numbers if needed
            task_files = defaultdict(list)
            for fif_path, (task, task_entities, run, acq, pattern_rule) in file_mapping.items():
                task_key = task
                run_extraction = pattern_rule.get('run_extraction', 'last_digits')
                task_files[task_key].append((fif_path, task, task_entities, run, acq, run_extraction))
            
            # Reassign run numbers ONLY if they were not extracted from filenames
            final_mapping = {}
            for task_key, files in task_files.items():
                if len(files) == 1:
                    fif_path, task, task_entities, run, acq, run_extraction = files[0]
                    final_mapping[fif_path] = (task, task_entities, run, acq)  # Keep extracted run and acq (or None)
                else:
                    # Multiple files per task
                    # Check if any files have extracted run numbers
                    has_extracted_runs = any(run is not None for _, _, _, run, _, _ in files)
                    
                    # Check if run_extraction was explicitly disabled
                    run_extraction_disabled = all(run_ext == "none" for _, _, _, _, _, run_ext in files)
                    
                    if has_extracted_runs:
                        # Keep extracted run numbers as-is
                        for fif_path, task, task_entities, run, acq, _ in files:
                            final_mapping[fif_path] = (task, task_entities, run, acq)
                    elif run_extraction_disabled:
                        # run_extraction is explicitly "none" - don't assign sequential numbers
                        for fif_path, task, task_entities, _, acq, _ in files:
                            final_mapping[fif_path] = (task, task_entities, None, acq)
                    else:
                        # No run numbers extracted and not explicitly disabled - assign sequential numbers (original behavior)
                        sorted_files = sorted(files, key=lambda x: (x[3] if x[3] is not None else float('inf'), x[0].name))
                        for idx, (fif_path, task, task_entities, _, acq, _) in enumerate(sorted_files, start=1):
                            final_mapping[fif_path] = (task, task_entities, idx, acq)
            
            # Convert files
            logger.info("Converting files...")
            for fif_path in primary_raw_files:
                if fif_path not in final_mapping:
                    stats.add_file('unknown', 'skipped', fif_path.name)
                    continue
                
                task, task_entities, run, acq = final_mapping[fif_path]
                split_parts_for_file = split_file_groups.get(fif_path, None)
                
                # Merge extracted acq into task_entities if present
                if acq is not None and task_entities is not None:
                    task_entities['acq'] = acq
                
                success = convert_raw_file(
                    fif_path, participant_id, session_id, task, run,
                    config, rawdata_dir, split_parts_for_file, task_entities
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
                    deriv_split_groups, deriv_split_parts = detect_derivative_split_files(derivative_files)
                    
                    # deriv_split_parts contains ALL files that are part of split groups
                    # Primary deriv files are ONLY those NOT in any split group
                    primary_deriv_files = [f for f in derivative_files if f not in deriv_split_parts]
                    
                    # Build mapping of task -> raw file organization (splits or runs)
                    task_organization = {}  # task -> {'type': 'splits'/'runs', 'base_names': set()}
                    for raw_path, (task, task_entities, run, _) in final_mapping.items():
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
                    
                    # Process each primary and split derivative file
                    for deriv_file in primary_deriv_files:
                        deriv_info = extract_derivative_info(deriv_file.name)
                        if deriv_info is None:
                            continue
                        
                        base_filename, proc_label = deriv_info
                        
                        # Try to match derivative to a raw file task/run using the BASE filename
                        # (not the derivative filename with processor suffix)
                        pattern_rule = match_file_pattern(base_filename, file_patterns)
                        if pattern_rule:
                            task = pattern_rule.get('task', 'unknown')
                            run_extraction = pattern_rule.get('run_extraction', 'last_digits')
                            
                            # Extract acquisition label if configured
                            acq = None
                            acq_config = pattern_rule.get('acq')
                            if acq_config:
                                # Check if acq_config is an extraction method (last_digits, first_digits) or static text
                                if acq_config in ('last_digits', 'first_digits'):
                                    # Extract number from filename
                                    acq = extract_run_from_filename(deriv_file.name, acq_config, meg_id)
                                    # Convert to string for BIDS
                                    if acq is not None:
                                        acq = str(acq)
                                else:
                                    # Static text value
                                    acq = acq_config
                            
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
                                    # This is a split file group
                                    is_split = True
                                    # Even for splits, extract run number from derivative base name
                                    # Example: RS_1_sss.fif and RS_1-1_sss.fif -> both have run=1
                                    run = extract_run_from_filename(deriv_file.name, run_extraction, meg_id)
                                else:
                                    # This is a run-based organization
                                    run = extract_run_from_filename(deriv_file.name, run_extraction, meg_id)
                            else:
                                # No corresponding raw file info, use default extraction
                                run = extract_run_from_filename(deriv_file.name, run_extraction, meg_id)
                            
                            # Format log message
                            if is_split and split_parts_for_deriv:
                                num_parts = len(split_parts_for_deriv)
                                if run is not None:
                                    run_str = f" ({num_parts} parts, run {run})"
                                else:
                                    run_str = f" ({num_parts} parts)"
                            elif run is not None:
                                run_str = f" (run {run})"
                            else:
                                run_str = ""
                            
                            acq_str = f", acq={acq}" if acq is not None else ""
                            logger.info(f"  ✓ Converting: {deriv_file.name}{run_str} -> task={task}{acq_str} (proc-{proc_label})")
                            
                            success = copy_derivative_file(
                                deriv_file, participant_id, session_id, task, run,
                                deriv_info, derivatives_dir, pipeline_name, pipeline_version,
                                split_parts=split_parts_for_deriv, acq=acq
                            )
                            
                            if not success:
                                logger.warning(f"    ✗ Copy failed for {deriv_file.name}")
                        else:
                            logger.warning(f"  ⊘ No pattern match for derivative: {deriv_file.name}")
                    
                    # Process split file groups
                    for primary_file, split_parts in deriv_split_groups.items():
                        deriv_info = extract_derivative_info(primary_file.name)
                        if deriv_info is None:
                            continue
                        
                        base_filename, proc_label = deriv_info
                        
                        # Try to match derivative to a raw file task/run using the BASE filename
                        pattern_rule = match_file_pattern(base_filename, file_patterns)
                        if pattern_rule:
                            task = pattern_rule.get('task', 'unknown')
                            run_extraction = pattern_rule.get('run_extraction', 'last_digits')
                            
                            # Extract acquisition label if configured
                            acq = None
                            acq_config = pattern_rule.get('acq')
                            if acq_config:
                                # Check if acq_config is an extraction method (last_digits, first_digits) or static text
                                if acq_config in ('last_digits', 'first_digits'):
                                    # Extract number from filename
                                    acq = extract_run_from_filename(primary_file.name, acq_config, meg_id)
                                    # Convert to string for BIDS
                                    if acq is not None:
                                        acq = str(acq)
                                else:
                                    # Static text value
                                    acq = acq_config
                            
                            # Determine run for this split group
                            run = None
                            if task in task_organization:
                                deriv_base = base_filename.split('-')[0]
                                # For split groups, always extract run number from the base filename
                                # Example: RS_1_sss.fif (split primary) has run=1 embedded
                                run = extract_run_from_filename(primary_file.name, run_extraction, meg_id)
                            else:
                                # No corresponding raw file info, use default extraction
                                run = extract_run_from_filename(primary_file.name, run_extraction, meg_id)
                            
                            # Log split file group processing
                            num_parts = len(split_parts)
                            if run is not None:
                                run_str = f" ({num_parts} parts, run {run})"
                            else:
                                run_str = f" ({num_parts} parts)"
                            acq_str = f", acq={acq}" if acq is not None else ""
                            logger.info(f"  ✓ Converting: {primary_file.name}{run_str} -> task={task}{acq_str} (proc-{proc_label})")
                            
                            success = copy_derivative_file(
                                primary_file, participant_id, session_id, task, run,
                                deriv_info, derivatives_dir, pipeline_name, pipeline_version,
                                split_parts=split_parts, acq=acq
                            )
                            
                            if not success:
                                logger.warning(f"    ✗ Copy failed for split group: {primary_file.name}")
                        else:
                            logger.warning(f"  ⊘ No pattern match for split derivative: {primary_file.name}")
            
            # Add AssociatedEmptyRoom to all MEG JSON files
            logger.info("Processing AssociatedEmptyRoom metadata...")
            if session_id:
                meg_dir = rawdata_dir / f"sub-{participant_id}" / f"ses-{session_id}" / "meg"
            else:
                meg_dir = rawdata_dir / f"sub-{participant_id}" / "meg"
            add_associated_empty_room_to_session(meg_dir, participant_id, session_id)

            # Keep one coordsystem file per session/subject (drop acq-specific duplicates)
            consolidate_coordsystem_metadata(meg_dir, participant_id, session_id)
        
        # Summary for this participant
        logger.info(f"{'-'*60}")
        logger.info(f"Summary for {bids_subject}:")
        logger.info(f"  Total files found: {stats.total_files}")
        logger.info(f"  ✓ Converted: {stats.converted}")
        if stats.excluded > 0:
            logger.info(f"  ⊗ Excluded:  {stats.excluded}")
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
