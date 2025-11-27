"""Physiological data to BIDS conversion using phys2bids."""

import logging
import subprocess
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def import_physio(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = False,
    venv_path: Optional[Path] = None
) -> bool:
    """Import physiological data to BIDS format.
    
    This is a placeholder for physio import functionality.
    Implementation will be discussed and finalized based on specific needs.
    
    Potential approaches:
    - Use phys2bids (https://phys2bids.readthedocs.io/)
    - Custom solution for specific hardware (e.g., BioPac, GE scanner physio)
    - Manual BIDS conversion with template
    
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
    venv_path : Optional[Path]
        Path to virtual environment
        
    Returns
    -------
    bool
        True if import successful, False otherwise
    """
    logger.warning("Physio import not yet implemented")
    logger.info("Physio data import will be added based on your specific requirements")
    logger.info("Please discuss the following:")
    logger.info("  1. What hardware/format are your physio recordings?")
    logger.info("  2. Are they already in BIDS-compatible format?")
    logger.info("  3. Should we use phys2bids or a custom solution?")
    
    # Check if physio directory exists
    physio_dir = sourcedata_dir / "physio"
    if not physio_dir.exists():
        logger.info(f"No physio directory found at {physio_dir}")
        return False
    
    logger.info(f"Found physio directory: {physio_dir}")
    
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
            logger.warning(f"Could not infer dataset initials from '{dataset}', will use flexible matching")
    
    # Check what directories exist for the requested participants
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        
        # Determine expected source directory name using strict pattern
        if ds_initials:
            # Use strict naming convention: AB042 or AB042SES4
            if session:
                expected_name = f"{ds_initials}{participant_id}SES{session}"
            else:
                expected_name = f"{ds_initials}{participant_id}"
            
            source_path = physio_dir / expected_name
            
            if source_path.exists():
                physio_files = list(source_path.rglob("*"))
                logger.info(f"  Found {expected_name}: {len(physio_files)} files/directories")
                
                # Show sample file types
                extensions = set()
                for f in physio_files:
                    if f.is_file():
                        extensions.add(f.suffix)
                if extensions:
                    logger.info(f"    File types: {', '.join(sorted(extensions))}")
            else:
                logger.info(f"  Expected directory not found: {expected_name}")
        else:
            # Fallback to flexible matching
            pattern = f"*{participant_id}*"
            if session:
                pattern = f"*{participant_id}*{session}*"
            
            matches = list(physio_dir.glob(pattern))
            if matches:
                for match in matches:
                    physio_files = list(match.rglob("*"))
                    logger.info(f"  Found {match.name}: {len(physio_files)} files/directories")
            else:
                logger.info(f"  No directory found matching {pattern}")
    
    return False


def check_phys2bids_available(venv_path: Optional[Path] = None) -> bool:
    """Check if phys2bids is installed and available.
    
    Parameters
    ----------
    venv_path : Optional[Path]
        Path to virtual environment
        
    Returns
    -------
    bool
        True if phys2bids is available
    """
    if venv_path is None:
        venv_path = Path.home() / "venvs" / "general_purpose_env"
    
    activate_script = venv_path / "bin" / "activate"
    if activate_script.exists():
        venv_cmd = f". {activate_script} && "
    else:
        venv_cmd = ""
    
    check_cmd = f"{venv_cmd}which phys2bids"
    result = subprocess.run(
        check_cmd,
        shell=True,
        capture_output=True,
        text=True,
        executable='/bin/bash'
    )
    
    return result.returncode == 0
