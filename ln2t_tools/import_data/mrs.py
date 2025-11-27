"""MRS data to BIDS conversion using spec2bids/spec2nii."""

import logging
import subprocess
import tarfile
from pathlib import Path
from typing import List, Optional
import json

logger = logging.getLogger(__name__)


def import_mrs(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = False,
    venv_path: Optional[Path] = None
) -> bool:
    """Import MRS data to BIDS format using spec2bids.
    
    Based on the spec2bids tool pattern: processes GE P-files and other
    MRS raw data formats into BIDS-compliant NIfTI format.
    
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
        Dataset initials prefix (e.g., 'CB', 'HP')
    session : Optional[str]
        Session label (without 'ses-' prefix)
    compress_source : bool
        Whether to compress source files after successful conversion
    venv_path : Optional[Path]
        Path to virtual environment containing spec2nii
        
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
    
    # Setup virtual environment
    if venv_path is None:
        venv_path = Path.home() / "venvs" / "general_purpose_env"
    
    activate_script = venv_path / "bin" / "activate"
    if not activate_script.exists():
        logger.warning(f"Virtual environment not found at {venv_path}, will try system spec2bids")
        venv_cmd = ""
    else:
        venv_cmd = f". {activate_script} && "
    
    # Create rawdata directory if needed
    rawdata_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if spec2bids is available
    check_cmd = f"{venv_cmd}which spec2bids"
    result = subprocess.run(
        check_cmd,
        shell=True,
        capture_output=True,
        text=True,
        executable='/bin/bash'
    )
    
    if result.returncode != 0:
        logger.error(
            "spec2bids not found. Please install it:\n"
            "  Clone from https://github.com/arovai/spec2bids or install via pip if available"
        )
        return False
    
    # Process each participant
    success_count = 0
    failed_participants = []
    
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
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        
        # Determine source directory name using strict pattern
        if ds_initials:
            # Use strict naming convention: AB042 or AB042SES4
            if session:
                source_name = f"{ds_initials}{participant_id}SES{session}"
            else:
                source_name = f"{ds_initials}{participant_id}"
            
            source_path = mrs_dir / source_name
            
            if not source_path.exists():
                logger.error(f"Source MRS directory not found: {source_path}")
                logger.error(f"Expected naming convention: {ds_initials}{participant_id}" + 
                           (f"SES{session}" if session else ""))
                failed_participants.append(participant_id)
                continue
        else:
            # Fallback to flexible matching if ds_initials could not be determined
            pattern = f"*{participant_id}*"
            if session:
                pattern = f"*{participant_id}*{session}*"
            
            matches = list(mrs_dir.glob(pattern))
            if not matches:
                logger.error(f"No MRS directory found matching {pattern} in {mrs_dir}")
                logger.error(f"Hint: Use --ds-initials flag for strict directory matching")
                failed_participants.append(participant_id)
                continue
            elif len(matches) > 1:
                logger.warning(f"Multiple matches for {pattern}: {[m.name for m in matches]}")
                logger.info(f"Using first match: {matches[0].name}")
            source_path = matches[0]
        
        # Run spec2bids
        logger.info(f"Running spec2bids for {participant_id}...")
        
        # Build spec2bids command
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
            
            # Compress source files if requested
            if compress_source:
                compressed_file = mrs_dir / f"{source_path.name}.tar.gz"
                if not compressed_file.exists():
                    logger.info(f"Compressing {source_path.name}...")
                    try:
                        with tarfile.open(compressed_file, "w:gz") as tar:
                            tar.add(source_path, arcname=source_path.name)
                        logger.info(f"✓ Created {compressed_file.name}")
                    except Exception as e:
                        logger.error(f"Failed to compress {source_path.name}: {e}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"✗ Failed to import MRS data for {participant_id}: {e.stderr}")
            failed_participants.append(participant_id)
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
