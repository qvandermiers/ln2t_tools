"""DICOM to BIDS conversion using dcm2bids."""

import logging
import subprocess
import shutil
from pathlib import Path
from typing import List, Optional
import tarfile

logger = logging.getLogger(__name__)


def import_dicom(
    dataset: str,
    participant_labels: List[str],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = False,
    deface: bool = False,
    venv_path: Optional[Path] = None
) -> bool:
    """Import DICOM data to BIDS format using dcm2bids.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    participant_labels : List[str]
        List of participant IDs (without 'sub-' prefix)
    sourcedata_dir : Path
        Path to sourcedata directory (e.g., ~/sourcedata/<dataset>-sourcedata)
    rawdata_dir : Path
        Path to BIDS rawdata directory
    ds_initials : Optional[str]
        Dataset initials prefix (e.g., 'CB', 'HP'). If None, tries to infer from directory structure
    session : Optional[str]
        Session label (without 'ses-' prefix)
    compress_source : bool
        Whether to compress source DICOM directories after successful conversion
    deface : bool
        Deface anatomical images after import
    venv_path : Optional[Path]
        Path to virtual environment containing dcm2bids
        
    Returns
    -------
    bool
        True if import successful, False otherwise
    """
    # Validate paths
    if not sourcedata_dir.exists():
        logger.error(f"Source data directory not found: {sourcedata_dir}")
        return False
    
    dicom_dir = sourcedata_dir / "dicom"
    if not dicom_dir.exists():
        logger.error(f"DICOM directory not found: {dicom_dir}")
        return False
    
    # Check for dcm2bids config
    config_file = sourcedata_dir / "dcm2bids" / "config.json"
    if not config_file.exists():
        # Try alternate location
        config_file = sourcedata_dir / "configs" / "dcm2bids.json"
        if not config_file.exists():
            logger.error(
                f"dcm2bids config not found at:\n"
                f"  {sourcedata_dir}/dcm2bids/config.json\n"
                f"  {sourcedata_dir}/configs/dcm2bids.json"
            )
            return False
    
    logger.info(f"Using dcm2bids config: {config_file}")
    
    # Setup virtual environment
    if venv_path is None:
        venv_path = Path.home() / "venvs" / "general_purpose_env"
    
    activate_script = venv_path / "bin" / "activate"
    if not activate_script.exists():
        logger.warning(f"Virtual environment not found at {venv_path}, will try system dcm2bids")
        venv_cmd = ""
    else:
        venv_cmd = f". {activate_script} && "
    
    # Create rawdata directory if needed
    rawdata_dir.mkdir(parents=True, exist_ok=True)
    
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
        participant_id = participant.replace('sub-', '')  # Remove prefix if present
        
        # Determine source directory name using strict pattern
        if ds_initials:
            # Use strict naming convention: AB042 or AB042SES4
            if session:
                source_name = f"{ds_initials}{participant_id}SES{session}"
            else:
                source_name = f"{ds_initials}{participant_id}"
            
            source_path = dicom_dir / source_name
            
            if not source_path.exists():
                logger.error(f"Source DICOM directory not found: {source_path}")
                logger.error(f"Expected naming convention: {ds_initials}{participant_id}" + 
                           (f"SES{session}" if session else ""))
                failed_participants.append(participant_id)
                continue
        else:
            # Fallback to flexible matching if ds_initials could not be determined
            pattern = f"*{participant_id}*"
            if session:
                pattern = f"*{participant_id}*{session}*"
            
            matches = list(dicom_dir.glob(pattern))
            if not matches:
                logger.error(f"No DICOM directory found matching {pattern} in {dicom_dir}")
                logger.error(f"Hint: Use --ds-initials flag for strict directory matching")
                failed_participants.append(participant_id)
                continue
            elif len(matches) > 1:
                logger.warning(f"Multiple matches for {pattern}: {[m.name for m in matches]}")
                logger.info(f"Using first match: {matches[0].name}")
            source_path = matches[0]
        
        # Compress source data if requested and not already compressed
        if compress_source:
            compressed_file = dicom_dir / f"{source_path.name}.tar.gz"
            if not compressed_file.exists():
                logger.info(f"Compressing {source_path.name}...")
                try:
                    with tarfile.open(compressed_file, "w:gz") as tar:
                        tar.add(source_path, arcname=source_path.name)
                    logger.info(f"✓ Created {compressed_file.name}")
                except Exception as e:
                    logger.error(f"Failed to compress {source_path.name}: {e}")
        
        # Run dcm2bids
        logger.info(f"Running dcm2bids for {participant_id}...")
        
        # Build dcm2bids command
        cmd = f"{venv_cmd}dcm2bids -o {rawdata_dir} -p {participant_id}"
        if session:
            cmd += f" -s {session}"
        cmd += f" -d {source_path} -c {config_file}"
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                executable='/bin/bash'
            )
            logger.info(f"✓ Successfully imported {participant_id}")
            if result.stdout:
                logger.debug(result.stdout)
            success_count += 1
            
        except subprocess.CalledProcessError as e:
            logger.error(f"✗ Failed to import {participant_id}: {e.stderr}")
            failed_participants.append(participant_id)
            continue
    
    # Cleanup tmp_dcm2bids directory
    tmp_dir = rawdata_dir / "tmp_dcm2bids"
    if tmp_dir.exists():
        logger.info("Cleaning up tmp_dcm2bids directory...")
        shutil.rmtree(tmp_dir)
    
    # Run defacing if requested
    if deface and success_count > 0:
        logger.info("Running pydeface on anatomical images...")
        
        # Ensure dataset_description.json exists (required by pydeface BIDS validation)
        dataset_desc_file = rawdata_dir / "dataset_description.json"
        if not dataset_desc_file.exists():
            logger.info("Creating dataset_description.json...")
            import json
            dataset_desc = {
                "Name": dataset,
                "BIDSVersion": "1.9.0"
            }
            with open(dataset_desc_file, 'w') as f:
                json.dump(dataset_desc, f, indent=4)
            logger.info(f"✓ Created {dataset_desc_file}")
        
        deface_success = run_pydeface(
            rawdata_dir,
            participant_labels=[p for p in participant_labels if p not in failed_participants],
            session=session,
            venv_cmd=venv_cmd
        )
        if not deface_success:
            logger.warning("Defacing step had some failures, but conversion was successful")
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"DICOM Import Summary:")
    logger.info(f"  Successful: {success_count}/{len(participant_labels)}")
    if failed_participants:
        logger.info(f"  Failed: {', '.join(failed_participants)}")
    logger.info(f"{'='*60}\n")
    
    return success_count > 0


def run_pydeface(
    rawdata_dir: Path,
    participant_labels: List[str],
    session: Optional[str] = None,
    venv_cmd: str = ""
) -> bool:
    """Run pydeface on anatomical images using bids-pydeface container.
    
    Parameters
    ----------
    rawdata_dir : Path
        Path to BIDS rawdata directory
    participant_labels : List[str]
        List of participant IDs
    session : Optional[str]
        Session label if applicable
    venv_cmd : str
        Virtual environment activation command
        
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    # Look for pydeface singularity image
    pydeface_img = Path.home() / "singularities" / "cbinyu.bids_pydeface.v2.0.6.sif"
    
    if not pydeface_img.exists():
        logger.warning(f"pydeface singularity image not found at {pydeface_img}")
        logger.info("Skipping defacing step. Install with:")
        logger.info("  singularity pull docker://cbinyu/bids-pydeface:v2.0.6")
        return False
    
    # Build participant label list
    participant_str = ' '.join([p.replace('sub-', '') for p in participant_labels])
    
    # Run pydeface
    cmd = f"singularity run -B {rawdata_dir}:/data {pydeface_img} "
    cmd += f"/data /data/derivatives participant --skip_bids_validator "
    cmd += f"--participant_label {participant_str}"
    
    if session:
        cmd += f" --session {session}"
    
    try:
        logger.info("Running pydeface...")
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            capture_output=True,
            text=True,
            executable='/bin/bash'
        )
        logger.info("✓ Defacing completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Defacing failed: {e.stderr}")
        return False
