"""DICOM to BIDS conversion using dcm2bids."""

import logging
import subprocess
import shutil
from pathlib import Path
from typing import List, Optional, Tuple
import tarfile
import hashlib
import os

logger = logging.getLogger(__name__)


def discover_participants_from_dicom_dir(
    dicom_dir: Path,
    ds_initials: str
) -> List[str]:
    """Discover participant labels from DICOM directory.
    
    Scans the dicom directory for folders and archives matching the dataset
    initials pattern and extracts participant labels.
    
    Parameters
    ----------
    dicom_dir : Path
        Path to dicom directory (e.g., sourcedata/dicom)
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
    
    for item in dicom_dir.iterdir():
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
    logger.info(f"Discovered {len(result)} participants from {dicom_dir}: {result}")
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
    dicom_dir: Path,
    source_name: str
) -> Tuple[Optional[Path], bool]:
    """Extract archive if directory doesn't exist but archive does.
    
    Parameters
    ----------
    dicom_dir : Path
        Path to dicom directory
    source_name : str
        Name of the source directory (e.g., 'CB042')
        
    Returns
    -------
    Tuple[Optional[Path], bool]
        (Path to source directory or None if not found, True if was extracted from archive)
    """
    source_path = dicom_dir / source_name
    archive_path = dicom_dir / f"{source_name}.tar.gz"
    
    # If directory exists, use it directly
    if source_path.exists():
        return source_path, False
    
    # If archive exists, extract it
    if archive_path.exists():
        logger.info(f"Directory {source_name} not found, extracting from archive...")
        try:
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(path=dicom_dir)
            
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


def import_dicom(
    dataset: str,
    participant_labels: Optional[List[str]],
    sourcedata_dir: Path,
    rawdata_dir: Path,
    ds_initials: Optional[str] = None,
    session: Optional[str] = None,
    compress_source: bool = False,
    deface: bool = False,
    venv_path: Optional[Path] = None,
    keep_tmp_files: bool = False,
    overwrite: bool = False
) -> bool:
    """Import DICOM data to BIDS format using dcm2bids.
    
    Parameters
    ----------
    dataset : str
        Dataset name
    participant_labels : Optional[List[str]]
        List of participant IDs (without 'sub-' prefix). If None, discovers
        participants automatically from the dicom directory.
    sourcedata_dir : Path
        Path to sourcedata directory (e.g., ~/sourcedata/<dataset>-sourcedata)
    rawdata_dir : Path
        Path to BIDS rawdata directory
    ds_initials : Optional[str]
        Dataset initials prefix (e.g., 'CB', 'HP'). If None, tries to infer from directory structure
    session : Optional[str]
        Session label (without 'ses-' prefix)
    compress_source : bool
        Whether to compress source DICOM directories after successful conversion.
        If compression is successful, the original directory is deleted.
    deface : bool
        Deface anatomical images after import
    venv_path : Optional[Path]
        Path to virtual environment containing dcm2bids
    keep_tmp_files : bool
        Keep temporary files created by dcm2bids (tmp_dcm2bids directory)
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
        logger.info("No participant labels provided, discovering from dicom directory...")
        participant_labels = discover_participants_from_dicom_dir(dicom_dir, ds_initials)
        
        if not participant_labels:
            logger.error(f"No participants found in {dicom_dir} matching pattern {ds_initials}*")
            return False
    
    # Filter out existing participants unless overwrite is enabled
    if not overwrite:
        new_participants = []
        for participant in participant_labels:
            participant_id = participant.replace('sub-', '')
            # Check if anatomical (anat) data already exists for this participant
            if session:
                anat_dir = rawdata_dir / f"sub-{participant_id}" / f"ses-{session}" / "anat"
            else:
                anat_dir = rawdata_dir / f"sub-{participant_id}" / "anat"
            
            if anat_dir.exists():
                logger.info(f"Participant {participant_id} already has DICOM data, skipping (use --overwrite to re-process)")
            else:
                new_participants.append(participant)
        
        if not new_participants:
            logger.info("All participants already have DICOM data. Skipping DICOM import.")
            return True
        
        participant_labels = new_participants
    
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
    extracted_archives = []  # Track archives that were extracted (to clean up later if needed)
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')  # Remove prefix if present
        
        # Determine source directory name using strict pattern
        # Use strict naming convention: AB042 or AB042SES4
        if session:
            source_name = f"{ds_initials}{participant_id}SES{session}"
        else:
            source_name = f"{ds_initials}{participant_id}"
        
        # Try to get source path (extract from archive if needed)
        source_path, was_extracted = extract_archive_if_needed(dicom_dir, source_name)
        
        if source_path is None:
            logger.error(f"Source DICOM not found: {source_name} (checked directory and .tar.gz archive)")
            logger.error(f"Expected naming convention: {ds_initials}{participant_id}" + 
                       (f"SES{session}" if session else ""))
            failed_participants.append(participant_id)
            continue
        
        if was_extracted:
            extracted_archives.append((source_path, dicom_dir / f"{source_name}.tar.gz"))
        
        # Run dcm2bids
        logger.info(f"Running dcm2bids for {participant_id}...")
        
        # Build dcm2bids command
        cmd = f"{venv_cmd}dcm2bids -o {rawdata_dir} -p {participant_id}"
        if session:
            cmd += f" -s {session}"
        cmd += f" -d {source_path} -c {config_file}"
        
        logger.debug(f"dcm2bids command: {cmd}")
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                executable='/bin/bash'
            )
            
            # Log dcm2bids output for debugging
            if result.stdout:
                logger.debug(f"dcm2bids stdout:\n{result.stdout}")
            if result.stderr:
                logger.debug(f"dcm2bids stderr:\n{result.stderr}")
            
            # Verify that subject directory was actually created
            expected_subj_dir = rawdata_dir / f"sub-{participant_id}"
            if not expected_subj_dir.exists():
                logger.error(
                    f"✗ dcm2bids completed but subject directory was NOT created: {expected_subj_dir}\n"
                    f"This indicates dcm2bids encountered an issue even though it reported success.\n"
                    f"Possible causes:\n"
                    f"  - dcm2bids config error\n"
                    f"  - DICOM files missing or corrupted\n"
                    f"  - dcm2bids output directory permissions issue\n"
                    f"Please review dcm2bids logs for details."
                )
                failed_participants.append(participant_id)
                
                # If we extracted from archive, still clean up
                if was_extracted and source_path.exists():
                    logger.info(f"Cleaning up extracted directory after failed conversion: {source_path.name}")
                    shutil.rmtree(source_path)
                continue
            
            logger.info(f"✓ Successfully imported {participant_id}")
            if result.stdout:
                logger.debug(result.stdout)
            success_count += 1
            
            # Compress source data if requested (only after successful conversion)
            if compress_source and not was_extracted:
                # Don't compress if we just extracted from an archive
                compressed_file = dicom_dir / f"{source_path.name}.tar.gz"
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
            logger.error(f"✗ Failed to import {participant_id}: {e.stderr}")
            failed_participants.append(participant_id)
            
            # If we extracted from archive and conversion failed, still clean up
            if was_extracted and source_path.exists():
                logger.info(f"Cleaning up extracted directory after failed conversion: {source_path.name}")
                shutil.rmtree(source_path)
            continue
    
    # Cleanup tmp_dcm2bids directory (unless --keep-tmp-files is set)
    tmp_dir = rawdata_dir / "tmp_dcm2bids"
    if tmp_dir.exists() and not keep_tmp_files:
        logger.info("Cleaning up tmp_dcm2bids directory...")
        shutil.rmtree(tmp_dir)
    elif tmp_dir.exists() and keep_tmp_files:
        logger.info(f"Keeping tmp_dcm2bids directory: {tmp_dir}")
    
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
        
        # Update JSON sidecars to mark images as defaced
        update_defaced_metadata(rawdata_dir, participant_labels, session)
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Defacing failed: {e.stderr}")
        return False


def update_defaced_metadata(
    rawdata_dir: Path,
    participant_labels: List[str],
    session: Optional[str] = None
) -> None:
    """Update JSON sidecar files to indicate images have been defaced.
    
    Parameters
    ----------
    rawdata_dir : Path
        Path to BIDS rawdata directory
    participant_labels : List[str]
        List of participant IDs
    session : Optional[str]
        Session label if applicable
    """
    import json
    from datetime import datetime
    
    for participant in participant_labels:
        participant_id = participant.replace('sub-', '')
        subj_dir = rawdata_dir / f"sub-{participant_id}"
        
        if not subj_dir.exists():
            continue
        
        # Determine which directory to search
        if session:
            search_dir = subj_dir / f"ses-{session}" / "anat"
        else:
            search_dir = subj_dir / "anat"
        
        if not search_dir.exists():
            continue
        
        # Find all anatomical NIfTI files
        for nii_file in search_dir.glob("*.nii*"):
            # Find corresponding JSON file
            json_file = nii_file.parent / f"{nii_file.name.split('.nii')[0]}.json"
            
            if json_file.exists():
                try:
                    # Read existing JSON
                    with open(json_file, 'r') as f:
                        metadata = json.load(f)
                    
                    # Add defacing information
                    metadata['Defaced'] = True
                    metadata['DefacingMethod'] = 'pydeface v2.0.6'
                    metadata['DefacingTimestamp'] = datetime.now().isoformat()
                    
                    # Write updated JSON
                    with open(json_file, 'w') as f:
                        json.dump(metadata, f, indent=4)
                    
                    logger.debug(f"Updated defacing metadata in {json_file.name}")
                    
                except Exception as e:
                    logger.warning(f"Could not update metadata for {json_file.name}: {e}")
            else:
                logger.debug(f"No JSON sidecar found for {nii_file.name}")
    
    logger.info("✓ Updated JSON metadata for defaced images")
