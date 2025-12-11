"""
FreeSurfer tool implementation.

FreeSurfer performs cortical reconstruction and segmentation from T1-weighted
MRI images, optionally using T2w or FLAIR images for improved pial surface
estimation.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_FS_VERSION

logger = logging.getLogger(__name__)


class FreeSurferTool(BaseTool):
    """FreeSurfer cortical reconstruction tool.
    
    FreeSurfer provides a full processing stream for structural MRI data,
    including skull stripping, cortical surface reconstruction, cortical
    and subcortical segmentation, and surface-based registration.
    
    This tool automatically detects T2w and FLAIR images when available
    and uses them to improve pial surface estimation.
    """
    
    name = "freesurfer"
    help_text = "FreeSurfer cortical reconstruction"
    description = """
FreeSurfer Cortical Reconstruction

FreeSurfer provides automated cortical reconstruction and volumetric
segmentation of brain MRI data. Key outputs include:

  - Skull-stripped brain volumes
  - White matter and pial surface meshes
  - Cortical parcellations (Desikan-Killiany, Destrieux)
  - Subcortical segmentation
  - Cortical thickness maps

The tool automatically uses T2w or FLAIR images when available for
improved pial surface estimation.

Typical runtime: 6-12 hours per subject
"""
    default_version = DEFAULT_FS_VERSION
    requires_gpu = False
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add FreeSurfer-specific CLI arguments.
        
        Tool-specific options should be passed via --tool-args.
        This method is kept for backward compatibility but adds no arguments.
        
        Example usage with --tool-args:
            ln2t_tools freesurfer --dataset MYDATA --participant-label 001 \\
                --tool-args "-autorecon1"
                
        Common FreeSurfer options (pass via --tool-args):
            -autorecon1     : Motion correction, normalization, skull stripping only
            -T2 <file>      : Use T2w for pial surface (auto-detected if available)
            -FLAIR <file>   : Use FLAIR for pial surface (auto-detected if available)
            -parallel       : Run hemispheres in parallel
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate FreeSurfer arguments.
        
        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if arguments are valid
        """
        # No specific validation needed for FreeSurfer
        return True
    
    @classmethod
    def check_requirements(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace
    ) -> bool:
        """Check if T1w image exists for this participant.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if T1w image is found
        """
        t1w_files = layout.get(
            subject=participant_label,
            scope="raw",
            suffix="T1w",
            extension=".nii.gz",
            return_type="filename"
        )
        
        if not t1w_files:
            logger.warning(f"No T1w images found for participant {participant_label}")
            return False
        
        return True
    
    @classmethod
    def get_output_dir(
        cls,
        dataset_derivatives: Path,
        participant_label: str,
        args: argparse.Namespace,
        session: Optional[str] = None,
        run: Optional[str] = None
    ) -> Path:
        """Get FreeSurfer output directory path.
        
        Parameters
        ----------
        dataset_derivatives : Path
            Base derivatives directory
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
        session : Optional[str]
            Session label (without 'ses-' prefix)
        run : Optional[str]
            Run label
            
        Returns
        -------
        Path
            Full path to output directory
        """
        version = args.version or cls.default_version
        output_label = args.output_label or f"freesurfer_{version}"
        
        # Build subject directory
        subdir = cls._build_subdir(participant_label, session, run)
        
        return dataset_derivatives / output_label / subdir
    
    @classmethod
    def build_command(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace,
        dataset_rawdata: Path,
        dataset_derivatives: Path,
        apptainer_img: str,
        **kwargs
    ) -> List[str]:
        """Build FreeSurfer Apptainer command.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
        dataset_rawdata : Path
            Path to BIDS rawdata directory
        dataset_derivatives : Path
            Path to derivatives directory
        apptainer_img : str
            Path to Apptainer image
        **kwargs : dict
            Additional parameters:
            - t1w: Path to specific T1w file (optional)
            - session: Session label (optional)
            - run: Run label (optional)
            
        Returns
        -------
        List[str]
            Command as list of strings
        """
        from ln2t_tools.utils.utils import build_apptainer_cmd
        
        t1w = kwargs.get('t1w')
        session = kwargs.get('session')
        run = kwargs.get('run')
        
        # If no specific T1w provided, get the first one
        if not t1w:
            t1w_files = layout.get(
                subject=participant_label,
                scope="raw",
                suffix="T1w",
                extension=".nii.gz",
                return_type="filename"
            )
            if t1w_files:
                t1w = t1w_files[0]
                entities = layout.parse_file_entities(t1w)
                session = entities.get('session')
                run = entities.get('run')
        
        # Get additional contrasts (T2w/FLAIR for pial surface improvement)
        additional_contrasts = cls._get_additional_contrasts(
            layout, participant_label, session, run
        )
        
        # Build FreeSurfer options for additional contrasts (T2w/FLAIR)
        fs_options = cls._build_fs_options(
            additional_contrasts, dataset_rawdata
        )
        
        version = args.version or cls.default_version
        output_label = args.output_label or f"freesurfer_{version}"
        
        # Combine auto-detected options with user-provided tool_args
        tool_args = getattr(args, 'tool_args', '') or ''
        if fs_options:
            # Prepend auto-detected options to user tool_args
            auto_options = " ".join(fs_options)
            tool_args = f"{auto_options} {tool_args}".strip()
        
        # Build command using utility function
        cmd = build_apptainer_cmd(
            tool="freesurfer",
            fs_license=args.fs_license,
            rawdata=str(dataset_rawdata),
            derivatives=str(dataset_derivatives),
            participant_label=participant_label,
            t1w=t1w,
            apptainer_img=apptainer_img,
            output_label=output_label,
            session=session,
            run=run,
            tool_args=tool_args
        )
        
        return [cmd] if isinstance(cmd, str) else cmd
    
    @classmethod
    def process_subject(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace,
        dataset_rawdata: Path,
        dataset_derivatives: Path,
        apptainer_img: str,
        **kwargs
    ) -> bool:
        """Process all T1w images for a subject with FreeSurfer.
        
        FreeSurfer processes each T1w image separately, so this method
        iterates over all T1w files found for the participant.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
        dataset_rawdata : Path
            Path to BIDS rawdata directory
        dataset_derivatives : Path
            Path to derivatives directory
        apptainer_img : str
            Path to Apptainer image
            
        Returns
        -------
        bool
            True if all T1w images processed successfully
        """
        from ln2t_tools.utils.utils import launch_apptainer
        
        # Check requirements
        if not cls.check_requirements(layout, participant_label, args):
            return False
        
        # Get all T1w files
        t1w_files = layout.get(
            subject=participant_label,
            scope="raw",
            suffix="T1w",
            extension=".nii.gz",
            return_type="filename"
        )
        
        success = True
        for t1w in t1w_files:
            entities = layout.parse_file_entities(t1w)
            session = entities.get('session')
            run = entities.get('run')
            
            # Check if output already exists
            output_dir = cls.get_output_dir(
                dataset_derivatives, participant_label, args, session, run
            )
            if output_dir.exists():
                logger.info(f"Output exists, skipping: {output_dir}")
                continue
            
            # Log input files
            cls._log_input_files(t1w, layout, participant_label, session, run, dataset_rawdata)
            
            # Build command
            cmd = cls.build_command(
                layout=layout,
                participant_label=participant_label,
                args=args,
                dataset_rawdata=dataset_rawdata,
                dataset_derivatives=dataset_derivatives,
                apptainer_img=apptainer_img,
                t1w=t1w,
                session=session,
                run=run
            )
            
            if not cmd:
                logger.error(f"Failed to build command for {participant_label}")
                success = False
                continue
            
            # Launch
            try:
                cmd_str = cmd[0] if isinstance(cmd, list) and len(cmd) == 1 else ' '.join(cmd)
                launch_apptainer(cmd_str)
            except Exception as e:
                logger.error(f"Error processing {participant_label}: {e}")
                success = False
        
        return success
    
    # Helper methods
    
    @staticmethod
    def _build_subdir(
        participant_label: str,
        session: Optional[str] = None,
        run: Optional[str] = None
    ) -> str:
        """Build BIDS-compliant subject directory name."""
        parts = [f"sub-{participant_label}"]
        if session:
            parts.append(f"ses-{session}")
        if run:
            parts.append(f"run-{run}")
        return "_".join(parts)
    
    @staticmethod
    def _get_additional_contrasts(
        layout: BIDSLayout,
        participant_label: str,
        session: Optional[str],
        run: Optional[str]
    ) -> dict:
        """Get T2w and FLAIR images if available."""
        contrasts = {'t2w': None, 'flair': None}
        
        # Build query filters
        filters = {
            'subject': participant_label,
            'scope': 'raw',
            'extension': '.nii.gz',
            'return_type': 'filename'
        }
        if session:
            filters['session'] = session
        
        # Look for T2w
        t2w_files = layout.get(suffix='T2w', **filters)
        if t2w_files:
            contrasts['t2w'] = t2w_files[0]
        
        # Look for FLAIR
        flair_files = layout.get(suffix='FLAIR', **filters)
        if flair_files:
            contrasts['flair'] = flair_files[0]
        
        return contrasts
    
    @staticmethod
    def _build_fs_options(
        additional_contrasts: dict,
        dataset_rawdata: Path
    ) -> List[str]:
        """Build FreeSurfer command options for additional contrasts."""
        fs_options = []
        
        if additional_contrasts['t2w']:
            logger.info(f"Found T2w image")
            t2w_host = Path(additional_contrasts['t2w'])
            try:
                t2w_relative = t2w_host.relative_to(dataset_rawdata)
                t2w_container = f"/rawdata/{t2w_relative}"
            except ValueError:
                t2w_container = str(t2w_host)
            fs_options.append(f"-T2 {t2w_container}")
            fs_options.append("-T2pial")
        
        if additional_contrasts['flair']:
            logger.info(f"Found FLAIR image")
            flair_host = Path(additional_contrasts['flair'])
            try:
                flair_relative = flair_host.relative_to(dataset_rawdata)
                flair_container = f"/rawdata/{flair_relative}"
            except ValueError:
                flair_container = str(flair_host)
            fs_options.append(f"-FLAIR {flair_container}")
            fs_options.append("-FLAIRpial")
            if additional_contrasts['t2w']:
                logger.info("Both T2w and FLAIR found, using only FLAIR for pial surface")
        
        return fs_options
    
    @staticmethod
    def _log_input_files(
        t1w: str,
        layout: BIDSLayout,
        participant_label: str,
        session: Optional[str],
        run: Optional[str],
        dataset_rawdata: Path
    ) -> None:
        """Log input file information."""
        logger.info("Verifying input files exist on host:")
        t1w_path = Path(t1w)
        logger.info(f"  T1w: {t1w_path}")
        if t1w_path.exists():
            logger.info(f"    ✓ File exists (size: {t1w_path.stat().st_size / (1024*1024):.2f} MB)")
        else:
            logger.error(f"    ✗ File NOT found!")
            raise FileNotFoundError(f"T1w file not found: {t1w_path}")
        
        additional = FreeSurferTool._get_additional_contrasts(
            layout, participant_label, session, run
        )
        
        for name, path in [('T2w', additional['t2w']), ('FLAIR', additional['flair'])]:
            if path:
                p = Path(path)
                logger.info(f"  {name}: {p}")
                if p.exists():
                    logger.info(f"    ✓ File exists (size: {p.stat().st_size / (1024*1024):.2f} MB)")
                else:
                    logger.warning(f"    ✗ File NOT found!")
