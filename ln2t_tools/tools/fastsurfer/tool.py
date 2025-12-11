"""
FastSurfer tool implementation.

FastSurfer is a fast and accurate deep-learning based neuroimaging pipeline
that provides FreeSurfer-compatible outputs with significantly reduced
processing time. It consists of:
  - FastSurferCNN: Deep learning segmentation (~5 min on GPU)
  - recon-surf: Surface reconstruction (~60-90 min)
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_FASTSURFER_VERSION

logger = logging.getLogger(__name__)


class FastSurferTool(BaseTool):
    """FastSurfer deep-learning based neuroimaging tool.
    
    FastSurfer is a fast and extensively validated deep-learning pipeline
    for the fully automated processing of structural human brain MRIs.
    It provides FreeSurfer-compatible outputs with dramatically reduced
    processing time.
    
    Key features:
    - FastSurferCNN: Whole brain segmentation into 95 classes in ~5 minutes (GPU)
    - CerebNet: Cerebellum sub-segmentation
    - HypVINN: Hypothalamus sub-segmentation
    - recon-surf: Cortical surface reconstruction in ~60-90 minutes
    - Full FreeSurfer compatibility for downstream analysis
    """
    
    name = "fastsurfer"
    help_text = "FastSurfer deep-learning brain segmentation and surface reconstruction"
    description = """
FastSurfer Deep-Learning Neuroimaging Pipeline

FastSurfer provides fast and accurate brain MRI analysis using deep learning:

  Segmentation (~5 min on GPU):
    - Whole brain segmentation (95 classes, DKTatlas-compatible)
    - Cerebellum sub-segmentation (CerebNet)
    - Hypothalamus sub-segmentation (HypVINN)
    - Bias field correction and partial volume statistics

  Surface Reconstruction (~60-90 min):
    - Cortical surface reconstruction (white/pial surfaces)
    - Spherical mapping and registration
    - Cortical thickness and parcellation
    - Full FreeSurfer output compatibility

Processing modes:
  - Full pipeline (default): segmentation + surface reconstruction
  - Segmentation only (--seg-only): ~5 min, volumetric outputs only
  - Surface only (--surf-only): requires prior segmentation

Typical runtime: ~1-1.5 hours (full), ~5 min (seg-only) on GPU
"""
    default_version = DEFAULT_FASTSURFER_VERSION
    requires_gpu = True  # GPU strongly recommended for deep learning
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add FastSurfer-specific CLI arguments.
        
        NOTE: Tool-specific arguments are now passed via --tool-args.
        This method is kept for compatibility but adds no arguments.
        
        FastSurfer accepts the following arguments via --tool-args:
        
        Processing modes:
          --seg-only          Segmentation only (skip surface recon)
          --surf-only         Surface reconstruction only
          
        Hardware options:
          --threads N         Number of threads (default: 4)
          --device DEVICE     Device for inference: auto, cpu, cuda, mps
          --vox-size SIZE     Voxel size: 'min' or 0.7-1.0 (default: min)
          
        Atlas options:
          --3T                Use 3T atlas for Talairach registration
          
        Segmentation options:
          --no-cereb          Skip cerebellum sub-segmentation
          --no-hypothal       Skip hypothalamus sub-segmentation  
          --no-biasfield      Skip bias field correction
          
        Additional inputs:
          --t2 PATH           T2w image for hypothalamus segmentation
        
        Parameters
        ----------
        parser : argparse.ArgumentParser
            The subparser for this tool
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate FastSurfer arguments.
        
        With the --tool-args pattern, validation is handled by the container.
        This method simply returns True.
        
        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True (container handles validation)
        """
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
        """Get FastSurfer output directory path.
        
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
        output_label = args.output_label or f"fastsurfer_{version}"
        
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
        """Build FastSurfer Apptainer command.
        
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
        
        version = args.version or cls.default_version
        output_label = args.output_label or f"fastsurfer_{version}"
        
        # Log processing info
        tool_args = getattr(args, 'tool_args', '') or ''
        if '--seg-only' in tool_args:
            logger.info("Running FastSurfer in segmentation-only mode (~5 min on GPU)")
            logger.info("  Surface reconstruction will be skipped")
        elif '--surf-only' in tool_args:
            logger.info("Running FastSurfer in surface-only mode")
            logger.info("  Requires prior segmentation to exist")
        else:
            logger.info("Running full FastSurfer pipeline")
            logger.info("  Segmentation: ~5 min on GPU")
            logger.info("  Surface reconstruction: ~60-90 min")
        
        # Build command using utility function with tool_args pass-through
        cmd = build_apptainer_cmd(
            tool="fastsurfer",
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
        """Process all T1w images for a subject with FastSurfer.
        
        FastSurfer processes each T1w image separately, so this method
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
            cls._log_input_files(t1w, layout, participant_label, session, run)
            
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
    def _log_input_files(
        t1w: str,
        layout: BIDSLayout,
        participant_label: str,
        session: Optional[str],
        run: Optional[str]
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
