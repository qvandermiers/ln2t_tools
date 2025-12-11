"""
fMRIPrep tool implementation.

fMRIPrep is a robust preprocessing pipeline for functional MRI data that
performs motion correction, slice timing correction, distortion correction,
coregistration, normalization, and confound extraction.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_FMRIPREP_VERSION, DEFAULT_FS_VERSION

logger = logging.getLogger(__name__)


class FMRIPrepTool(BaseTool):
    """fMRIPrep functional MRI preprocessing tool.
    
    fMRIPrep is a robust and easy-to-use preprocessing pipeline for
    functional MRI data. It automatically adapts its processing decisions
    based on the input data.
    
    Key features:
    - Automatic skull-stripping and brain extraction
    - Motion correction and realignment
    - Slice timing correction
    - Distortion correction (fieldmap-based or fieldmap-less)
    - Coregistration to anatomical image
    - Surface projection (if FreeSurfer is available)
    - Spatial normalization to standard spaces
    - Confound extraction for denoising
    """
    
    name = "fmriprep"
    help_text = "fMRIPrep functional MRI preprocessing"
    description = """
fMRIPrep Functional MRI Preprocessing

fMRIPrep is a robust preprocessing pipeline for functional MRI data.
Key outputs include:

  - Preprocessed BOLD time series in native and standard spaces
  - Confound time series for denoising (motion, physiological, etc.)
  - Surface-mapped functional data (if FreeSurfer available)
  - Quality assessment reports

The pipeline automatically:
  - Detects and uses available fieldmaps for distortion correction
  - Uses existing FreeSurfer output if available
  - Adapts processing based on input data characteristics

Typical runtime: 2-6 hours per subject (depending on data)
"""
    default_version = DEFAULT_FMRIPREP_VERSION
    requires_gpu = False
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add fMRIPrep-specific CLI arguments.
        
        Tool-specific options should be passed via --tool-args.
        This method is kept for backward compatibility but adds no arguments.
        
        Example usage with --tool-args:
            ln2t_tools fmriprep --dataset MYDATA --participant-label 001 \\
                --tool-args "--output-spaces MNI152NLin2009cAsym:res-2 --nprocs 8"
                
        Common fMRIPrep options (pass via --tool-args):
            --output-spaces <spaces>  : Output template spaces
            --fs-no-reconall          : Skip FreeSurfer surface reconstruction
            --use-aroma               : Enable ICA-AROMA denoising
            --nprocs <n>              : Number of processes
            --omp-nthreads <n>        : Number of OpenMP threads
            --ignore fieldmaps        : Ignore fieldmap correction
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate fMRIPrep arguments.
        
        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if arguments are valid
        """
        # Validate output_spaces format if needed
        return True
    
    @classmethod
    def check_requirements(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace
    ) -> bool:
        """Check if T1w and BOLD images exist for this participant.
        
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
            True if requirements are met
        """
        # Check for T1w
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
        
        # Check for BOLD
        func_files = layout.get(
            subject=participant_label,
            scope="raw",
            suffix="bold",
            extension=".nii.gz",
            return_type="filename"
        )
        
        if not func_files:
            logger.warning(f"No functional data found for participant {participant_label}")
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
        """Get fMRIPrep output directory path.
        
        Parameters
        ----------
        dataset_derivatives : Path
            Base derivatives directory
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
        session : Optional[str]
            Session label (not used for fMRIPrep)
        run : Optional[str]
            Run label (not used for fMRIPrep)
            
        Returns
        -------
        Path
            Full path to output directory
        """
        version = args.version or cls.default_version
        output_label = args.output_label or f"fmriprep_{version}"
        
        return dataset_derivatives / output_label / f"sub-{participant_label}"
    
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
        """Build fMRIPrep Apptainer command.
        
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
            Additional parameters
            
        Returns
        -------
        List[str]
            Command as list of strings
        """
        from ln2t_tools.utils.utils import build_apptainer_cmd
        
        version = args.version or cls.default_version
        output_label = args.output_label or f"fmriprep_{version}"
        output_dir = dataset_derivatives / output_label
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get tool_args from user
        tool_args = getattr(args, 'tool_args', '') or ''
        
        cmd = build_apptainer_cmd(
            tool="fmriprep",
            fs_license=args.fs_license,
            rawdata=str(dataset_rawdata),
            derivatives=str(output_dir),
            participant_label=participant_label,
            apptainer_img=apptainer_img,
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
        """Process a subject with fMRIPrep.
        
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
            True if processing succeeded
        """
        from ln2t_tools.utils.utils import launch_apptainer
        
        # Check requirements
        if not cls.check_requirements(layout, participant_label, args):
            return False
        
        # Check if output already exists
        output_dir = cls.get_output_dir(dataset_derivatives, participant_label, args)
        if output_dir.exists():
            logger.info(f"Output exists, skipping: {output_dir}")
            return True
        
        # Build command
        cmd = cls.build_command(
            layout=layout,
            participant_label=participant_label,
            args=args,
            dataset_rawdata=dataset_rawdata,
            dataset_derivatives=dataset_derivatives,
            apptainer_img=apptainer_img,
            **kwargs
        )
        
        if not cmd:
            logger.error(f"Failed to build command for {participant_label}")
            return False
        
        # Launch
        try:
            cmd_str = cmd[0] if isinstance(cmd, list) and len(cmd) == 1 else ' '.join(cmd)
            launch_apptainer(cmd_str)
            return True
        except Exception as e:
            logger.error(f"Error processing {participant_label} with fMRIPrep: {e}")
            return False
