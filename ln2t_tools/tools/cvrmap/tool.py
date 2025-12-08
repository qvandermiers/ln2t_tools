"""
CVRmap tool implementation.

CVRmap performs cerebrovascular reactivity (CVR) mapping from BOLD fMRI data
and CO₂ physiological recordings or ROI-based probes. It generates CVR maps,
delay maps, and comprehensive quality control reports.

Documentation: https://github.com/arovai/cvrmap
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_CVRMAP_VERSION

logger = logging.getLogger(__name__)


class CvrMapTool(BaseTool):
    """CVRmap cerebrovascular reactivity mapping tool.
    
    CVRmap processes BOLD fMRI data with CO₂ physiological recordings to
    generate maps of cerebrovascular reactivity (%BOLD/mmHg) and hemodynamic
    delay (seconds).
    
    The tool requires:
    - fMRIPrep preprocessed data (with AROMA)
    - Physiological recordings (CO₂ traces) OR ROI-based probe configuration
    
    Key features:
    - BIDS-compatible input/output
    - 4-step denoising pipeline
    - Cross-correlation delay mapping
    - Interactive HTML reports
    """
    
    name = "cvrmap"
    help_text = "Cerebrovascular reactivity mapping from BOLD fMRI"
    description = """
CVRmap - Cerebrovascular Reactivity Mapping

CVRmap processes BOLD fMRI data with CO₂ physiological recordings to
generate quantitative maps of cerebrovascular reactivity (CVR).

Key outputs include:
  - CVR maps: Vascular reactivity (%BOLD/mmHg)
  - Delay maps: Hemodynamic response timing (seconds)
  - Correlation maps: Cross-correlation quality metrics
  - Interactive HTML reports with quality control figures

Prerequisites:
  - fMRIPrep preprocessed data (with AROMA enabled)
  - Physiological recordings (CO₂) OR ROI-based probe analysis

Documentation: https://github.com/arovai/cvrmap

Typical runtime: 10-30 minutes per subject
"""
    default_version = DEFAULT_CVRMAP_VERSION
    requires_gpu = False
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add CVRmap-specific CLI arguments."""
        parser.add_argument(
            "--task",
            type=str,
            required=True,
            help="Task name for CVR analysis (e.g., 'gas', 'breathhold', 'rest')"
        )
        parser.add_argument(
            "--fmriprep-version",
            type=str,
            default=None,
            help="fMRIPrep version to use as input (default: auto-detect)"
        )
        parser.add_argument(
            "--space",
            type=str,
            default="MNI152NLin2009cAsym",
            help="Output space for CVR maps (default: MNI152NLin2009cAsym)"
        )
        parser.add_argument(
            "--baseline-method",
            type=str,
            choices=["peakutils", "mean"],
            default="peakutils",
            help="Baseline computation method: 'peakutils' for gas challenges, "
                 "'mean' for resting-state (default: peakutils)"
        )
        parser.add_argument(
            "--n-jobs",
            type=int,
            default=-1,
            help="Number of parallel jobs (-1 = all CPUs, default: -1)"
        )
        parser.add_argument(
            "--roi-probe",
            action="store_true",
            help="Use ROI-based probe instead of physiological recordings"
        )
        parser.add_argument(
            "--roi-coordinates",
            type=float,
            nargs=3,
            metavar=("X", "Y", "Z"),
            help="ROI center coordinates in mm (e.g., 0 -52 26 for PCC)"
        )
        parser.add_argument(
            "--roi-radius",
            type=float,
            default=6.0,
            help="ROI radius in mm (default: 6.0)"
        )
        parser.add_argument(
            "--roi-mask",
            type=Path,
            help="Path to ROI mask file (alternative to coordinates)"
        )
        parser.add_argument(
            "--config",
            type=Path,
            help="Path to custom YAML configuration file"
        )
        parser.add_argument(
            "--debug-level",
            type=int,
            choices=[0, 1],
            default=0,
            help="Verbosity level: 0=info, 1=debug (default: 0)"
        )
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate CVRmap arguments.
        
        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if arguments are valid
        """
        # Validate ROI probe options
        if getattr(args, 'roi_probe', False):
            has_coords = getattr(args, 'roi_coordinates', None) is not None
            has_mask = getattr(args, 'roi_mask', None) is not None
            
            if not has_coords and not has_mask:
                logger.error(
                    "--roi-probe requires either --roi-coordinates or --roi-mask"
                )
                return False
            
            if has_coords and has_mask:
                logger.warning(
                    "Both --roi-coordinates and --roi-mask provided; "
                    "using --roi-mask"
                )
        
        # Check config file exists if provided
        config = getattr(args, 'config', None)
        if config and not config.exists():
            logger.error(f"Configuration file not found: {config}")
            return False
        
        return True
    
    @classmethod
    def check_requirements(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace
    ) -> bool:
        """Check if fMRIPrep preprocessed data exists.
        
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
        # Check for fMRIPrep derivatives
        # This is a simplified check - actual fMRIPrep path will be resolved at runtime
        task = getattr(args, 'task', None)
        if not task:
            logger.error("--task is required for CVRmap")
            return False
        
        # Check for functional data with the specified task
        bold_files = layout.get(
            subject=participant_label,
            task=task,
            suffix='bold',
            extension=['.nii', '.nii.gz'],
            return_type='filename'
        )
        
        if not bold_files:
            logger.warning(
                f"No BOLD data found for participant {participant_label} "
                f"with task '{task}'"
            )
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
        """Get CVRmap output directory path.
        
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
        output_label = args.output_label or f"cvrmap_{version}"
        
        # Build subject directory
        subdir = f"sub-{participant_label}"
        
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
        """Build CVRmap Apptainer command.
        
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
        output_label = args.output_label or f"cvrmap_{version}"
        
        # Find fMRIPrep derivatives directory
        fmriprep_version = getattr(args, 'fmriprep_version', None)
        fmriprep_dir = cls._find_fmriprep_dir(dataset_derivatives, fmriprep_version)
        
        if not fmriprep_dir:
            logger.error(
                f"fMRIPrep derivatives not found in {dataset_derivatives}. "
                "Please run fMRIPrep first or specify --fmriprep-version."
            )
            return []
        
        logger.info(f"Using fMRIPrep derivatives: {fmriprep_dir}")
        
        # Build command using utility function
        cmd = build_apptainer_cmd(
            tool="cvrmap",
            rawdata=str(dataset_rawdata),
            derivatives=str(dataset_derivatives),
            participant_label=participant_label,
            apptainer_img=apptainer_img,
            output_label=output_label,
            fmriprep_dir=str(fmriprep_dir),
            task=getattr(args, 'task', 'gas'),
            space=getattr(args, 'space', 'MNI152NLin2009cAsym'),
            baseline_method=getattr(args, 'baseline_method', 'peakutils'),
            n_jobs=getattr(args, 'n_jobs', -1),
            debug_level=getattr(args, 'debug_level', 0),
            roi_probe=getattr(args, 'roi_probe', False),
            roi_coordinates=getattr(args, 'roi_coordinates', None),
            roi_radius=getattr(args, 'roi_radius', 6.0),
            roi_mask=getattr(args, 'roi_mask', None),
            config=getattr(args, 'config', None),
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
        """Process a subject with CVRmap.
        
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
            True if processing was successful
        """
        from ln2t_tools.utils.utils import launch_apptainer
        
        # Check requirements
        if not cls.check_requirements(layout, participant_label, args):
            return False
        
        # Check if output already exists
        output_dir = cls.get_output_dir(
            dataset_derivatives, participant_label, args
        )
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
        )
        
        if not cmd:
            logger.error(f"Failed to build command for {participant_label}")
            return False
        
        # Launch
        try:
            cmd_str = cmd[0] if isinstance(cmd, list) and len(cmd) == 1 else ' '.join(cmd)
            logger.info(f"Running CVRmap for participant {participant_label}")
            logger.info(f"Task: {args.task}")
            logger.info(f"Space: {args.space}")
            launch_apptainer(cmd_str)
            return True
        except Exception as e:
            logger.error(f"Error processing {participant_label}: {e}")
            return False
    
    # Helper methods
    
    @staticmethod
    def _find_fmriprep_dir(
        derivatives_dir: Path,
        version: Optional[str] = None
    ) -> Optional[Path]:
        """Find fMRIPrep derivatives directory.
        
        Parameters
        ----------
        derivatives_dir : Path
            Base derivatives directory
        version : Optional[str]
            Specific fMRIPrep version to look for
            
        Returns
        -------
        Optional[Path]
            Path to fMRIPrep directory, or None if not found
        """
        if version:
            fmriprep_dir = derivatives_dir / f"fmriprep_{version}"
            if fmriprep_dir.exists():
                return fmriprep_dir
            return None
        
        # Auto-detect: find the first fmriprep_* directory
        for item in derivatives_dir.iterdir():
            if item.is_dir() and item.name.startswith("fmriprep_"):
                return item
        
        return None
