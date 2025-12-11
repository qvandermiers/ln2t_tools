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
from ln2t_tools.utils.defaults import DEFAULT_CVRMAP_VERSION, DEFAULT_CVRMAP_FMRIPREP_VERSION

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
        """Add CVRmap-specific CLI arguments.
        
        NOTE: Tool-specific arguments are now passed via --tool-args.
        This method is kept for compatibility but adds no arguments.
        
        CVRmap accepts the following arguments via --tool-args:
        
        Data selection:
          --task TASK             Task name (e.g., 'gas', 'breathhold', 'rest')
          --fmriprep-version VER  fMRIPrep version to use as input
          --space SPACE           Output space (default: MNI152NLin2009cAsym)
          
        Analysis options:
          --baseline-method METHOD  'peakutils' or 'mean' (default: peakutils)
          --n-jobs N                Parallel jobs (-1 = all CPUs)
          --config FILE             Custom YAML configuration
          --debug-level N           Verbosity: 0=info, 1=debug
          
        ROI probe options:
          --roi-probe               Use ROI-based probe instead of physio
          --roi-coordinates X Y Z   ROI center in mm (e.g., 0 -52 26)
          --roi-radius R            ROI radius in mm (default: 6.0)
          --roi-mask FILE           ROI mask file (alternative to coordinates)
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate CVRmap arguments.
        
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
        """Check if BOLD data exists for this participant.
        
        With --tool-args pattern, detailed validation is handled by the container.
        This method just checks for any BOLD data.
        
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
        # Check for functional data
        query_params = {
            'subject': participant_label,
            'suffix': 'bold',
            'extension': ['.nii', '.nii.gz'],
            'return_type': 'filename'
        }
        
        bold_files = layout.get(**query_params)
        
        if not bold_files:
            logger.warning(
                f"No BOLD data found for participant {participant_label}"
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
        
        # Get tool_args from user
        tool_args = getattr(args, 'tool_args', '') or ''
        
        # Find fMRIPrep derivatives directory (use default version)
        fmriprep_dir = cls._find_fmriprep_dir(dataset_derivatives, DEFAULT_CVRMAP_FMRIPREP_VERSION)
        
        if not fmriprep_dir:
            logger.warning(
                f"fMRIPrep derivatives not found at default location in {dataset_derivatives}. "
                f"Specify --fmriprep-version via --tool-args if using a different version."
            )
            # Still continue - container will validate
        else:
            logger.info(f"Using fMRIPrep derivatives: {fmriprep_dir}")
        
        # Build command using utility function with tool_args pass-through
        cmd = build_apptainer_cmd(
            tool="cvrmap",
            rawdata=str(dataset_rawdata),
            derivatives=str(dataset_derivatives),
            participant_label=participant_label,
            apptainer_img=apptainer_img,
            output_label=output_label,
            fmriprep_dir=str(fmriprep_dir) if fmriprep_dir else None,
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
            task = getattr(args, 'task', None)
            if task:
                logger.info(f"Task: {task}")
            else:
                logger.info("Task: auto-discover")
                # Space is now passed via --tool-args; cannot access as attribute
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
