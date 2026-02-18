"""
QSIRecon tool implementation.

QSIRecon performs reconstruction and tractography from QSIPrep outputs,
supporting various diffusion models and connectome generation.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_QSIRECON_VERSION, DEFAULT_QSIPREP_VERSION

logger = logging.getLogger(__name__)


class QSIReconTool(BaseTool):
    """QSIRecon diffusion MRI reconstruction tool.
    
    QSIRecon performs reconstruction and tractography from preprocessed
    DWI data (from QSIPrep). It supports various reconstruction pipelines
    including MRtrix3-based multi-shell multi-tissue CSD and ACT.
    
    IMPORTANT: QSIRecon requires QSIPrep v1.1.1 preprocessed data as input.
    This is a hard requirement and cannot be changed.
    """
    
    name = "qsirecon"
    help_text = "QSIRecon diffusion MRI reconstruction"
    required_qsiprep_version = DEFAULT_QSIPREP_VERSION  # QSIRecon requires this specific version
    description = """
QSIRecon Diffusion MRI Reconstruction

QSIRecon performs reconstruction and tractography from QSIPrep v1.1.1 outputs.
QSIRecon requires QSIPrep v1.1.1 - other versions are not supported.

Available reconstruction specifications include:

  - mrtrix_multishell_msmt_ACT-hsvs: Multi-shell multi-tissue CSD with ACT
  - mrtrix_singleshell_ss3t_ACT-hsvs: Single-shell 3-tissue CSD with ACT
  - dsi_studio_gqi: DSI Studio GQI reconstruction
  - amico_noddi: NODDI fitting with AMICO

Key outputs:
  - Fiber orientation distributions (FODs)
  - Streamline tractography
  - Structural connectivity matrices
  - Per-bundle statistics

Typical runtime: 2-4 hours per subject

REQUIREMENT: QSIRecon requires QSIPrep v1.1.1 preprocessed data as input.
Please ensure QSIPrep has been run with version 1.1.1 before running QSIRecon.
"""
    default_version = DEFAULT_QSIRECON_VERSION
    requires_gpu = False
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add QSIRecon-specific CLI arguments.
        
        Tool-specific options should be passed via --tool-args.
        This method is kept for backward compatibility but adds no arguments.
        
        Example usage with --tool-args:
            ln2t_tools qsirecon --dataset MYDATA --participant-label 001 \\
                --tool-args "--recon-spec mrtrix_multishell_msmt_ACT-hsvs"
                
        Common QSIRecon options (pass via --tool-args):
            --recon-spec <spec>   : Reconstruction specification
            --nprocs <n>          : Number of processes
            --omp-nthreads <n>    : Number of OpenMP threads
            
        NOTE: QSIRecon requires QSIPrep preprocessed data. Use --output-label
        to point to the correct QSIPrep version if needed.
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate QSIRecon arguments.
        
        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if arguments are valid
        """
        return True
    
    @classmethod
    def check_requirements(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace,
        dataset_derivatives: Optional[Path] = None
    ) -> bool:
        """Check if QSIPrep output exists for this participant.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
        dataset_derivatives : Optional[Path]
            Path to derivatives directory (needed to check QSIPrep output)
            
        Returns
        -------
        bool
            True if requirements are met
        """
        if dataset_derivatives is None:
            logger.error("dataset_derivatives is required for QSIRecon requirement check")
            return False
        
        # QSIRecon requires QSIPrep v1.1.1 - this is a hard requirement
        qsiprep_dir = dataset_derivatives / f"qsiprep_{cls.required_qsiprep_version}"
        
        if not qsiprep_dir.exists():
            logger.warning(
                f"QSIPrep output not found at default location: {qsiprep_dir}\n"
                f"QSIRecon requires QSIPrep preprocessed data as input.\n"
                f"If using a different QSIPrep version, specify via --tool-args.\n"
                f"Expected QSIPrep output directory: {qsiprep_dir}"
            )
            # Return True anyway - container will perform full validation
            return True
        
        # Check if participant exists in QSIPrep output
        participant_qsiprep_dir = qsiprep_dir / f"sub-{participant_label}"
        if not participant_qsiprep_dir.exists():
            logger.error(
                f"Participant {participant_label} not found in QSIPrep output at: {qsiprep_dir}\n"
                f"Please run QSIPrep for this participant first."
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
        """Get QSIRecon output directory path.
        
        Parameters
        ----------
        dataset_derivatives : Path
            Base derivatives directory
        participant_label : str
            Participant ID (without 'sub-' prefix)
        args : argparse.Namespace
            Parsed command line arguments
        session : Optional[str]
            Session label (not used for QSIRecon)
        run : Optional[str]
            Run label (not used for QSIRecon)
            
        Returns
        -------
        Path
            Full path to output directory
        """
        version = args.version or cls.default_version
        output_label = args.output_label or f"qsirecon_{version}"
        
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
        """Build QSIRecon Apptainer command.
        
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
        
        # QSIRecon requires QSIPrep v1.1.1 - this is a hard requirement
        qsiprep_version = cls.required_qsiprep_version
        qsiprep_dir = dataset_derivatives / f"qsiprep_{qsiprep_version}"
        
        version = args.version or cls.default_version
        output_label = args.output_label or f"qsirecon_{version}"
        output_dir = dataset_derivatives / output_label
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Using QSIPrep data from: {qsiprep_dir}")
        
        # Get tool_args from user
        tool_args = getattr(args, 'tool_args', '') or ''
        
        cmd = build_apptainer_cmd(
            tool="qsirecon",
            fs_license=args.fs_license,
            qsiprep_dir=str(qsiprep_dir),
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
        """Process a subject with QSIRecon.
        
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
        
        # Check requirements (pass dataset_derivatives for QSIPrep check)
        if not cls.check_requirements(layout, participant_label, args, dataset_derivatives):
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
            logger.error(f"Error processing {participant_label} with QSIRecon: {e}")
            return False
