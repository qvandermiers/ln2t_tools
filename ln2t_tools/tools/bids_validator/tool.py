"""
BIDS Validator tool implementation.

The BIDS Validator validates datasets to ensure they conform to the
Brain Imaging Data Structure (BIDS) specification.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_BIDS_VALIDATOR_VERSION

logger = logging.getLogger(__name__)


class BidsValidatorTool(BaseTool):
    """BIDS Validator tool for validating BIDS datasets.
    
    The BIDS Validator checks if a dataset conforms to the BIDS specification,
    reporting any errors or warnings found in the dataset structure, metadata,
    or file naming conventions.
    
    Unlike other tools in ln2t_tools, the BIDS Validator operates on the entire
    dataset rather than individual participants. The --participant-label flag
    is not required for this tool.
    """
    
    name = "bids_validator"
    help_text = "BIDS Validator - validate BIDS dataset compliance"
    description = """
BIDS Validator - Dataset Validation

The BIDS Validator checks datasets for compliance with the Brain Imaging
Data Structure (BIDS) specification. It validates:

  - Directory structure and file naming conventions
  - JSON metadata files and their required fields
  - Data file integrity and format compliance
  - Cross-references between files (e.g., events and imaging data)

Usage notes:
  - This tool validates the entire dataset, not individual subjects
  - The --participant-label option is NOT available for this tool
  - Additional options can be passed via --tool-args

Common options (pass via --tool-args):
    --ignoreWarnings     : Only report errors, not warnings
    --ignoreNiftiHeaders : Skip NIfTI header validation
    --json               : Output results in JSON format
    --verbose            : Verbose output
    --config <file>      : Path to custom configuration file

Typical runtime: A few seconds to minutes depending on dataset size
"""
    default_version = DEFAULT_BIDS_VALIDATOR_VERSION
    requires_gpu = False
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add BIDS Validator-specific CLI arguments.
        
        Tool-specific options should be passed via --tool-args.
        
        Example usage with --tool-args:
            ln2t_tools bids_validator --dataset MYDATA \\
                --tool-args "--ignoreWarnings --json"
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate BIDS Validator arguments.
        
        Parameters
        ----------
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if arguments are valid
        """
        # No specific validation needed for BIDS Validator
        return True
    
    @classmethod
    def check_requirements(
        cls,
        layout: BIDSLayout,
        participant_label: Optional[str],
        args: argparse.Namespace
    ) -> bool:
        """Check if requirements are met to run BIDS validation.
        
        For BIDS Validator, we only need the dataset to exist.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : Optional[str]
            Not used - BIDS Validator operates on entire dataset
        args : argparse.Namespace
            Parsed command line arguments
            
        Returns
        -------
        bool
            True if dataset exists
        """
        # The dataset exists if we have a valid layout
        if layout is None:
            logger.warning("No valid BIDS dataset found")
            return False
        
        return True
    
    @classmethod
    def get_output_dir(
        cls,
        dataset_derivatives: Path,
        participant_label: Optional[str],
        args: argparse.Namespace,
        session: Optional[str] = None,
        run: Optional[str] = None
    ) -> Path:
        """Get BIDS Validator output directory path.
        
        BIDS Validator doesn't produce outputs in the traditional sense,
        but we return a path for logging purposes.
        
        Parameters
        ----------
        dataset_derivatives : Path
            Base derivatives directory
        participant_label : Optional[str]
            Not used - BIDS Validator operates on entire dataset
        args : argparse.Namespace
            Parsed command line arguments
        session : Optional[str]
            Session label (not used)
        run : Optional[str]
            Run label (not used)
            
        Returns
        -------
        Path
            Path to validation logs directory
        """
        version = args.version or cls.default_version
        output_label = args.output_label or f"bids_validator_{version}"
        
        return dataset_derivatives / output_label
    
    @classmethod
    def build_command(
        cls,
        layout: BIDSLayout,
        participant_label: Optional[str],
        args: argparse.Namespace,
        dataset_rawdata: Path,
        dataset_derivatives: Path,
        apptainer_img: str,
        **kwargs
    ) -> List[str]:
        """Build BIDS Validator Apptainer command.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : Optional[str]
            Not used - BIDS Validator operates on entire dataset
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
        
        # Get tool_args from user
        tool_args = getattr(args, 'tool_args', '') or ''
        
        cmd = build_apptainer_cmd(
            tool="bids_validator",
            rawdata=str(dataset_rawdata),
            apptainer_img=apptainer_img,
            tool_args=tool_args
        )
        
        return [cmd] if isinstance(cmd, str) else cmd
    
    @classmethod
    def process_subject(
        cls,
        layout: BIDSLayout,
        participant_label: Optional[str],
        args: argparse.Namespace,
        dataset_rawdata: Path,
        dataset_derivatives: Path,
        apptainer_img: str,
        **kwargs
    ) -> bool:
        """Run BIDS validation on the dataset.
        
        Note: For BIDS Validator, this validates the entire dataset.
        It doesn't process individual subjects.
        
        Parameters
        ----------
        layout : BIDSLayout
            BIDS dataset layout
        participant_label : Optional[str]
            Not used - BIDS Validator operates on entire dataset
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
            True if validation ran successfully (not if dataset is valid)
        """
        from ln2t_tools.utils.utils import launch_apptainer
        
        # Validate arguments
        if not cls.validate_args(args):
            return False
        
        # Check requirements
        if not cls.check_requirements(layout, participant_label, args):
            return False
        
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
            logger.error("Failed to build BIDS Validator command")
            return False
        
        # Launch
        try:
            cmd_str = cmd[0] if isinstance(cmd, list) and len(cmd) == 1 else ' '.join(cmd)
            exit_code = launch_apptainer(cmd_str)
            # BIDS validator returns non-zero if there are validation errors
            # We still consider the run successful if it executed
            if exit_code == 0:
                logger.info("BIDS validation passed - no errors found")
            else:
                logger.warning(f"BIDS validation found issues (exit code: {exit_code})")
            return True
        except Exception as e:
            logger.error(f"Error running BIDS Validator: {e}")
            return False
