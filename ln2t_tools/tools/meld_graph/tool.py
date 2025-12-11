"""MELD Graph tool for lesion detection.

MELD (Multi-centre Epilepsy Lesion Detection) Graph is a deep learning
pipeline for automated detection of focal cortical dysplasia (FCD)
lesions from structural MRI data.

Features:
- GPU-accelerated inference (CPU fallback available)
- Optional ComBat harmonization for multi-site data
- Uses precomputed FreeSurfer outputs or runs FreeSurfer internally
- Special directory structure for MELD workflow
"""

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import (
    DEFAULT_MELDGRAPH_VERSION,
    DEFAULT_MELD_FS_VERSION
)

logger = logging.getLogger(__name__)


class MELDGraphTool(BaseTool):
    """MELD Graph lesion detection tool.
    
    MELD Graph has a specific workflow:
    1. Setup MELD data structure in dataset derivatives
    2. Create symlinks to input data in MELD format
    3. Optionally use precomputed FreeSurfer outputs
    4. Run prediction with optional harmonization
    
    Requires GPU for reasonable performance (CPU fallback available).
    """
    
    name = "meld_graph"
    description = "MELD Graph lesion detection for focal cortical dysplasia"
    default_version = DEFAULT_MELDGRAPH_VERSION
    default_image_name = "meld_graph_{version}.sif"
    requires_gpu = True  # GPU by default, --no-gpu for CPU fallback
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add MELD Graph-specific arguments.
        
        NOTE: Tool-specific arguments are now passed via --tool-args.
        This method is kept for compatibility but adds no arguments.
        
        MELD Graph accepts the following arguments via --tool-args:
        
        FreeSurfer options:
          --fs-version VERSION      FreeSurfer version to use as input
          --use-precomputed-fs      Use existing FreeSurfer outputs
          
        Model options:
          --download-weights        Download model weights (run once)
          --skip-feature-extraction Skip feature extraction if already done
          
        Harmonization:
          --harmonize               Compute harmonization parameters
          --harmo-code CODE         Scanner harmonization code (e.g., H1, H2)
          
        Hardware options:
          --no-gpu                  Disable GPU, use CPU (slower)
          --gpu-memory-limit MB     GPU memory split size (default: 128)
        
        Args:
            parser: Argument parser to add arguments to
        """
        pass  # Tool-specific args now passed via --tool-args
    
    @classmethod
    def validate_inputs(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace
    ) -> Tuple[bool, str]:
        """Validate MELD Graph inputs for a participant.
        
        With the --tool-args pattern, most validation is delegated to the
        container. This method only checks for T1w data existence.
        
        Args:
            layout: BIDSLayout object for the BIDS dataset
            participant_label: Subject ID without 'sub-' prefix
            args: Parsed command line arguments
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for T1w data
        t1w_files = layout.get(
            subject=participant_label,
            suffix='T1w',
            extension=['nii', 'nii.gz']
        )
        
        if not t1w_files:
            return False, f"No T1w data found for participant {participant_label}"
        
        # Other validations (FreeSurfer existence, harmonization) are now
        # delegated to the container via --tool-args
        return True, ""
    
    @classmethod
    def get_output_dir(
        cls,
        dataset_derivatives: Path,
        args: argparse.Namespace,
        participant_label: str
    ) -> Path:
        """Get the output directory for MELD Graph results.
        
        MELD has a special directory structure:
        derivatives/meld_graph_{version}/data/output/predictions_reports/sub-{label}/
        
        Args:
            dataset_derivatives: Path to the derivatives directory
            args: Parsed command line arguments
            participant_label: Subject ID without 'sub-' prefix
            
        Returns:
            Path to the output directory
        """
        version = args.version or cls.default_version
        return (
            dataset_derivatives / f"meld_graph_{version}" / "data" / "output" /
            "predictions_reports" / f"sub-{participant_label}"
        )
    
    @classmethod
    def build_command(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace,
        apptainer_img: str,
        work_dir: Path,
        dataset_rawdata: Path,
        dataset_derivatives: Path,
        dataset_code: Path
    ) -> List[str]:
        """Build the Apptainer command for MELD Graph.
        
        Note: MELD Graph uses a specialized command builder due to its
        complex directory structure and workflow.
        
        Args:
            layout: BIDSLayout object
            participant_label: Subject ID without 'sub-' prefix
            args: Parsed command line arguments
            apptainer_img: Path to the Apptainer image
            work_dir: Path to the work directory
            dataset_rawdata: Path to BIDS rawdata
            dataset_derivatives: Path to derivatives
            dataset_code: Path to code directory
            
        Returns:
            List of command components
        """
        meld_version = args.version or cls.default_version
        
        # Get tool_args from user
        tool_args = getattr(args, 'tool_args', '') or ''
        
        # Build basic Apptainer command structure
        cmd = ["apptainer", "run", "--cleanenv"]
        
        # GPU support - check if --no-gpu is in tool_args
        use_gpu = '--no-gpu' not in tool_args
        if use_gpu:
            cmd.extend(["--nv"])  # NVIDIA GPU support
        
        # Default GPU memory environment
        cmd.extend(["--env", "PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128"])
        
        # MELD data directory structure
        meld_data_dir = dataset_derivatives / f"meld_graph_{meld_version}" / "data"
        
        # Bind the MELD data directory
        cmd.extend(["-B", f"{meld_data_dir}:/data"])
        
        # Bind FreeSurfer license
        fs_license = str(args.fs_license)
        cmd.extend(["-B", f"{fs_license}:/opt/freesurfer/license.txt"])
        
        # Add the container image
        cmd.append(apptainer_img)
        
        # Add MELD command and basic arguments
        cmd.extend([
            "python", "-m", "scripts.manage_results.run_script_prediction",
            "--subject-id", f"sub-{participant_label}",
            "--data-dir", "/data"
        ])
        
        # Append tool-specific args passed via --tool-args
        if tool_args:
            cmd.extend(tool_args.split())
        
        return cmd
    
    @classmethod
    def get_meld_data_structure(
        cls,
        dataset_derivatives: Path,
        dataset_code: Path,
        version: str
    ) -> Tuple[Path, Path, Path]:
        """Get the MELD-specific directory structure paths.
        
        MELD Graph expects a specific directory layout:
        - data_dir: Main data directory with input/output subdirs
        - config_dir: Configuration files
        - output_dir: Prediction results
        
        Args:
            dataset_derivatives: Path to derivatives directory
            dataset_code: Path to code directory
            version: MELD Graph version
            
        Returns:
            Tuple of (meld_data_dir, meld_config_dir, meld_output_dir)
        """
        meld_base = dataset_derivatives / f"meld_graph_{version}"
        
        meld_data_dir = meld_base / "data"
        meld_config_dir = meld_base / "config"
        meld_output_dir = meld_data_dir / "output"
        
        return meld_data_dir, meld_config_dir, meld_output_dir
    
    @classmethod
    def requires_harmonization_setup(cls, args: argparse.Namespace) -> bool:
        """Check if harmonization setup is required.
        
        Args:
            args: Parsed command line arguments
            
        Returns:
            True if harmonization workflow is requested
        """
        tool_args = getattr(args, 'tool_args', '') or ''
        return '--harmonize' in tool_args
