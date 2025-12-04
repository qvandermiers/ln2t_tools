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
        
        Args:
            parser: Argument parser to add arguments to
        """
        parser.add_argument(
            "--fs-version",
            default=DEFAULT_MELD_FS_VERSION,
            help=f"FreeSurfer version to use as input (default: {DEFAULT_MELD_FS_VERSION})"
        )
        parser.add_argument(
            "--download-weights",
            action="store_true",
            help="Download MELD Graph model weights (run once before first use)"
        )
        parser.add_argument(
            "--harmonize",
            action="store_true",
            help="Compute harmonization parameters for the provided cohort (requires --harmo-code)"
        )
        parser.add_argument(
            "--harmo-code",
            help="Harmonization code for scanner (e.g., H1, H2)"
        )
        parser.add_argument(
            "--use-precomputed-fs",
            action="store_true",
            help="Use precomputed FreeSurfer outputs instead of running FreeSurfer"
        )
        parser.add_argument(
            "--skip-feature-extraction",
            action="store_true",
            help="Skip MELD feature extraction (only use if .sm3.mgh files already exist from a previous run)"
        )
        parser.add_argument(
            "--no-gpu",
            action="store_true",
            help="Disable GPU and use CPU for inference (slower but uses less memory)"
        )
        parser.add_argument(
            "--gpu-memory-limit",
            type=int,
            default=128,
            help="GPU memory split size in MB for PyTorch (default: 128)"
        )
    
    @classmethod
    def validate_inputs(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace
    ) -> Tuple[bool, str]:
        """Validate MELD Graph inputs for a participant.
        
        Requirements:
        - T1w image exists
        - If using precomputed FreeSurfer, FreeSurfer outputs must exist
        - If harmonizing, demographics file must be valid
        
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
        
        # If using precomputed FreeSurfer, check it exists
        use_precomputed = getattr(args, 'use_precomputed_fs', False)
        if use_precomputed:
            dataset_derivatives = Path(args.dataset) / "derivatives"
            fs_version = getattr(args, 'fs_version', DEFAULT_MELD_FS_VERSION)
            fs_dir = dataset_derivatives / f"freesurfer_{fs_version}" / f"sub-{participant_label}"
            
            if not fs_dir.exists():
                return False, (
                    f"FreeSurfer output not found for {participant_label} at {fs_dir}. "
                    f"Cannot use --use-precomputed-fs without existing FreeSurfer outputs."
                )
            
            # Check for critical FreeSurfer outputs
            required_files = [
                fs_dir / "surf" / "lh.white",
                fs_dir / "surf" / "rh.white",
                fs_dir / "surf" / "lh.pial",
                fs_dir / "surf" / "rh.pial",
            ]
            
            missing = [str(f) for f in required_files if not f.exists()]
            if missing:
                return False, (
                    f"FreeSurfer outputs incomplete for {participant_label}. "
                    f"Missing: {missing}"
                )
        
        # If harmonize, check harmo-code is provided
        harmonize = getattr(args, 'harmonize', False)
        if harmonize and not getattr(args, 'harmo_code', None):
            return False, "--harmo-code is required when using --harmonize"
        
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
        complex directory structure and workflow. This method prepares
        the environment but the actual command construction happens in
        the main module's build_apptainer_cmd function.
        
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
        # MELD Graph has a very specific directory structure and workflow
        # that is handled in the main processing function
        # This method returns the key parameters for command building
        
        meld_version = args.version or cls.default_version
        
        # Build basic Apptainer command structure
        cmd = ["apptainer", "run", "--cleanenv"]
        
        # GPU support
        use_gpu = not getattr(args, 'no_gpu', False)
        if use_gpu:
            cmd.extend(["--nv"])  # NVIDIA GPU support
        
        # Environment variable for GPU memory
        gpu_memory = getattr(args, 'gpu_memory_limit', 128)
        cmd.extend(["--env", f"PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:{gpu_memory}"])
        
        # MELD data directory structure
        meld_data_dir = dataset_derivatives / f"meld_graph_{meld_version}" / "data"
        
        # Bind the MELD data directory
        cmd.extend(["-B", f"{meld_data_dir}:/data"])
        
        # Bind FreeSurfer license
        fs_license = str(args.fs_license)
        cmd.extend(["-B", f"{fs_license}:/opt/freesurfer/license.txt"])
        
        # Handle precomputed FreeSurfer outputs
        use_precomputed = getattr(args, 'use_precomputed_fs', False)
        if use_precomputed:
            fs_version = getattr(args, 'fs_version', DEFAULT_MELD_FS_VERSION)
            fs_subjects_dir = dataset_derivatives / f"freesurfer_{fs_version}"
            if fs_subjects_dir.exists():
                cmd.extend(["-B", f"{fs_subjects_dir}:/data/output/fs_outputs"])
        
        # Add the container image
        cmd.append(apptainer_img)
        
        # Add MELD command and arguments
        cmd.extend([
            "python", "-m", "scripts.manage_results.run_script_prediction",
            "--subject-id", f"sub-{participant_label}",
            "--data-dir", "/data"
        ])
        
        # Harmonization options
        harmo_code = getattr(args, 'harmo_code', None)
        if harmo_code:
            cmd.extend(["--harmo-code", harmo_code])
        
        skip_feature_extraction = getattr(args, 'skip_feature_extraction', False)
        if skip_feature_extraction:
            cmd.append("--skip-feature-extraction")
        
        harmonize = getattr(args, 'harmonize', False)
        if harmonize:
            cmd.append("--harmo-only")
        
        # Additional options
        additional_opts = getattr(args, 'additional_options', '')
        if additional_opts:
            cmd.extend(additional_opts.split())
        
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
        return getattr(args, 'harmonize', False)
