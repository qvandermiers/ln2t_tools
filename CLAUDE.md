# Project Context: ln2t_tools

## Overview
ln2t_tools is a Python package for managing and processing neuroimaging data following BIDS conventions. It provides command-line tools to run FreeSurfer, FastSurfer, fMRIPrep, QSIPrep, QSIRecon, MELD Graph, and CVRmap pipelines using Apptainer containers.

## Architecture

### Tool-Args Pass-Through Pattern
ln2t_tools uses a pass-through argument pattern for tool-specific options. This design ensures:
- **Robustness**: ln2t_tools doesn't need updates when upstream tools change their CLI
- **Flexibility**: Users have access to all tool options, not just a curated subset
- **Simplicity**: Core ln2t_tools code is simpler without tool-specific argument definitions

How it works:
1. Core arguments (dataset, participant, version, HPC options) are handled by ln2t_tools
2. Tool-specific arguments are passed via `--tool-args "<arguments>"`
3. Arguments in tool_args are passed verbatim to the container
4. The container (not ln2t_tools) validates tool-specific arguments

Example:
```bash
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
    --tool-args "--fs-no-reconall --output-spaces MNI152NLin2009cAsym:res-2"
```

### Key Components

### Directory Structure
```
ln2t_tools/
├── ln2t_tools/
│   ├── __init__.py
│   ├── ln2t_tools.py        # Main processing logic
│   ├── cli/
│   │   └── cli.py           # Command line interface
│   ├── tools/               # Tool implementations
│   │   ├── base.py          # Base tool class
│   │   ├── freesurfer/
│   │   ├── fastsurfer/
│   │   ├── fmriprep/
│   │   ├── qsiprep/
│   │   ├── qsirecon/
│   │   ├── meld_graph/
│   │   └── cvrmap/
│   ├── utils/
│   │   ├── utils.py         # Utility functions including build_apptainer_cmd
│   │   ├── hpc.py           # HPC cluster support (SLURM)
│   │   └── defaults.py      # Default configurations
│   ├── completion/          # Bash completion
│   └── install/             # Installation scripts
├── setup.py
└── requirements.txt
```

### Data Organization
- Raw data: `~/rawdata/<dataset>-rawdata/`
- Derivatives: `~/derivatives/<dataset>-derivatives/`
- Apptainer images: `/opt/apptainer/`

### Main Features
- **FreeSurfer**: Cortical reconstruction with T1w, T2w, and FLAIR support
- **FastSurfer**: Fast deep-learning brain segmentation and surface reconstruction
- **fMRIPrep**: Functional MRI preprocessing with existing FreeSurfer results integration
- **QSIPrep**: Diffusion MRI preprocessing
- **QSIRecon**: Diffusion MRI reconstruction and tractography
- **MELD Graph**: Focal cortical dysplasia lesion detection
- **CVRmap**: Cerebrovascular reactivity mapping
- BIDS-compliant input/output
- Multi-session and multi-run support
- HPC cluster support (SLURM)
- Bash completion for command line usage
- Instance management to prevent resource overload

### Usage Examples
```bash
# Run FreeSurfer on single participant
ln2t_tools freesurfer --dataset <dataset> --participant-label <subject>

# Run fMRIPrep with tool-specific args
ln2t_tools fmriprep --dataset <dataset> --participant-label <subject> \
    --tool-args "--fs-no-reconall --output-spaces MNI152NLin2009cAsym:res-2"

# Run QSIPrep with output resolution
ln2t_tools qsiprep --dataset <dataset> --participant-label <subject> \
    --tool-args "--output-resolution 1.25"

# Run on HPC cluster
ln2t_tools fmriprep --dataset <dataset> --participant-label <subject> --hpc \
    --tool-args "--nprocs 8 --omp-nthreads 4"
```

## Key Functions
- `build_apptainer_cmd()`: Generic container command builder with tool_args pass-through
- `process_subject()`: Process subject with any tool (dispatches to tool-specific handlers)
- `generate_hpc_script()`: Generate SLURM batch scripts for HPC submission
- `InstanceManager`: Manage parallel instances and resource usage

## Dependencies
- Python 3.8+
- pybids
- pandas
- Apptainer (formerly Singularity)
- FreeSurfer license file (for FreeSurfer/FastSurfer/fMRIPrep)

## Notes
- Tool-specific arguments are passed via `--tool-args`
- Container validates tool-specific arguments, not ln2t_tools
- FreeSurfer outputs are reused by fMRIPrep when available
- Maximum 10 parallel instances by default (configurable with --max-instances)
- Lock files in `/tmp/ln2t_tools_locks/` track active instances
- All paths follow BIDS naming conventions