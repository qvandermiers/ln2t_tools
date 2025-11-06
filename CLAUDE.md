# Project Context: ln2t_tools

## Overview
ln2t_tools is a Python package for managing and processing neuroimaging data following BIDS conventions. It provides command-line tools to run FreeSurfer, fMRIPrep, and QSIPrep pipelines using Apptainer containers.

## Key Components

### Directory Structure
```
ln2t_tools/
├── ln2t_tools/
│   ├── __init__.py
│   ├── ln2t_tools.py        # Main processing logic
│   ├── cli/
│   │   └── cli.py           # Command line interface
│   ├── utils/
│   │   ├── utils.py         # Utility functions
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
- FreeSurfer processing with T1w, T2w, and FLAIR support
- fMRIPrep processing with existing FreeSurfer results integration
- QSIPrep processing for diffusion-weighted imaging
- BIDS-compliant input/output
- Multi-session and multi-run support
- Bash completion for command line usage
- Instance management to prevent resource overload (max 10 parallel instances)

### Usage Examples
```bash
# Run FreeSurfer on single participant
ln2t_tools freesurfer --dataset <dataset> --participant-label <subject>

# Run FreeSurfer on multiple participants
ln2t_tools freesurfer --dataset <dataset> --participant-label 01 02 42

# Run fMRIPrep
ln2t_tools fmriprep --dataset <dataset> --participant-label <subject>

# Run QSIPrep (requires output resolution)
ln2t_tools qsiprep --dataset <dataset> --participant-label <subject> --output-resolution 1.25
```

## Key Functions
- `process_freesurfer_subject()`: Process subject with FreeSurfer
- `process_fmriprep_subject()`: Process subject with fMRIPrep
- `process_qsiprep_subject()`: Process subject with QSIPrep
- `get_additional_contrasts()`: Get T2w/FLAIR images for enhanced processing
- `build_apptainer_cmd()`: Generate container commands
- `get_freesurfer_output()`: Check for existing FreeSurfer results
- `InstanceManager`: Manage parallel instances and resource usage

## Dependencies
- Python 3.8+
- pybids
- pandas
- Apptainer (formerly Singularity)
- FreeSurfer license file

## Notes
- FreeSurfer outputs are reused by fMRIPrep when available
- QSIPrep requires diffusion-weighted imaging data and output resolution specification
- Maximum 10 parallel instances by default (configurable with --max-instances)
- Lock files in `/tmp/ln2t_tools_locks/` track active instances
- All paths follow BIDS naming conventions
- Bash completion provides context-aware suggestions