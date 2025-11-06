# ln2t-tools
Useful tools for the LN2T

## Overview

ln2t_tools is a neuroimaging pipeline manager that supports multiple processing tools:
- **FreeSurfer**: Cortical reconstruction and surface-based analysis
- **fMRIPrep**: Functional MRI preprocessing
- **QSIPrep**: Diffusion MRI preprocessing
- **QSIRecon**: Diffusion MRI reconstruction (requires QSIPrep)
- **MELD Graph**: Lesion detection (requires FreeSurfer)

## Table of Contents
1. [Quick Start](#quick-start)
2. [Pipeline Usage Examples](#pipeline-usage-examples)
   - [FreeSurfer](#freesurfer)
   - [fMRIPrep](#fmriprep)
   - [QSIPrep](#qsiprep)
   - [QSIRecon](#qsirecon)
   - [MELD Graph](#meld-graph)
3. [Configuration-Based Processing](#configuration-based-processing)
4. [Instance Management](#instance-management)
5. [Command-line Completion](#command-line-completion)

---

## Quick Start

```bash
# Install
pip install -e .

# List available datasets
ln2t_tools --list-datasets

# Run a basic pipeline
ln2t_tools freesurfer --dataset mydataset --participant-label 01
```

---

## Pipeline Usage Examples

### FreeSurfer

FreeSurfer performs cortical reconstruction and surface-based morphometric analysis.

#### Default Values
- **Version**: `7.3.2`
- **Output directory**: `~/derivatives/{dataset}-derivatives/freesurfer_7.3.2/`
- **Container**: `freesurfer/freesurfer:7.3.2`

#### Basic Usage

```bash
# Process single participant
ln2t_tools freesurfer --dataset mydataset --participant-label 01

# Process multiple participants
ln2t_tools freesurfer --dataset mydataset --participant-label 01 02 03

# Process all participants in dataset
ln2t_tools freesurfer --dataset mydataset
```

#### Advanced Options

```bash
# Use specific FreeSurfer version
ln2t_tools freesurfer --dataset mydataset --participant-label 01 --version 7.4.0

# Custom output label
ln2t_tools freesurfer --dataset mydataset --participant-label 01 --output-label my_freesurfer_run

# Custom FreeSurfer license location
ln2t_tools freesurfer --dataset mydataset --participant-label 01 --fs-license /path/to/license.txt

# Custom Apptainer images directory
ln2t_tools freesurfer --dataset mydataset --participant-label 01 --apptainer-dir /custom/path/apptainer
```

**Notes**:
- FreeSurfer automatically detects T2w and FLAIR images if available and uses them for pial surface refinement
- Handles multi-session and multi-run data automatically using BIDS entities

---

### fMRIPrep

fMRIPrep performs robust preprocessing of functional MRI data.

#### Default Values
- **Version**: `25.1.4`
- **Output directory**: `~/derivatives/{dataset}-derivatives/fmriprep_25.1.4/`
- **Container**: `nipreps/fmriprep:25.1.4`
- **Output spaces**: `MNI152NLin2009cAsym:res-2`
- **Number of processes**: `8`
- **OpenMP threads**: `8`

#### Basic Usage

```bash
# Process single participant
ln2t_tools fmriprep --dataset mydataset --participant-label 01

# Process multiple participants
ln2t_tools fmriprep --dataset mydataset --participant-label 01 02 03
```

#### Advanced Options

```bash
# Use specific version
ln2t_tools fmriprep --dataset mydataset --participant-label 01 --version 24.0.1

# Skip FreeSurfer reconstruction (uses existing FreeSurfer data if available)
ln2t_tools fmriprep --dataset mydataset --participant-label 01 --fs-no-reconall

# Custom output spaces
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
  --output-spaces MNI152NLin2009cAsym:res-1 fsaverage:den-10k

# Adjust computational resources
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
  --nprocs 16 --omp-nthreads 16

# Combine multiple options
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
  --fs-no-reconall \
  --output-spaces MNI152NLin2009cAsym:res-2 MNI152NLin6Asym:res-2 \
  --nprocs 12 --omp-nthreads 12
```

**Notes**:
- Automatically uses existing FreeSurfer outputs if found (from `freesurfer_{version}` directory)
- Set `--fs-no-reconall` to skip FreeSurfer even if outputs don't exist

---

### QSIPrep

QSIPrep performs preprocessing of diffusion MRI data.

#### Default Values
- **Version**: `1.0.1`
- **Output directory**: `~/derivatives/{dataset}-derivatives/qsiprep_1.0.1/`
- **Container**: `pennlinc/qsiprep:1.0.1`
- **Denoise method**: `dwidenoise`
- **Number of processes**: `8`
- **OpenMP threads**: `8`

#### Basic Usage

```bash
# Process single participant (requires --output-resolution)
ln2t_tools qsiprep --dataset mydataset --participant-label 01 --output-resolution 1.25

# Process multiple participants
ln2t_tools qsiprep --dataset mydataset --participant-label 01 02 03 --output-resolution 1.5
```

#### Advanced Options

```bash
# Use specific version
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --version 0.24.0 --output-resolution 1.25

# Different denoising methods
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --output-resolution 1.25 --denoise-method patch2self

ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --output-resolution 1.25 --denoise-method none

# DWI-only processing (skip anatomical)
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --output-resolution 1.25 --dwi-only

# Anatomical-only processing
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --output-resolution 1.25 --anat-only

# Adjust computational resources
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --output-resolution 1.25 --nprocs 16 --omp-nthreads 16

# Full example with multiple options
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --version 1.0.1 \
  --output-resolution 1.5 \
  --denoise-method dwidenoise \
  --nprocs 12 --omp-nthreads 12
```

**Required Options**:
- `--output-resolution`: Isotropic voxel size in mm (e.g., 1.25, 1.5, 2.0)

**Notes**:
- QSIPrep automatically skips BIDS validation
- Requires DWI data in BIDS format

---

### QSIRecon

QSIRecon performs reconstruction and tractography on QSIPrep preprocessed data.

#### Default Values
- **Version**: `1.1.1`
- **Output directory**: `~/derivatives/{dataset}-derivatives/qsirecon_1.1.1/`
- **Container**: `pennlinc/qsirecon:1.1.1`
- **QSIPrep version**: `1.0.1` (default input)
- **Reconstruction spec**: `mrtrix_multishell_msmt_ACT-hsvs`
- **Number of processes**: `8`
- **OpenMP threads**: `8`

#### Basic Usage

```bash
# Process single participant (uses default QSIPrep version 1.0.1)
ln2t_tools qsirecon --dataset mydataset --participant-label 01

# Process multiple participants
ln2t_tools qsirecon --dataset mydataset --participant-label 01 02 03
```

#### Advanced Options

```bash
# Use specific QSIPrep version as input
ln2t_tools qsirecon --dataset mydataset --participant-label 01 --qsiprep-version 0.24.0

# Use specific QSIRecon version
ln2t_tools qsirecon --dataset mydataset --participant-label 01 --version 1.0.0

# Different reconstruction pipelines
# Single-shell reconstruction
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --recon-spec mrtrix_singleshell_ss3t_ACT-hsvs

# Multi-shell without ACT
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --recon-spec mrtrix_multishell_msmt

# DSI Studio pipeline
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --recon-spec dsi_studio_gqi

# Adjust computational resources
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --nprocs 16 --omp-nthreads 16

# Full example combining options
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --qsiprep-version 1.0.1 \
  --version 1.1.1 \
  --recon-spec mrtrix_multishell_msmt_ACT-hsvs \
  --nprocs 12 --omp-nthreads 12
```

**Prerequisites**:
- QSIPrep must be run first
- QSIRecon looks for preprocessed data in `qsiprep_{version}` directory

**Error Messages**:
If QSIPrep output is not found, you'll see:
```
QSIPrep output not found at: ~/derivatives/dataset-derivatives/qsiprep_1.0.1
QSIRecon requires QSIPrep preprocessed data as input.
Please run QSIPrep first, or specify the correct QSIPrep version with --qsiprep-version.
```

---

### MELD Graph

MELD Graph performs automated lesion detection using FreeSurfer surfaces.

#### Default Values
- **Version**: `v2.2.3`
- **Output directory**: `~/derivatives/{dataset}-derivatives/meld_graph_v2.2.3/`
- **Container**: `meldproject/meld_graph:v2.2.3`
- **FreeSurfer version**: `7.3.2` (default input)

#### Basic Usage

```bash
# Process single participant (uses default FreeSurfer version 7.3.2)
ln2t_tools meld_graph --dataset mydataset --participant-label 01

# Process multiple participants
ln2t_tools meld_graph --dataset mydataset --participant-label 01 02 03
```

#### Advanced Options

```bash
# Use specific FreeSurfer version as input
ln2t_tools meld_graph --dataset mydataset --participant-label 01 --fs-version 7.4.0

# Use specific MELD Graph version
ln2t_tools meld_graph --dataset mydataset --participant-label 01 --version v2.1.0

# Full example
ln2t_tools meld_graph --dataset mydataset --participant-label 01 \
  --fs-version 7.3.2 \
  --version v2.2.3
```

**Prerequisites**:
- FreeSurfer reconstruction must be completed first
- MELD Graph looks for FreeSurfer data in `freesurfer_{version}` directory

**Error Messages**:
If FreeSurfer output is not found, you'll see:
```
No FreeSurfer output found for participant 01.
MELD Graph requires FreeSurfer recon-all to be completed first.
```

---

## Configuration-Based Processing

ln2t_tools supports configuration-based processing using a TSV file. Create a file named `processing_config.tsv` in your rawdata directory (`~/rawdata/processing_config.tsv`).

### Configuration File Format

```tsv
dataset	freesurfer	fmriprep	qsiprep	qsirecon	meld_graph
dataset1	7.3.2	25.1.4	1.0.1	1.1.1	v2.2.3
dataset2		25.1.4			
dataset3	7.4.0		1.0.1	1.1.1	
dataset4	7.3.2				v2.2.3
```

**Rules**:
- First column must be named `dataset`
- Each subsequent column represents a tool
- Cell values are tool versions
- Empty cells = tool won't run for that dataset
- Dependencies are automatically respected (e.g., qsirecon requires qsiprep)

### Configuration Usage Examples

```bash
# Process all datasets according to config
ln2t_tools

# Process specific dataset from config
ln2t_tools --dataset dataset1

# Process specific participant from dataset (uses config for tools/versions)
ln2t_tools --dataset dataset1 --participant-label 01

# Override config: run specific tool regardless of config
ln2t_tools freesurfer --dataset dataset2 --participant-label 01 --version 7.3.2
```

### Pipeline Dependencies in Config

When using configuration files, tools are run in dependency order:

1. **FreeSurfer** → Used by fMRIPrep (optional) and MELD Graph (required)
2. **QSIPrep** → Required by QSIRecon

Example workflow:
```tsv
dataset	freesurfer	qsiprep	qsirecon	meld_graph
mydata	7.3.2	1.0.1	1.1.1	v2.2.3
```

This will automatically:
1. Run FreeSurfer 7.3.2
2. Run QSIPrep 1.0.1
3. Run QSIRecon 1.1.1 (using QSIPrep output)
4. Run MELD Graph v2.2.3 (using FreeSurfer output)

---

## Instance Management

ln2t_tools includes built-in safeguards to prevent resource overload:

- **Default limit**: Maximum 10 parallel instances
- **Lock files**: Stored in `/tmp/ln2t_tools_locks/` with detailed JSON metadata
- **Automatic cleanup**: Removes stale lock files from terminated processes
- **Graceful handling**: Shows helpful messages when limits are reached

### Lock File Information

Each instance creates a lock file with:
- Process ID (PID)
- Dataset name(s)
- Tool(s) being run
- Participant labels
- Hostname
- Username
- Start time

### Instance Management Commands

```bash
# Check currently running instances
ln2t_tools --list-instances

# Set custom maximum instances limit
ln2t_tools --max-instances 5 --dataset mydataset

# Example output of --list-instances:
# Found 2 active instances:
#   1. PID: 1217649, User: arovai@dataserver.local
#      Dataset: mydataset, Tool: qsiprep
#      Participants: sub-01, sub-02
#      Running for: 3245.2s, Lock: ln2t_tools_1217649.lock
```

---

## Command-line Completion

To enable command-line completion with tab:

### Installation

```bash
# Install the package
pip install -e .

# The completion script is automatically installed to:
# ~/.local/share/bash-completion/completions/ln2t_tools
```

### Activation

```bash
# Temporary activation (current session only)
source ~/.local/share/bash-completion/completions/ln2t_tools

# Permanent activation (add to ~/.bashrc)
echo "source ~/.local/share/bash-completion/completions/ln2t_tools" >> ~/.bashrc
source ~/.bashrc
```

### Completion Features

The completion script provides intelligent suggestions for:

- **Tools**: `freesurfer`, `fmriprep`, `qsiprep`, `qsirecon`, `meld_graph`
- **Datasets**: Auto-detects from `~/rawdata/*-rawdata` directories
- **Participants**: Auto-detects from selected dataset's BIDS structure
- **Tool-specific options**: Shows relevant flags based on selected tool

#### Examples

```bash
# Tab after command shows all tools
ln2t_tools <TAB>
# → freesurfer fmriprep qsiprep qsirecon meld_graph

# Tab after tool shows --dataset option
ln2t_tools freesurfer <TAB>
# → --dataset

# Tab after --dataset shows available datasets
ln2t_tools freesurfer --dataset <TAB>
# → dataset1 dataset2 dataset3

# Tab after --participant-label shows participants in that dataset
ln2t_tools freesurfer --dataset mydataset --participant-label <TAB>
# → 01 02 03 04 05

# Tab shows tool-specific options
ln2t_tools qsiprep --<TAB>
# → --output-resolution --denoise-method --dwi-only --anat-only --nprocs --omp-nthreads

ln2t_tools qsirecon --<TAB>
# → --qsiprep-version --recon-spec --nprocs --omp-nthreads
```

---

## Common Workflows

### Full Pipeline for Single Participant

```bash
# 1. Run FreeSurfer
ln2t_tools freesurfer --dataset mydataset --participant-label 01

# 2. Run fMRIPrep (uses FreeSurfer output)
ln2t_tools fmriprep --dataset mydataset --participant-label 01

# 3. Run QSIPrep  
ln2t_tools qsiprep --dataset mydataset --participant-label 01 --output-resolution 1.25

# 4. Run QSIRecon (uses QSIPrep output)
ln2t_tools qsirecon --dataset mydataset --participant-label 01

# 5. Run MELD Graph (uses FreeSurfer output)
ln2t_tools meld_graph --dataset mydataset --participant-label 01
```

### Using Configuration File (Recommended)

```bash
# Create config file
cat > ~/rawdata/processing_config.tsv << EOF
dataset	freesurfer	fmriprep	qsiprep	qsirecon	meld_graph
mydataset	7.3.2	25.1.4	1.0.1	1.1.1	v2.2.3
EOF

# Process all participants for dataset
ln2t_tools --dataset mydataset

# Process specific participants
ln2t_tools --dataset mydataset --participant-label 01 02 03
```

### Monitoring and Utilities

```bash
# List available datasets
ln2t_tools --list-datasets

# List missing subjects for a tool
ln2t_tools freesurfer --dataset mydataset --list-missing

# Check running instances
ln2t_tools --list-instances

# Limit parallel executions
ln2t_tools --max-instances 3 --dataset mydataset
```

---

## Directory Structure

ln2t_tools expects and creates the following structure:

```
~/rawdata/
  ├── processing_config.tsv          # Configuration file
  ├── dataset1-rawdata/              # BIDS raw data
  │   ├── sub-01/
  │   ├── sub-02/
  │   └── ...
  └── dataset2-rawdata/
      └── ...

~/derivatives/
  ├── dataset1-derivatives/
  │   ├── freesurfer_7.3.2/         # FreeSurfer outputs
  │   ├── fmriprep_25.1.4/          # fMRIPrep outputs
  │   ├── qsiprep_1.0.1/            # QSIPrep outputs
  │   ├── qsirecon_1.1.1/           # QSIRecon outputs
  │   └── meld_graph_v2.2.3/        # MELD Graph outputs
  └── dataset2-derivatives/
      └── ...

/opt/apptainer/                      # Apptainer/Singularity images
  ├── freesurfer.freesurfer.7.3.2.sif
  ├── nipreps.fmriprep.25.1.4.sif
  ├── pennlinc.qsiprep.1.0.1.sif
  ├── pennlinc.qsirecon.1.1.1.sif
  └── meldproject.meld_graph.v2.2.3.sif
```

---

## Troubleshooting

### Images Not Found
If Apptainer images are missing, they will be built automatically:
```bash
ln2t_tools will attempt to build: freesurfer/freesurfer:7.3.2
```

### FreeSurfer License
Ensure your FreeSurfer license is at `/opt/freesurfer/.license` or specify:
```bash
ln2t_tools freesurfer --dataset mydataset --participant-label 01 --fs-license /path/to/license.txt
```

### Instance Limit Reached
```bash
Cannot start new instance. Maximum instances (10) reached.
Currently running: 10 instances.
Please wait for other instances to complete or increase --max-instances.
```

Solution: Wait for jobs to finish or increase limit:
```bash
ln2t_tools --max-instances 15 --dataset mydataset
```

### Missing Prerequisites
For QSIRecon or MELD Graph, ensure prerequisite tools have been run first:
```bash
# QSIRecon needs QSIPrep
ln2t_tools qsiprep --dataset mydataset --participant-label 01 --output-resolution 1.25
ln2t_tools qsirecon --dataset mydataset --participant-label 01

# MELD Graph needs FreeSurfer
ln2t_tools freesurfer --dataset mydataset --participant-label 01
ln2t_tools meld_graph --dataset mydataset --participant-label 01
```


