<div align="center">

# ln2t_tools

**Neuroimaging pipeline manager for the [LN2T](https://ln2t.ulb.be/)**

[About](#about) | [Installation](#installation) | [Quick Start](#quick-start) | [Data Organization](#data-and-code-organization) | [CLI Options](#cli-options) | [Examples](#examples) | [Advanced Documentation](#advanced-documentation)

</div>

## About

`ln2t_tools` is a command-line interface (CLI) tool that simplifies the execution of standard neuroimaging pipelines on BIDS-organized datasets. It provides:

- **Unified interface**: Run multiple neuroimaging pipelines (FreeSurfer, fMRIPrep, QSIPrep, etc.) with consistent syntax
- **Container-based reproducibility**: All pipelines run in Apptainer containers, ensuring reproducibility and eliminating dependency conflicts
- **Automated data discovery**: Automatically detects BIDS datasets and participants for streamlined processing
- **HPC support**: Seamlessly submit jobs to high-performance computing clusters (CECI, etc.)
- **Modular architecture**: Easily extensible with new tools and pipelines

For a comprehensive guide on data organization, visit the [LN2T data repository](https://github.com/ln2t/ln2t_data).

## Installation

### Prerequisites

- **Python 3.8+**
- **Apptainer**: Must be installed system-wide
- **Git** (for cloning the repository)
- **SSH key** (for HPC access, optional)

### Local Installation

1. Clone the repository:
```bash
git clone git@github.com:ln2t/ln2t_tools.git
cd ln2t_tools
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

3. Install the package:
```bash
pip install -U pip && pip install -U .
```

4. (Optional) Enable bash completion:
```bash
echo "source ~/.local/share/bash-completion/completions/ln2t_tools" >> ~/.bashrc
source ~/.bashrc
```

### Apptainer Configuration

`ln2t_tools` uses Apptainer to run pipelines in containers. Container images are stored in `/opt/apptainer` by default.

**Setup permissions:**
```bash
sudo mkdir -p /opt/apptainer
sudo chown -R $USER:$USER /opt/apptainer
sudo chmod -R 755 /opt/apptainer
```

**Custom directory** (optional):
```bash
ln2t_tools freesurfer --dataset mydataset --apptainer-dir /path/to/custom/dir
```

### FreeSurfer License

Some pipelines require a FreeSurfer license (free academic license available at [FreeSurfer Registration](https://surfer.nmr.mgh.harvard.edu/registration.html)).

**Default location:** `~/licenses/license.txt`

**Custom location:**
```bash
ln2t_tools freesurfer --dataset mydataset --fs-license /path/to/license.txt
```
### HPC Setup (Optional)

To use `ln2t_tools` for submitting jobs to high-performance computing clusters, SSH key-based authentication must be configured. This is a prerequisite for HPC functionality.

**SSH Key Requirements:**
- `ln2t_tools` uses SSH keys to authenticate with HPC clusters
- Default key location: `~/.ssh/id_rsa`
- Custom key location can be specified with `--hpc-keyfile <path>`

**Setup:**
```bash
# Generate SSH key (if not already present)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa

# Ensure correct permissions
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub
```

Configure your cluster's SSH settings according to your HPC provider's documentation. Most HPC centers provide detailed setup instructions for SSH access. Once configured, you can use HPC features with commands like:
```bash
ln2t_tools <pipeline> --dataset <dataset_name> --hpc \
           --hpc-username <user> --hpc-hostname <host>
```
## Quick Start

The basic syntax for running any pipeline is:
```bash
ln2t_tools <pipeline_name> --dataset <dataset_name> [options]
```

### Example: FreeSurfer on a Full Dataset

```bash
ln2t_tools freesurfer --dataset <dataset_name>
```

This command will:
1. Detect the latest FreeSurfer version
2. Download/install the container if needed
3. Discover all subjects in the dataset
4. Check for already-processed subjects
5. Process remaining subjects sequentially

### Example: FreeSurfer on Specific Participants

```bash
ln2t_tools freesurfer --dataset <dataset_name> \
            --participant-label 001 042
```

**Tip:** Start with a few subjects to verify the setup before processing large cohorts.

### Available Pipelines

- **FreeSurfer** - `freesurfer`: Cortical reconstruction and surface-based analysis
- **fMRIPrep** - `fmriprep`: Functional MRI preprocessing
- **QSIPrep** - `qsiprep`: Diffusion MRI preprocessing
- **QSIRecon** - `qsirecon`: Diffusion MRI reconstruction
- **MELD Graph** - `meld_graph`: Lesion detection

## Data and Code Organization

### BIDS Structure

`ln2t_tools` requires data organized according to the [Brain Imaging Data Structure (BIDS) specification](https://bids-specification.readthedocs.io/). Data are organized into three main categories:

#### 1. Raw Data (BIDS-formatted)
Your primary dataset in BIDS format. This contains all the imaging and physiological data acquired for your study.

**Location:** `~/rawdata/<dataset_name>`

**Format:** Strictly follows BIDS specification with:
- One folder per participant: `sub-001`, `sub-002`, etc.
- Data type folders within each participant: `anat/`, `func/`, `dwi/`, `meg/`, `mrs/`, etc.
- Required metadata files at dataset root: `dataset_description.json`, `participants.tsv`

#### 2. Derivatives
Output data from processing pipelines. Each pipeline creates its own versioned folder.

**Location:** `~/derivatives/<dataset_name>`

**Structure:**
```
derivatives/
├── freesurfer_7.3.2/
│   ├── sub-001/
│   ├── sub-002/
│   └── ...
├── fmriprep_20.2.0/
│   ├── sub-001/
│   ├── sub-002/
│   └── ...
└── qsiprep_0.14.0/
    ├── sub-001/
    ├── sub-002/
    └── ...
```

**Key principles:**
- One folder per pipeline with version number
- One subfolder per processed participant
- Version numbers enable multiple pipeline versions on the same dataset

#### 3. Code and Configuration
Custom scripts and configuration files for your analysis.

**Location:** `~/code/<dataset_name>`

**Recommended structure:**
```
code/
├── README.md                          # Document your analysis pipeline
├── config/                            # Configuration files
│   └── qsirecon_config.yaml
└── analysis/                          # Custom analysis scripts
    ├── preprocessing.py
    └── statistical_analysis.R
```

### Typical Folder Layout

A minimal example of a properly structured dataset:

```
~/
├── rawdata/
│   └── <dataset_name>/
│       ├── dataset_description.json
│       ├── participants.tsv
│       └── sub-001/
│           ├── anat/
│           │   ├── sub-001_T1w.nii.gz
│           │   └── sub-001_T1w.json
│           ├── func/
│           │   ├── sub-001_task-rest_bold.nii.gz
│           │   └── sub-001_task-rest_bold.json
│           └── dwi/
│               ├── sub-001_dwi.nii.gz
│               ├── sub-001_dwi.bval
│               └── sub-001_dwi.bvec
│
├── derivatives/
│   └── <dataset_name>/
│       ├── freesurfer_7.3.2/
│       │   └── sub-001/
│       └── fmriprep_20.2.0/
│           └── sub-001/
│
└── code/
    └── <dataset_name>/
        └── README.md
```

### Data Validation

Before running pipelines, ensure:

1. **Participant folders**: Check that `sub-XXX` folders exist for all expected participants
2. **Data types**: Verify expected imaging modalities are present (`func/`, `dwi/`, `anat/`, etc.)
3. **BIDS compliance**: Validate with [BIDS Validator](https://bids-standard.github.io/bids-validator/) available as a `ln2t_tools` command
4. **Metadata files**: Confirm `dataset_description.json` and `participants.tsv` exist in the raw data root

## CLI Options

### Core Options

```bash
ln2t_tools <tool> --dataset <dataset>                # BIDS dataset name
                 --participant-label <labels>       # Process specific participants
                 --output-label <label>              # Custom output directory label
                 --version <version>                 # Specific tool version
                 --max-instances <n>                 # Max parallel processes (default: 4)
                 --tool-args "<args>"                # Pass arguments to the tool
                 --list-missing                      # Show missing participants
                 --verbosity <level>                 # Log level: silent, minimal, verbose, debug
```

### Global Options

```bash
ln2t_tools --list-datasets                           # List available BIDS datasets
           --list-instances                          # Show running processes
           --hpc-status [JOB_ID]                     # Check HPC job status
```

### HPC Options

```bash
ln2t_tools <tool> --dataset <dataset> --hpc \
           --hpc-username <user> \                   # HPC username
           --hpc-hostname <host> \                   # Cluster hostname (e.g., lyra)
           --hpc-time <HH:MM:SS> \                   # Job time limit (default: 24:00:00)
           --hpc-mem <memory> \                      # Memory allocation (default: 32G)
           --hpc-cpus <n> \                          # Number of CPUs (default: 8)
           --hpc-partition <partition>               # HPC partition name
```

### Advanced Options

```bash
--fs-license <path>                                  # FreeSurfer license file path
--apptainer-dir <path>                              # Apptainer images directory
--fs-version <version>                              # FreeSurfer version for input data
--hpc-keyfile <path>                                # SSH private key (default: ~/.ssh/id_rsa)
--hpc-gateway <gateway>                             # ProxyJump gateway hostname
```

Use `--help` after any pipeline name for tool-specific options:
```bash
ln2t_tools <pipeline> --help
```

## Examples

### Basic Usage

**Run fMRIPrep on a dataset:**
```bash
ln2t_tools fmriprep --dataset mydata --participant-label 001 002 003
```

**Run QSIPrep with custom output label:**
```bash
ln2t_tools qsiprep --dataset mydata --output-label "my_custom_run"
```

### Using Tool-Specific Arguments

Pass arguments directly to tools using `--tool-args`:
```bash
ln2t_tools qsiprep --dataset mydata --participant-label 001 \
    --tool-args "--output-resolution 1.5"
```

### HPC Submission

Submit jobs to a cluster:
```bash
ln2t_tools freesurfer --dataset mydata --hpc \
           --hpc-username myuser \
           --hpc-hostname lyra \
           --hpc-time 10:00:00
```

### Finding Missing Participants

List participants that still need processing:
```bash
ln2t_tools fmriprep --dataset mydata --list-missing
```

This helps when:
- Resuming after errors
- Managing large cohorts
- Generating batch commands

For more examples and detailed workflows, see [Examples](#examples).

## Advanced Documentation

For advanced topics and detailed configuration, see the following guides:

- [Data Import Guide](docs/data_import.md) - Convert raw neuroimaging data to BIDS format (DICOM, MRS, Physio, MEG)
- [Adding a New Tool](docs/adding_new_tool.md) - Develop and integrate new neuroimaging pipelines with custom Apptainer recipes
- [Instance Management](docs/instance_management.md) - Control parallel processing limits and resource usage
- [MELD Graph Setup](docs/meld_graph.md) - Advanced configuration for focal cortical dysplasia detection

For general information, see [docs/index.md](docs/index.md).
