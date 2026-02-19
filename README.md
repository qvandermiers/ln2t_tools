# ln2t-tools
Useful tools for the LN2T

## Overview

ln2t_tools is a neuroimaging pipeline manager that supports multiple processing tools:
- **FreeSurfer**: Cortical reconstruction and surface-based analysis ([official docs](https://surfer.nmr.mgh.harvard.edu/))
- **fMRIPrep**: Functional MRI preprocessing ([official docs](https://fmriprep.org/))
- **QSIPrep**: Diffusion MRI preprocessing ([official docs](https://qsiprep.readthedocs.io/))
- **QSIRecon**: Diffusion MRI reconstruction ([official docs](https://qsiprep.readthedocs.io/))
- **MELD Graph**: Lesion detection ([official docs](https://meld-graph.readthedocs.io/))

## Table of Contents
1. [Data Organization](#data-organization)
2. [Setup Requirements](#setup-requirements)
3. [Quick Start](#quick-start)
4. [Pipeline Usage Examples](#pipeline-usage-examples)
   - [FreeSurfer](#freesurfer)
   - [fMRIPrep](#fmriprep)
   - [QSIPrep](#qsiprep)
   - [QSIRecon](#qsirecon)
   - [MELD Graph](#meld-graph)
5. [Finding Available Options](#finding-available-options)
6. [Instance Management](#instance-management)
7. [HPC Job Submission](#hpc-job-submission)
8. [Command-line Completion](#command-line-completion)
9. [Adding a New Tool](#adding-a-new-tool)

---

## Data Organization

ln2t_tools follows the [BIDS (Brain Imaging Data Structure) specification](https://bids-specification.readthedocs.io/) for organizing neuroimaging data.

### Dataset Naming Convention

Datasets follow a consistent naming pattern:
- **Source data**: `{dataset}-sourcedata` (e.g., `myproject-sourcedata`)
- **Raw BIDS data**: `{dataset}-rawdata` (e.g., `myproject-rawdata`)
- **Derivatives**: `{dataset}-derivatives` (e.g., `myproject-derivatives`)
- **Code**: `{dataset}-code` (e.g., `myproject-code`)

### Directory Structure

All datasets are organized under your home directory:

```
~/
├── sourcedata/
│   └── {dataset}-sourcedata/
│       ├── dicom/                          # DICOM files from scanner
│       ├── physio/                         # Physiological recordings (GE scanner)
│       ├── mrs/                            # Magnetic Resonance Spectroscopy data
│       ├── meg/                            # MEG data from Neuromag/Elekta/MEGIN
│       │   ├── meg_XXXX/                   # MEG subject folders (4-digit ID)
│       │   │   └── YYMMDD/                 # Session date folders
│       │   │       └── *.fif               # MEG FIF files
│       │   └── ...
│       └── configs/                        # Configuration files
│           ├── dcm2bids.json              # DICOM to BIDS conversion config
│           ├── spec2bids.json             # MRS to BIDS conversion config
│           ├── physio.json                # Physiological data processing config
│           └── meg2bids.json              # MEG to BIDS conversion config
│
├── rawdata/
│   └── {dataset}-rawdata/                  # BIDS-formatted data
│       ├── dataset_description.json
│       ├── participants.tsv
│       ├── sub-{id}/
│       │   ├── anat/
│       │   │   ├── sub-{id}_T1w.nii.gz
│       │   │   ├── sub-{id}_T2w.nii.gz
│       │   │   └── sub-{id}_FLAIR.nii.gz
│       │   ├── func/
│       │   │   ├── sub-{id}_task-{name}_bold.nii.gz
│       │   │   └── sub-{id}_task-{name}_bold.json
│       │   ├── dwi/
│       │   │   ├── sub-{id}_dwi.nii.gz
│       │   │   ├── sub-{id}_dwi.bval
│       │   │   └── sub-{id}_dwi.bvec
│       │   ├── mrs/
│       │   │   ├── sub-{id}_svs.nii.gz
│       │   │   └── sub-{id}_svs.json
│       │   ├── meg/
│       │   │   ├── sub-{id}_task-{name}_meg.fif
│       │   │   ├── sub-{id}_task-{name}_meg.json
│       │   │   ├── sub-{id}_task-{name}_channels.tsv
│       │   │   ├── sub-{id}_acq-crosstalk_meg.fif
│       │   │   └── sub-{id}_acq-calibration_meg.dat
│       │   └── func/ (physiological recordings)
│       │       ├── sub-{id}_task-{name}_recording-cardiac_physio.tsv.gz
│       │       ├── sub-{id}_task-{name}_recording-cardiac_physio.json
│       │       ├── sub-{id}_task-{name}_recording-respiratory_physio.tsv.gz
│       │       └── sub-{id}_task-{name}_recording-respiratory_physio.json
│
├── derivatives/
│   └── {dataset}-derivatives/
│       ├── freesurfer_{version}/          # FreeSurfer outputs
│       ├── fmriprep_{version}/            # fMRIPrep outputs
│       ├── qsiprep_{version}/             # QSIPrep outputs
│       ├── qsirecon_{version}/            # QSIRecon outputs
│       ├── maxfilter_{version}/           # MaxFilter MEG derivatives
│       └── meld_graph_{version}/          # MELD Graph outputs
│
└── code/
    └── {dataset}-code/
        └── meld_graph_{version}/
            └── config/                     # MELD configuration files
                ├── meld_bids_config.json
                └── dataset_description.json
```

**Key Points**:
- **sourcedata**: Original, unmodified data from the scanner
- **rawdata**: BIDS-formatted data, ready for processing
- **derivatives**: Processed outputs from analysis pipelines
- **code**: Analysis code and pipeline-specific configurations

For more details on BIDS structure, see the [BIDS Specification](https://bids-specification.readthedocs.io/).

---

## Setup Requirements

### Apptainer (formerly Singularity)

ln2t_tools uses [Apptainer](https://apptainer.org/) containers to run neuroimaging pipelines. This ensures reproducibility and eliminates dependency conflicts.

**Installation**:
- Apptainer must be installed system-wide
- Requires sudo/root access for installation
- Container images are stored in `/opt/apptainer/` by default

**Permissions**:
The `/opt/apptainer/` directory requires write access for pulling and caching container images:
```bash
# Create directory with proper permissions (requires sudo)
sudo mkdir -p /opt/apptainer
sudo chown -R $USER:$USER /opt/apptainer
sudo chmod -R 755 /opt/apptainer
```

Alternatively, you can use a custom directory:
```bash
ln2t_tools freesurfer --dataset mydataset --apptainer-dir /path/to/custom/dir
```

### FreeSurfer License

FreeSurfer requires a valid license file (free academic license available at [FreeSurfer Registration](https://surfer.nmr.mgh.harvard.edu/registration.html)).

**Default License Location**:
```bash
~/licenses/license.txt
```

To use a custom license location:
```bash
ln2t_tools freesurfer --dataset mydataset --fs-license /path/to/license.txt
```

### Internet Connection

An active internet connection is required for:
- Downloading container images (first run only)
- Downloading MELD Graph model weights (one-time setup)
- Accessing template spaces and atlases

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

## Tool-Specific Arguments with --tool-args

ln2t_tools uses a pass-through argument pattern for tool-specific options. This allows the tools to be updated independently of ln2t_tools, and gives you access to the full range of options each tool supports.

### How it Works

Core arguments (dataset, participant, version, HPC options) are handled by ln2t_tools. Tool-specific arguments are passed verbatim to the container using `--tool-args`:

```bash
ln2t_tools <tool> --dataset mydataset --participant-label 01 --tool-args "<tool-specific-arguments>"
```

### Examples

#### FreeSurfer
```bash
# Skip surface reconstruction (segmentation only)
ln2t_tools freesurfer --dataset mydataset --participant-label 01 \
    --tool-args "-parallel"
```

#### FastSurfer
```bash
# Run segmentation only (fast mode, ~5 min on GPU)
ln2t_tools fastsurfer --dataset mydataset --participant-label 01 \
    --tool-args "--seg-only --threads 4"

# Run on CPU with 3T atlas
ln2t_tools fastsurfer --dataset mydataset --participant-label 01 \
    --tool-args "--device cpu --3T --threads 8"
```

#### fMRIPrep
```bash
# Skip FreeSurfer reconstruction with specific output spaces
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
    --tool-args "--fs-no-reconall --output-spaces MNI152NLin2009cAsym:res-2 fsaverage:den-10k"

# Set number of threads
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
    --tool-args "--nprocs 8 --omp-nthreads 4"
```

#### QSIPrep
```bash
# Basic preprocessing with output resolution
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
    --tool-args "--output-resolution 1.25 --denoise-method dwidenoise"

# DWI-only processing
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
    --tool-args "--output-resolution 2.0 --dwi-only"
```

#### QSIRecon
```bash
# Tractography reconstruction
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
    --tool-args "--recon-spec mrtrix_multishell_msmt_ACT-hsvs"
```

#### MELD Graph
```bash
# Run with harmonization
ln2t_tools meld_graph --dataset mydataset --participant-label 01 \
    --tool-args "--harmonize --harmo-code H1"

# Skip feature extraction (if already computed)
ln2t_tools meld_graph --dataset mydataset --participant-label 01 \
    --tool-args "--skip-feature-extraction"
```

#### CVRmap
```bash
# Specific task with ROI probe
ln2t_tools cvrmap --dataset mydataset --participant-label 01 \
    --tool-args "--task gas --space MNI152NLin2009cAsym"

# Using ROI-based probe
ln2t_tools cvrmap --dataset mydataset --participant-label 01 \
    --tool-args "--roi-probe --roi-coordinates 0 -52 26 --roi-radius 6"
```

### Finding Available Options

Each tool has its own documentation for available options:
- **FreeSurfer**: `recon-all --help` or [FreeSurfer Wiki](https://surfer.nmr.mgh.harvard.edu/fswiki/recon-all)
- **FastSurfer**: [FastSurfer Documentation](https://deep-mi.org/research/fastsurfer/)
- **fMRIPrep**: `fmriprep --help` or [fMRIPrep Documentation](https://fmriprep.org/en/stable/usage.html)
- **QSIPrep**: `qsiprep --help` or [QSIPrep Documentation](https://qsiprep.readthedocs.io/)
- **QSIRecon**: [QSIRecon Documentation](https://qsiprep.readthedocs.io/)
- **MELD Graph**: [MELD Graph Documentation](https://meld-graph.readthedocs.io/)
- **CVRmap**: [CVRmap Documentation](https://github.com/arovai/cvrmap)

### Finding Missing Participants

The `--list-missing` flag helps identify which participants in your dataset still need processing for a specific tool. This is useful when:
- Resuming incomplete pipelines after errors
- Managing large cohorts with multiple tools
- Generating copy-paste commands to process missing participants

#### Basic Usage

```bash
# Show missing participants for a tool
ln2t_tools <tool> --dataset mydataset --list-missing
```

#### Example Output

```bash
$ ln2t_tools freesurfer --dataset mydataset --list-missing

FreeSurfer Processing Status for mydataset
============================================
Processed participants: 8
Missing participants: 5

Missing Participant IDs:
  sub-05
  sub-08
  sub-12
  sub-15
  sub-19

To process missing participants, run:
  ln2t_tools freesurfer --dataset mydataset --participant-label 05 08 12 15 19
```

#### With Multiple Tools

```bash
# Check missing for fMRIPrep
ln2t_tools fmriprep --dataset mydataset --list-missing

# Check missing for QSIPrep
ln2t_tools qsiprep --dataset mydataset --list-missing

# Check missing for MELD Graph
ln2t_tools meld_graph --dataset mydataset --list-missing
```

#### How It Works

The `--list-missing` flag:
1. Scans the rawdata directory for all participant IDs
2. Checks the tool's derivatives directory for completed outputs
3. Compares the two to identify missing participants
4. Generates a ready-to-use command for processing missing subjects
5. Automatically detects tool version from your setup

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

# Allow FreeSurfer reconstruction if pre-computed outputs don't exist
ln2t_tools fmriprep --dataset mydataset --participant-label 01 --fmriprep-reconall

# Custom output spaces
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-spaces MNI152NLin2009cAsym:res-1 fsaverage:den-10k"

# Combine multiple options
ln2t_tools fmriprep --dataset mydataset --participant-label 01 \
  --fmriprep-reconall \
  --tool-args "--output-spaces MNI152NLin2009cAsym:res-2 MNI152NLin6Asym:res-2"
```

**Important Notes**:
- **fMRIPrep now requires pre-computed FreeSurfer outputs by default**
  - If FreeSurfer outputs are not found, processing will be skipped
  - Run FreeSurfer first: `ln2t_tools freesurfer --dataset mydataset --participant-label 01`
  - Or use `--fmriprep-reconall` flag to allow fMRIPrep to run FreeSurfer reconstruction
- Automatically uses existing FreeSurfer outputs if found (from `freesurfer_{version}` directory)

---

### QSIPrep

QSIPrep performs preprocessing of diffusion MRI data.

#### Default Values
- **Version**: `1.0.1`
- **Output directory**: `~/derivatives/{dataset}-derivatives/qsiprep_1.0.1/`
- **Container**: `pennlinc/qsiprep:1.0.1`

#### Basic Usage

QSIPrep-specific options must be passed via `--tool-args`. The `--output-resolution`
option is required by QSIPrep.

```bash
# Process single participant (--output-resolution is required)
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25"

# Process multiple participants
ln2t_tools qsiprep --dataset mydataset --participant-label 01 02 03 \
  --tool-args "--output-resolution 1.5"
```

#### Advanced Options

```bash
# Use specific version
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --version 0.24.0 --tool-args "--output-resolution 1.25"

# Different denoising methods
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25 --denoise-method patch2self"

ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25 --denoise-method none"

# DWI-only processing (skip anatomical)
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25 --dwi-only"

# Anatomical-only processing
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25 --anat-only"

# Full example with multiple options
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --version 1.0.1 \
  --tool-args "--output-resolution 1.5 --denoise-method dwidenoise"
```

**Common QSIPrep Options** (pass via `--tool-args`):
- `--output-resolution <mm>`: Isotropic voxel size in mm (REQUIRED, e.g., 1.25, 1.5, 2.0)
- `--denoise-method <method>`: dwidenoise, patch2self, or none
- `--dwi-only`: Process only DWI data (skip anatomical)
- `--anat-only`: Process only anatomical data
- `--nprocs <n>`: Number of processes
- `--omp-nthreads <n>`: Number of OpenMP threads

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

# Full example combining options
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --qsiprep-version 1.0.1 \
  --version 1.1.1 \
  --recon-spec mrtrix_multishell_msmt_ACT-hsvs
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

MELD Graph performs automated FCD (Focal Cortical Dysplasia) lesion detection using FreeSurfer surfaces and deep learning.

#### Default Values
- **Version**: `v2.2.3`
- **Data directory**: `~/derivatives/{dataset}-derivatives/meld_graph_v2.2.3/`
- **Config directory**: `~/code/{dataset}-code/meld_graph_v2.2.3/config/`
- **Output location**: `~/derivatives/{dataset}-derivatives/meld_graph_v2.2.3/data/output/predictions_reports/`
- **Container**: `meldproject/meld_graph:v2.2.3`
- **FreeSurfer version**: `7.2.0` (default input - **required**)

> **⚠️ Compatibility Note**: MELD Graph **requires FreeSurfer 7.2.0 or earlier**. It does not work with FreeSurfer 7.3 and above. The default FreeSurfer version for MELD Graph is set to 7.2.0.

> **⚠️ Recommendation**: MELD works best with T1w scans only. If using T1w+FLAIR, interpret results with caution as FLAIR may introduce more false positives.

#### MELD Workflow Overview

MELD Graph has a unique three-step workflow:

1. **Download Weights** (one-time setup): Download pretrained model weights
2. **Harmonization** (optional but recommended): Compute scanner-specific harmonization parameters using 20+ subjects
3. **Prediction**: Run lesion detection on individual subjects

#### Directory Structure

MELD uses a specific directory structure with data in derivatives and config in code:
```
~/derivatives/{dataset}-derivatives/
└── meld_graph_v2.2.3/
    └── data/
        ├── input/
        │   └── sub-{id}/
        │       ├── T1/T1.nii.gz
        │       └── FLAIR/FLAIR.nii.gz (optional)
        └── output/
            ├── predictions_reports/
            ├── fs_outputs/
            └── preprocessed_surf_data/

~/code/{dataset}-code/
└── meld_graph_v2.2.3/
    └── config/
        ├── meld_bids_config.json
        └── dataset_description.json
```

---

#### Step 1: Download Model Weights (One-time Setup)

Before first use, download the MELD Graph pretrained model weights:

```bash
ln2t_tools meld_graph --dataset mydataset --download-weights
```

This downloads ~2GB of model weights into the MELD data directory.

---

#### Step 2: Harmonization (Optional but Recommended)

Harmonization adjusts for scanner/sequence differences and improves prediction accuracy. 

**Requirements**:
- At least 20 subjects from the same scanner/protocol
- Harmonization code (e.g., `H1`, `H2`) to identify this scanner
- BIDS `participants.tsv` file with demographic data (see below)

**Demographics Data**:

ln2t_tools automatically creates the MELD-compatible demographics file from your BIDS dataset's `participants.tsv`. The `participants.tsv` file should contain:

Required columns:
- `participant_id`: Subject ID (e.g., sub-001)
- `age` (or `Age`): Numeric age value
- `sex` (or `Sex`, `gender`, `Gender`): M/F or male/female

Optional columns:
- `group`: patient or control (defaults to 'patient' if missing)

Example `participants.tsv`:
```tsv
participant_id	age	sex	group
sub-001	25	M	patient
sub-002	28	F	control
sub-003	32	M	patient
```

**Compute harmonization parameters** (demographics file auto-generated):
```bash
ln2t_tools meld_graph --dataset mydataset \
  --participant-label 01 02 03 ... 20 \
  --harmonize \
  --harmo-code H1
```

The demographics CSV is automatically generated from `participants.tsv`. If you need to inspect or customize it, it will be created at:
```
~/derivatives/{dataset}-derivatives/meld_graph_v2.2.3/demographics_H1.csv
```

The demographics CSV format:
```csv
ID,Harmo code,Group,Age at preoperative,Sex
sub-001,H1,patient,25,male
sub-002,H1,control,28,female
sub-003,H1,patient,32,male
```

This runs FreeSurfer segmentation, extracts features, and computes harmonization parameters. Results saved in `preprocessed_surf_data/`.

> **Note**: This step needs to be run only once per scanner. You can reuse the harmonization parameters for all future subjects from the same scanner.

---

#### Step 3: Prediction - Run Lesion Detection

Once setup is complete, run predictions on individual subjects.

##### Basic Prediction (without harmonization)

```bash
# Single subject
ln2t_tools meld_graph --dataset mydataset --participant-label 01

# Multiple subjects
ln2t_tools meld_graph --dataset mydataset --participant-label 01 02 03
```

##### Prediction with Harmonization

```bash
ln2t_tools meld_graph --dataset mydataset \
  --participant-label 01 \
  --harmo-code H1
```

##### Using Precomputed FreeSurfer Outputs

If you already have FreeSurfer recon-all outputs:

```bash
ln2t_tools meld_graph --dataset mydataset \
  --participant-label 01 \
  --use-precomputed-fs \
  --fs-version 7.2.0
```

This will:
- Look for FreeSurfer outputs in `~/derivatives/{dataset}-derivatives/freesurfer_7.2.0/`
- Bind them to `/data/output/fs_outputs` in the container
- Automatically skip the FreeSurfer segmentation step
- Use the existing FreeSurfer surfaces for feature extraction

##### Skip Feature Extraction (Use Existing MELD Features)

If MELD features (`.sm3.mgh` files) are already extracted from a previous MELD run and you only want to rerun prediction:

```bash
ln2t_tools meld_graph --dataset mydataset \
  --participant-label 01 \
  --skip-feature-extraction
```

> **Important**: `--skip-feature-extraction` tells MELD to skip computing surface features (`.sm3.mgh` files). Use this only when those files already exist from a previous MELD run.

> **Note**: When using `--use-precomputed-fs`, MELD automatically detects existing FreeSurfer outputs and skips recon-all, but still runs feature extraction to create `.sm3.mgh` files. Don't use `--skip-feature-extraction` unless those feature files already exist.

---

#### Complete Example Workflow

```bash
# 1. Download weights (one-time)
ln2t_tools meld_graph --dataset epilepsy_study --download-weights

# 2. Compute harmonization with 25 subjects (one-time per scanner)
#    Demographics automatically created from participants.tsv
ln2t_tools meld_graph --dataset epilepsy_study \
  --participant-label 01 02 03 04 05 06 07 08 09 10 \
                        11 12 13 14 15 16 17 18 19 20 \
                        21 22 23 24 25 \
  --harmonize \
  --harmo-code H1

# 3. Run prediction on new patient with harmonization
ln2t_tools meld_graph --dataset epilepsy_study \
  --participant-label 26 \
  --harmo-code H1

# 4. Run prediction using precomputed FreeSurfer
ln2t_tools meld_graph --dataset epilepsy_study \
  --participant-label 27 \
  --use-precomputed-fs \
  --fs-version 7.2.0 \
  --harmo-code H1
```

---

#### Output Files

Results are saved in:
```
~/derivatives/{dataset}-derivatives/meld_graph_v2.2.3/data/output/predictions_reports/sub-{id}/
├── predictions/
│   ├── predictions.nii.gz           # Lesion probability map in native space
│   └── reports/
│       ├── {id}_prediction.pdf      # Visual report with inflated brain
│       ├── {id}_saliency.pdf        # Model attention maps
│       └── {id}_mri_slices.pdf      # Predictions on MRI slices
└── features/
    └── {id}.hdf5                    # Extracted surface features
```

---

## HPC Job Submission

All neuroimaging tools in ln2t_tools can be submitted to HPC clusters using SLURM (Simple Linux Utility for Resource Management). This feature is not limited to MELD Graph—you can use `--slurm` with any tool: FreeSurfer, fMRIPrep, QSIPrep, QSIRecon, FastSurfer, CVRmap, etc.

### Basic SLURM Submission

```bash
# FreeSurfer on HPC
ln2t_tools freesurfer --dataset mydataset \
  --participant-label 01 \
  --slurm \
  --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer/images/on/cluster

# fMRIPrep on HPC
ln2t_tools fmriprep --dataset mydataset \
  --participant-label 01 \
  --slurm \
  --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer/images/on/cluster

# QSIPrep on HPC
ln2t_tools qsiprep --dataset mydataset \
  --participant-label 01 \
  --tool-args "--output-resolution 1.25" \
  --slurm \
  --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer/images/on/cluster
```

### Advanced Options with Custom Resources

```bash
# SLURM with GPU allocation
ln2t_tools fastsurfer --dataset mydataset \
  --participant-label 01 02 03 \
  --slurm \
  --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer/images \
  --slurm-gpus 1 \
  --slurm-mem 16G \
  --slurm-time 4:00:00 \
  --slurm-partition gpu

# MELD Graph on HPC with harmonization
ln2t_tools meld_graph --dataset mydataset \
  --participant-label 01 \
  --harmo-code H1 \
  --slurm \
  --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer/images \
  --slurm-gpus 1 \
  --slurm-mem 32G
```

### SLURM Options

All tools support the following SLURM options:

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--slurm` | Enable SLURM job submission | No | Disabled |
| `--slurm-user` | Your username on the HPC cluster | **Yes** (with `--slurm`) | - |
| `--slurm-host` | HPC hostname to SSH into | No | `lyra.ulb.be` |
| `--slurm-apptainer-dir` | Path to apptainer images on cluster | **Yes** (with `--slurm`) | - |
| `--slurm-rawdata` | Path to rawdata directory on cluster | No | `$GLOBALSCRATCH/rawdata` |
| `--slurm-derivatives` | Path to derivatives on cluster | No | `$GLOBALSCRATCH/derivatives` |
| `--slurm-fs-license` | Path to FreeSurfer license on cluster | No | `$HOME/licenses/license.txt` |
| `--slurm-fs-version` | FreeSurfer version on cluster (for tools requiring it) | No | `7.2.0` |
| `--slurm-partition` | SLURM partition to submit to | No | None (cluster default) |
| `--slurm-time` | Job time limit (format: HH:MM:SS) | No | `1:00:00` |
| `--slurm-mem` | Memory allocation (format: Num[G,M]) | No | `32G` |
| `--slurm-gpus` | Number of GPUs to allocate | No | `1` |

### Examples by Tool

#### FreeSurfer on HPC
```bash
ln2t_tools freesurfer --dataset mydataset --participant-label 01 \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer
```

#### fMRIPrep on HPC with Custom Resources
```bash
ln2t_tools fmriprep --dataset mydataset --participant-label 01 02 03 \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer \
  --slurm-mem 48G --slurm-time 6:00:00
```

#### QSIPrep + QSIRecon Pipeline on HPC
```bash
# First run QSIPrep
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25" \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer \
  --slurm-gpus 1 --slurm-mem 32G

# Then run QSIRecon (uses QSIPrep output)
ln2t_tools qsirecon --dataset mydataset --participant-label 01 \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer
```

#### FastSurfer with GPU on HPC
```bash
ln2t_tools fastsurfer --dataset mydataset --participant-label 01 02 03 \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer \
  --slurm-gpus 1 --slurm-partition gpu --slurm-time 4:00:00
```

#### MELD Graph on HPC
```bash
ln2t_tools meld_graph --dataset mydataset --participant-label 01 \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer \
  --slurm-gpus 1 --slurm-mem 32G
```

### How HPC Submission Works

When you use the `--slurm` flag:

1. **Script Generation**: ln2t_tools generates a SLURM batch script with your job parameters
2. **Remote Submission**: The script is transferred to your HPC cluster and submitted via SSH
3. **Job Monitoring**: Job ID is printed so you can monitor progress with `squeue`
4. **Automatic Cleanup**: Job scripts are cleaned up after completion

### Troubleshooting HPC Submission

**SSH Connection Issues**:
```bash
# Verify SSH access to your cluster
ssh your_username@lyra.ulb.be
# or with custom host
ln2t_tools freesurfer --dataset mydataset --slurm-host custom.hpc.org ...
```

**Missing Apptainer Directory**:
```bash
# Ensure directory exists on cluster and contains images
ssh your_username@lyra.ulb.be ls /path/to/apptainer/

# Copy images if needed
scp freesurfer_7.3.2.sif your_username@lyra.ulb.be:/path/to/apptainer/
```

**Job Not Starting**:
```bash
# Check SLURM queue status on cluster
ssh your_username@lyra.ulb.be squeue -u your_username
# or check job details
ssh your_username@lyra.ulb.be sinfo
```

---

#### Advanced Options Summary

```bash
ln2t_tools meld_graph --dataset DATASET [OPTIONS]

Required:
  --dataset DATASET                Dataset name

Participant Selection:
  --participant-label ID [ID ...]  One or more participant IDs

MELD Workflow:
  --download-weights               Download model weights (run once)
  --harmonize                      Compute harmonization parameters only
  --harmo-code CODE                Harmonization code (e.g., H1, H2)

FreeSurfer:
  --fs-version VERSION             FreeSurfer version (default: 7.2.0, max: 7.2.0)
  --use-precomputed-fs             Use existing FreeSurfer outputs (skips recon-all, runs feature extraction)
  --skip-feature-extraction        Skip MELD feature extraction (only if .sm3.mgh files already exist)

SLURM Options:
  --slurm                          Submit to SLURM cluster
  --slurm-user USER                HPC username (required with --slurm)
  --slurm-apptainer-dir PATH       Apptainer images directory on cluster (required with --slurm)
  --slurm-gpus N                   Number of GPUs (default: 1)
  --slurm-mem SIZE                 Memory allocation (default: 32G)
  --slurm-time TIME                Time limit (default: 1:00:00)

Version:
  --version VERSION                MELD Graph version (default: v2.2.3)
```

---

#### Prerequisites & Important Notes

**Prerequisites**:
- BIDS-formatted T1w (and optionally FLAIR) images in `~/rawdata/{dataset}-rawdata/`
- For using precomputed FreeSurfer: outputs in `~/derivatives/{dataset}-derivatives/freesurfer_7.2.0/`
- For harmonization: `participants.tsv` with demographic data (age, sex, group) and 20+ subjects from same scanner

**Important Limitations**:
- **FreeSurfer version must be 7.2.0 or earlier** (7.3+ not compatible)
- **Not appropriate for**: tuberous sclerosis, hippocampal sclerosis, hypothalamic hamartoma, periventricular heterotopia, previous resections
- **Research use only**: Not FDA/EMA approved for clinical diagnosis
- **T1w recommended**: FLAIR may increase false positives

**Error Messages**:
```
No FreeSurfer output found for participant 01.
MELD Graph requires FreeSurfer recon-all to be completed first.
Note: MELD Graph requires FreeSurfer 7.2.0 or earlier (current default: 7.2.0).
```
→ Run FreeSurfer 7.2.0 first or use `--use-precomputed-fs` if outputs exist

```
Harmonization recommended with at least 20 subjects. You have 15 subjects.
```
→ Add more subjects for reliable harmonization or proceed without harmonization

---

## Checking HPC Job Status

After submitting jobs with `--hpc`, you can monitor their progress without needing to SSH into the cluster. ln2t_tools automatically tracks submitted jobs and provides an easy way to check their status.

### Basic Status Checking

```bash
# Show status of recent jobs (last 20 submitted)
ln2t_tools --hpc-status

# Show status of a specific job ID
ln2t_tools --hpc-status 12345678

# Show status of multiple job IDs
ln2t_tools --hpc-status 12345678 12345679 12345680

# Show all jobs for a specific dataset
ln2t_tools --hpc-status --dataset mydataset

# Show all jobs for a specific tool
ln2t_tools --hpc-status --tool freesurfer
```

### Job Status Categories

The status output shows jobs organized by their current state:

- **⏳ PENDING**: Job is waiting to start (in SLURM queue)
- **▶️ RUNNING**: Job is currently executing
- **✅ COMPLETED**: Job finished successfully
- **❌ FAILED**: Job encountered an error
- **⏱️ TIMEOUT**: Job exceeded time limit
- **⛔ CANCELLED**: Job was cancelled by user or system

### Example Output

```bash
$ ln2t_tools --hpc-status

======================================================================
HPC Job Status Summary
======================================================================

⏳ PENDING:
  Job 12345678: freesurfer / mydataset / sub-01
  Job 12345679: fmriprep / mydataset / sub-02

▶️  RUNNING:
  Job 12345680: qsiprep / mydataset / sub-03

✅ COMPLETED:
  Job 12345677: freesurfer / mydataset / sub-02

❌ FAILED/ERROR:
  Job 12345676: meld_graph (Timed out)
    Tool: meld_graph, Dataset: mydataset, Sub: sub-01
    Reason: TIME_LIMIT_EXCEEDED

======================================================================
Total: 6 jobs
  Pending: 2
  Running: 1
  Completed: 1
  Failed: 2
======================================================================
```

### Detailed Job Information

For more detailed information about a specific job, you can also manually query the HPC cluster if you have SSH access:

```bash
# Check a specific job on the cluster
ssh your_username@lyra.ulb.be squeue -j 12345678

# View job history and exit codes
ssh your_username@lyra.ulb.be sacct -j 12345678 --format=JobID,State,ExitCode,Reason,Start,End,Elapsed

# View job output/error logs on cluster
ssh your_username@lyra.ulb.be cat ~/ln2t_hpc_jobs/{dataset}/{tool}_{participant}*_{job_id}.{out,err}
```

### Job History

ln2t_tools keeps a local record of all submitted jobs in `~/.ln2t_tools/hpc_jobs.json`. This allows you to check job status even after logging out of your local machine or restarting.

```bash
# View the job history file (JSON format)
cat ~/.ln2t_tools/hpc_jobs.json

# Filter recent jobs with jq (if installed)
jq '.[] | select(.submit_time > "2025-02-01") | {job_id, tool, dataset, state}' ~/.ln2t_tools/hpc_jobs.json
```

### Practical Workflow Example

```bash
# 1. Submit jobs to HPC cluster
ln2t_tools freesurfer --dataset mydataset --participant-label 01 02 03 \
  --hpc --hpc-user your_username --hpc-apptainer-dir /path/to/apptainer

# Output shows: ✓ Job submitted successfully! Job IDs: 12345678, 12345679, 12345680

# 2. Check status immediately
ln2t_tools --hpc-status

# Output: 3 pending jobs

# 3. Check later (wait 30 minutes)
ln2t_tools --hpc-status

# Output: 1 running, 2 pending

# 4. Check dataset-specific jobs
ln2t_tools --hpc-status --dataset mydataset

# 5. If issues detected, check detailed job logs
ssh your_username@lyra.ulb.be sacct -j 12345678

# 6. If job timed out, increase time limit and resubmit
ln2t_tools freesurfer --dataset mydataset --participant-label 01 \
  --hpc --hpc-time 48:00:00 --hpc-user your_username --hpc-apptainer-dir /path/to/apptainer
```

### Troubleshooting Job Status Queries

**Jobs not showing in status**:
- Jobs are stored locally in `~/.ln2t_tools/hpc_jobs.json`
- Deleting this file will remove job history
- Job status requires either the local job file or SSH access to the cluster

**Want live status updates from cluster**:
```bash
# Provide HPC credentials for live query
ln2t_tools --hpc-status 12345678 \
  --hpc-user your_username \
  --hpc-hostname lyra.ulb.be
```

**Job appears stuck**:
```bash
# Check more details on the cluster
ssh your_username@lyra.ulb.be scontrol show job 12345678

# Check cluster queue status
ssh your_username@lyra.ulb.be squeue

# Check system resources
ssh your_username@lyra.ulb.be sinfo
```

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

---

## Data Import

ln2t_tools includes import utilities to convert source data to BIDS format:

- **DICOM**: Convert DICOM files to BIDS using dcm2bids with optional defacing
- **MRS**: Convert MRS data to BIDS using spec2nii
- **Physio**: Convert GE physiological monitoring data to BIDS using phys2bids

### DICOM Import

Convert DICOM files to BIDS-compliant NIfTI format with optional defacing:

```bash
# Basic DICOM import
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom

# Import with defacing (opt-in)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --deface

# Import multiple participants
ln2t_tools import --dataset mydataset --participant-label 01 02 03 --datatype dicom

# Compress source data after successful import
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --compress-source
```

**Notes**:
- Defacing is **opt-in** via `--deface` flag (not applied by default)
- Uses pydeface v2.0.6 via Singularity container
- Adds metadata to JSON sidecars: `Defaced: true`, `DefacingMethod`, `DefacingTimestamp`
- Auto-creates `dataset_description.json` if missing (required by pydeface)

### MRS Import

Convert Magnetic Resonance Spectroscopy data to BIDS format:

```bash
# Import MRS data
ln2t_tools import --dataset mydataset --participant-label 01 --datatype mrs

# Import with session
ln2t_tools import --dataset mydataset --participant-label 01 --datatype mrs --session 01

# Import and compress source
ln2t_tools import --dataset mydataset --participant-label 01 --datatype mrs --compress-source
```

### Physio Import

Convert GE physiological monitoring data (respiratory, PPG) to BIDS format with automatic fMRI matching.

**By default**, uses in-house processing (simple and fast). Optionally use `--phys2bids` for phys2bids-based processing.

```bash
# Import physio data using in-house processing (default)
# Config file will be auto-detected from sourcedata/configs/physio.json
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio

# Or specify a custom config file location
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio \
  --physio-config /path/to/custom_config.json

# Import with session
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio \
  --session 01

# Import and compress source
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio \
  --compress-source

# Use phys2bids instead (optional)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio \
  --phys2bids
```

#### Configuration File

The physio config file should be placed in your sourcedata directory:

**Preferred location:**
```
~/sourcedata/{dataset}-sourcedata/configs/physio.json
```

**Legacy location (also supported):**
```
~/sourcedata/{dataset}-sourcedata/physio/config.json
```

Configuration file is **required** and must contain task-specific DummyVolumes definitions.

Create a JSON configuration file with the following format:

```json
{
  "DummyVolumes": {
    "task-rest": 5,
    "task-motor_run-01": 3,
    "task-motor_run-02": 4,
    "_comment": "Specify dummy volumes for each task/run"
  },
  "PhysioTimeTolerance": 1,
  "PhysioTimeToleranceUnits": "h"
}
```

**DummyVolumes**: Map of task-specific dummy volumes. The StartTime will be calculated as:
```
StartTime = -(30s + (TR × DummyVolumes))
```

Where:
- 30s = GE scanner pre-recording period (hardcoded)
- TR = Repetition time from fMRI JSON metadata
- DummyVolumes = Number of dummy scans from config for that specific task/run
- Negative sign indicates recording started BEFORE the first trigger

Keys should match BIDS naming:
- `"task-<taskname>"` for simple task names
- `"task-<taskname>_run-<runnum>"` for multiple runs

Example: If TR=2.0s and DummyVolumes=5:
```
StartTime = -(30s + (2.0s × 5)) = -40.0s
```

**PhysioTimeTolerance & PhysioTimeToleranceUnits** (Optional):
- Used during **pre-import** to match physio files to exam start time
- Default if not specified: 1 hour
- `PhysioTimeTolerance`: Numeric value
- `PhysioTimeToleranceUnits`: One of:
  - `"s"` for seconds
  - `"min"` for minutes  
  - `"h"` for hours (default)
- Example: `"PhysioTimeTolerance": 2, "PhysioTimeToleranceUnits": "h"` for 2-hour tolerance
- If config file is present and these fields are defined, they will be used; otherwise, default 1 hour is applied with a warning

See `example_physio_config.json` for a template.

#### In-House Processing Details

**Input**:
- GE physio files: Single column of numerical values
- Filename format: `{SIGNAL}Data_{SEQUENCE}_{TIMESTAMP}`
  - RESP: Respiratory signal (25 Hz)
  - PPG: Cardiac/photoplethysmography signal (100 Hz)

**Output**:
- TSV file: Single column with physio values (gzip compressed)
- JSON sidecar with:
  - `SamplingFrequency`: 25 Hz (RESP) or 100 Hz (PPG)
  - `StartTime`: Calculated from config (30s + TR × DummyVolumes)
  - `Columns`: BIDS recording type ("respiratory" or "cardiac")

**How It Works**:
1. Parses GE physio filenames to extract signal type (RESP/PPG) and timestamp
2. Reads fMRI JSON metadata (AcquisitionTime, TR) and NIfTI files (volumes)
3. Matches physio files to fMRI runs based on timestamps (35s tolerance)
4. Loads physio data (single column of values)
5. Creates BIDS-compliant TSV.GZ and JSON files
6. Calculates StartTime based on config file

#### phys2bids Processing (Optional)

When using `--phys2bids`:
- Requires phys2bids Apptainer container (auto-built if missing)
- Recipe: `apptainer_recipes/phys2bids.def`
- Built to: `/opt/apptainer/phys2bids.phys2bids.latest.sif`
- Auto-generates phys2bids heuristic file
- Includes time column and trigger channel
- Conversion logs: `~/sourcedata/{dataset}-sourcedata/phys2bids_logs/`

**Filename Format**:
- Example: `RESPData_epiRTphysio_1124202515_54_58_279` (Nov 24, 2025, 15:54:58)
- Data files are processed, Trig files are ignored

**Requirements**:
- fMRI data must already be in BIDS format in rawdata
- Physio files in `~/sourcedata/{dataset}-sourcedata/physio/{ID}/`

**Output**:
- Converted BIDS files: `~/rawdata/{dataset}-rawdata/sub-{ID}/func/`

### Import MEG Data

```bash
# Import MEG data (config file will be auto-detected from sourcedata/configs/meg2bids.json)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype meg

# Or specify a custom config file location
ln2t_tools import --dataset mydataset --participant-label 01 --datatype meg \
  --meg-config /path/to/custom_config.json

# Import with session (if not auto-detected)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype meg \
  --session 01

# Import multiple participants
ln2t_tools import --dataset mydataset --participant-label 01 02 03 --datatype meg
```

#### MEG Source Data Structure

MEG source data should be organized as:

```
~/sourcedata/{dataset}-sourcedata/
├── meg/
│   ├── meg_1001/              # MEG subject folder (4-digit ID)
│   │   ├── 250115/            # Session date folder (YYMMDD format)
│   │   │   ├── rest.fif       # Raw MEG FIF files
│   │   │   ├── task1.fif
│   │   │   └── task1_mc.fif   # MaxFilter derivative (auto-detected)
│   │   └── 250122/            # Second session (if applicable)
│   │       └── rest.fif
│   ├── meg_1002/
│   │   └── 250116/
│   │       └── rest.fif
│   └── ...
├── configs/
│   └── meg2bids.json          # MEG conversion config (required)
└── participants_complete.tsv  # Subject mapping (required)
```

**Key Points**:
- **meg_XXXX folders**: 4-digit MEG ID for each subject
- **Date folders**: Session date in YYMMDD format (e.g., 250115 for Jan 15, 2025)
- **FIF files**: Neuromag/Elekta/MEGIN MEG data files
- **Split files**: Large files (>2GB) automatically split as `file.fif`, `file-1.fif`, `file-2.fif`
- **Derivatives**: MaxFilter processed files with suffixes like `_mc`, `_tsss`, `_ave` are auto-detected

#### MEG Configuration File

Create `meg2bids.json` in your configs directory with pattern matching rules:

```json
{
  "dataset": {
    "dataset_name": "MyMEGStudy",
    "datatype": "meg"
  },
  "file_patterns": [
    {
      "pattern": "*rest*.fif",
      "task": "rest",
      "run_extraction": "last_digits",
      "description": "Resting state recording"
    },
    {
      "pattern": "*visual*.fif",
      "task": "visual",
      "run_extraction": "last_digits",
      "description": "Visual task"
    }
  ],
  "calibration": {
    "system": "triux",
    "auto_detect": true,
    "maxfilter_root": "/path/to/MEG/maxfilter"
  },
  "derivatives": {
    "pipeline_name": "maxfilter",
    "maxfilter_version": "v2.2.20"
  },
  "options": {
    "allow_maxshield": true,
    "overwrite": true
  }
}
```

**file_patterns**: List of rules to match FIF filenames to BIDS task names
- `pattern`: Glob pattern (e.g., `*rest*.fif`)
- `task`: BIDS task name
- `run_extraction`: How to extract run numbers (`"last_digits"` or `"none"`)

**calibration**: Calibration file settings
- `system`: `"triux"` or `"vectorview"`
- `auto_detect`: Enable automatic calibration file detection
- `maxfilter_root`: Path to MEG/maxfilter directory containing `ctc/` and `sss/` subdirectories

**derivatives**: MaxFilter derivative handling (optional)
- `pipeline_name`: Name for derivatives folder (e.g., `"maxfilter"`) or `"none"` to skip
- `maxfilter_version`: Version to append to folder name

**options**:
- `allow_maxshield`: Allow reading MaxShield data (default: true)
- `overwrite`: Overwrite existing BIDS files (default: true)

See `example_meg_config.json` for a complete template.

#### Participants Mapping

The `participants_complete.tsv` file must map MEG IDs to BIDS subject IDs:

```tsv
participant_id	meg_id
sub-01	1001
sub-02	1002
sub-03	1003
```

**Format**:
- `participant_id`: BIDS subject ID (with `sub-` prefix)
- `meg_id`: 4-digit MEG ID (matches `meg_XXXX` folder name)

#### Session Auto-Detection

- **Single session**: If only one date folder exists, no session label is added
- **Multiple sessions**: Automatically numbered as `ses-01`, `ses-02`, etc. based on date folder order
- **Manual override**: Use `--session` flag to specify a particular session

#### Calibration Files

MEG systems require calibration files for accurate measurements:

**Triux System**:
- Crosstalk: `ct_sparse_triux2.fif`
- Fine-calibration: `sss_cal_XXXX_YYMMDD.dat` (date-matched to session)

**VectorView System**:
- Crosstalk: `ct_sparse_vectorview.fif`
- Fine-calibration: `sss_cal_vectorview.dat`

Files are auto-detected from `maxfilter_root/ctc/` and `maxfilter_root/sss/` directories and copied to BIDS rawdata as:
- `sub-{id}_acq-crosstalk_meg.fif`
- `sub-{id}_acq-calibration_meg.dat`

#### MaxFilter Derivatives

Automatically detected suffixes:
- `_sss`, `_tsss`: Signal Space Separation
- `_mc`: Movement compensation
- `_trans`, `_quat`: Head position transforms
- `_av`, `_ave`: Averaged data

Example: `rest_mc_ave.fif` → BIDS: `sub-01_task-rest_proc-mc-ave_meg.fif` in derivatives

#### MEG Metadata Features

**AssociatedEmptyRoom (BIDS Standard)**:
- Automatically detects empty room recordings (`*task-noise*_meg.fif`)
- Adds `AssociatedEmptyRoom` field to all MEG JSON sidecars
- References the empty room file used for noise characterization
- Enables advanced noise processing and artifact rejection
- See [BIDS Specification](https://bids-specification.readthedocs.io/en/stable/glossary.html#associatedemptyroom-metadata)

Example JSON output:
```json
{
  "TaskName": "visual",
  "Manufacturer": "Elekta",
  "SamplingFrequency": 1000.0,
  "AssociatedEmptyRoom": "sub-01_task-noise_meg.fif",
  ...
}
```

**Head Shape and Digitized Points**:
- Extracts digitized head points from raw FIF files using `raw.info['dig']`
- Creates `*_headshape.pos` file in Polhemus format (plain text: x y z coordinates per line)
- File is session-specific and shared across all tasks/runs
- Contains head surface points and anatomical landmarks
- Automatically skips if file already exists (no-op on re-conversion)
- **Note**: Only created if digitized points were recorded during acquisition

Example file: `sub-01_ses-01_headshape.pos` (coordinates in meters, 7 decimal precision)

**Output Structure**:

```
~/rawdata/{dataset}-rawdata/
└── sub-01/
    └── [ses-01/]
        └── meg/
            ├── sub-01_task-rest_meg.fif
            ├── sub-01_task-rest_meg.json
            ├── sub-01_task-rest_channels.tsv
            ├── sub-01_acq-crosstalk_meg.fif
            └── sub-01_acq-calibration_meg.dat

~/derivatives/{dataset}-derivatives/
└── maxfilter_v2.2.20/
    └── sub-01/
        └── [ses-01/]
            └── meg/
                └── sub-01_task-rest_proc-mc-ave_meg.fif
```

### Import All Datatypes

```bash
# Import all available datatypes for a participant
ln2t_tools import --dataset mydataset --participant-label 01 --datatype all

# With multiple options
ln2t_tools import --dataset mydataset --participant-label 01 --datatype all --deface --compress-source
```

### Directory Naming Convention

ln2t_tools uses strict folder matching for source data:

- **Without sessions**: `{INITIALS}{ID}` (e.g., `FF042` for Fantastic_Fox participant 042)
- **With sessions**: `{INITIALS}{ID}SES{session}` (e.g., `FF042SES1`)
- **Initials**: Auto-extracted from dataset name (first letter of each word)
  - "2024-Fantastic_Fox-123" → "FF"
  - "Careful_Cod" → "CC"

Example source structure:
```
~/sourcedata/mydataset-sourcedata/
├── dicom/
│   ├── FF001/
│   ├── FF002/
│   └── FF002SES2/
├── mrs/
│   └── FF001/
└── physio/
    └── FF001/
```

---

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
# → --tool-args (QSIPrep options passed via --tool-args)

ln2t_tools qsirecon --<TAB>
# → --qsiprep-version --recon-spec
```

---

## Common Workflows

### Full Pipeline for Single Participant

```bash
# 1. Run FreeSurfer
ln2t_tools freesurfer --dataset mydataset --participant-label 01

# 2. Run fMRIPrep (uses FreeSurfer output)
ln2t_tools fmriprep --dataset mydataset --participant-label 01

# 3. Run QSIPrep (--output-resolution required via --tool-args)
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25"

# 4. Run QSIRecon (uses QSIPrep output)
ln2t_tools qsirecon --dataset mydataset --participant-label 01

# 5. Run MELD Graph (uses FreeSurfer output)
ln2t_tools meld_graph --dataset mydataset --participant-label 01
```

### Resume Processing with Missing Participants

If processing was interrupted or failed for some participants, you can easily identify and reprocess them:

```bash
# List missing participants for FreeSurfer
ln2t_tools freesurfer --dataset mydataset --list-missing

# Output shows command to run missing participants
# To run, simply copy-paste the suggested command from output

# Or manually specify missing participants
ln2t_tools freesurfer --dataset mydataset --participant-label 05 08 12 15 19
```

### Monitoring and Utilities

```bash
# List available datasets
ln2t_tools --list-datasets

# List missing subjects for any tool
ln2t_tools freesurfer --dataset mydataset --list-missing
ln2t_tools fmriprep --dataset mydataset --list-missing
ln2t_tools qsiprep --dataset mydataset --list-missing
ln2t_tools meld_graph --dataset mydataset --list-missing

# Check running instances
ln2t_tools --list-instances

# Limit parallel executions
ln2t_tools --max-instances 3 --dataset mydataset
```

### Processing on HPC Cluster

```bash
# Submit FreeSurfer to SLURM cluster
ln2t_tools freesurfer --dataset mydataset \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer

# Submit multiple participants to HPC
ln2t_tools fmriprep --dataset mydataset --participant-label 01 02 03 \
  --slurm --slurm-user your_username \
  --slurm-apptainer-dir /path/to/apptainer \
  --slurm-gpus 1 --slurm-mem 32G

# List missing and submit to HPC (using output from --list-missing)
ln2t_tools qsiprep --dataset mydataset --participant-label 05 08 12 \
  --tool-args "--output-resolution 1.25" \
  --slurm --slurm-user your_username --slurm-apptainer-dir /path/to/apptainer
```

---

## Troubleshooting

### Images Not Found
If Apptainer images are missing, they will be built automatically:
```bash
ln2t_tools will attempt to build: freesurfer/freesurfer:7.3.2
```

### FreeSurfer License
Ensure your FreeSurfer license is at `~/licenses/license.txt` (default location) or specify:
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
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
  --tool-args "--output-resolution 1.25"
ln2t_tools qsirecon --dataset mydataset --participant-label 01

# MELD Graph needs FreeSurfer
ln2t_tools freesurfer --dataset mydataset --participant-label 01
ln2t_tools meld_graph --dataset mydataset --participant-label 01
```

---

## Adding a New Tool

ln2t_tools uses a modular plugin architecture that allows you to add new neuroimaging tools without modifying the core codebase. Each tool is self-contained in its own directory with CLI arguments, validation, and processing logic.

### Quick Start

1. Create a directory: `ln2t_tools/tools/mytool/`
2. Create `__init__.py` and `tool.py`
3. Implement the `BaseTool` interface
4. Add default version to `utils/defaults.py`
5. Register the tool in `utils/utils.py` (image lookup and command builder)
6. Add the tool to the supported tools list in `ln2t_tools.py`
7. Update bash completion (optional but recommended)
8. Your tool is automatically discovered and available!

### Step-by-Step Guide

#### 1. Create the Tool Directory

```bash
mkdir -p ln2t_tools/tools/mytool
```

#### 2. Create `__init__.py`

```python
# ln2t_tools/tools/mytool/__init__.py
"""My custom neuroimaging tool."""

from .tool import MyTool

# Required: export TOOL_CLASS for auto-discovery
TOOL_CLASS = MyTool

__all__ = ['MyTool', 'TOOL_CLASS']
```

#### 3. Create `tool.py` with BaseTool Implementation

```python
# ln2t_tools/tools/mytool/tool.py
"""My custom tool implementation."""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from bids import BIDSLayout

from ln2t_tools.tools.base import BaseTool
from ln2t_tools.utils.defaults import DEFAULT_MYTOOL_VERSION

logger = logging.getLogger(__name__)


class MyTool(BaseTool):
    """My custom neuroimaging tool.
    
    Brief description of what the tool does and when to use it.
    """
    
    # Required class attributes
    name = "mytool"                                    # CLI subcommand name
    help_text = "Brief help shown in ln2t_tools -h"   # Short description
    description = "Detailed tool description"          # Long description
    default_version = DEFAULT_MYTOOL_VERSION           # Default container version
    requires_gpu = False                               # Set True if GPU-accelerated
    
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        """Add tool-specific CLI arguments.
        
        Common arguments (--dataset, --participant-label, --version, etc.)
        are added automatically. Only add tool-specific options here.
        """
        parser.add_argument(
            "--my-option",
            default="default_value",
            help="Description of my option (default: default_value)"
        )
        parser.add_argument(
            "--my-flag",
            action="store_true",
            help="Enable special feature"
        )
    
    @classmethod
    def validate_args(cls, args: argparse.Namespace) -> bool:
        """Validate tool-specific arguments.
        
        Returns True if arguments are valid, False otherwise.
        Log error messages to explain validation failures.
        """
        # Example: check that required options are set
        if getattr(args, 'my_option', None) == 'invalid':
            logger.error("--my-option cannot be 'invalid'")
            return False
        return True
    
    @classmethod
    def check_requirements(
        cls,
        layout: BIDSLayout,
        participant_label: str,
        args: argparse.Namespace
    ) -> bool:
        """Check if all requirements are met to process this participant.
        
        Verify that required input files exist and any prerequisites
        (like FreeSurfer outputs) are available.
        """
        # Example: check for T1w image
        t1w_files = layout.get(
            subject=participant_label,
            suffix='T1w',
            extension=['.nii', '.nii.gz']
        )
        
        if not t1w_files:
            logger.warning(f"No T1w images found for {participant_label}")
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
        """Get the output directory path for this participant.
        
        Follow BIDS derivatives naming: {tool}_{version}/sub-{id}/
        """
        version = args.version or cls.default_version
        subdir = f"sub-{participant_label}"
        if session:
            subdir = f"{subdir}_ses-{session}"
        
        return dataset_derivatives / f"{cls.name}_{version}" / subdir
    
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
        """Build the Apptainer command to run the tool.
        
        Returns a list of command components that will be joined
        and executed via Apptainer.
        """
        version = args.version or cls.default_version
        output_dir = cls.get_output_dir(
            dataset_derivatives, participant_label, args
        )
        
        # Build Apptainer command
        cmd = [
            "apptainer", "run", "--cleanenv",
            # Bind directories
            "-B", f"{dataset_rawdata}:/input:ro",
            "-B", f"{dataset_derivatives}:/output",
            # Container image
            apptainer_img,
            # Tool arguments
            "/input",
            "/output",
            "--participant-label", participant_label,
        ]
        
        # Add tool-specific options
        if getattr(args, 'my_flag', False):
            cmd.append("--my-flag")
        
        my_option = getattr(args, 'my_option', 'default_value')
        cmd.extend(["--my-option", my_option])
        
        return cmd
```

#### 4. Add Default Version Constant

Add a default version constant for your tool in `ln2t_tools/utils/defaults.py`. **Important**: Use the exact Docker tag for reproducibility (e.g., `"v1.0.0"` or `"cuda-v2.4.2"`, not just `"1.0.0"`):

```python
# ln2t_tools/utils/defaults.py

# Add with the other default version constants
# Use the EXACT Docker tag for reproducibility
DEFAULT_MYTOOL_VERSION = "v1.0.0"  # Must match Docker Hub tag exactly
```

Then import and use it in your tool:

```python
# ln2t_tools/tools/mytool/tool.py
from ln2t_tools.utils.defaults import DEFAULT_MYTOOL_VERSION

class MyTool(BaseTool):
    default_version = DEFAULT_MYTOOL_VERSION
```

#### 5. Register Tool in Utils (Critical Step!)

This step is essential for the tool to work. You must register your tool in two functions in `ln2t_tools/utils/utils.py`:

##### 5.1 Add to `ensure_image_exists()`

This function maps tool names to Docker image owners and handles container building:

```python
# ln2t_tools/utils/utils.py - in ensure_image_exists() function

def ensure_image_exists(tool, version, images_dir, logger):
    """Ensure Apptainer image exists, build from Docker Hub if needed."""
    
    # ... existing code ...
    
    # Add your tool to the tool owner mapping
    if tool == "freesurfer":
        tool_owner = "freesurfer"
    elif tool == "fmriprep":
        tool_owner = "nipreps"
    elif tool == "mytool":           # <-- ADD THIS
        tool_owner = "myorg"         # Docker Hub organization/user
    else:
        raise ValueError(f"Unknown tool: {tool}")
    
    # The version is used directly as the Docker tag
    # Make sure DEFAULT_MYTOOL_VERSION in defaults.py matches the exact Docker tag
```

##### 5.2 Add to `build_apptainer_cmd()`

This function builds the actual Apptainer execution command with all bindings:

```python
# ln2t_tools/utils/utils.py - in build_apptainer_cmd() function

def build_apptainer_cmd(tool, participant_label, args, ...):
    # ... existing code ...
    
    elif tool == "mytool":
        cmd = [
            "apptainer", "run", "--cleanenv",
            "-B", f"{dataset_rawdata}:/input:ro",
            "-B", f"{dataset_derivatives}:/output",
            apptainer_img,
            "/input", "/output",
            "--participant-label", participant_label,
        ]
        # Add tool-specific options
        if getattr(args, 'my_flag', False):
            cmd.append("--my-flag")
```

**Note**: Without this registration step, running your tool will fail with "Unsupported tool mytool" error!

##### 5.3 Add to Supported Tools List in `ln2t_tools.py`

The main processing loop has a hardcoded list of supported tools. Add your tool to this list:

```python
# ln2t_tools/ln2t_tools.py - in the main processing loop

# Find this line and add your tool:
if tool not in ["freesurfer", "fastsurfer", "fmriprep", "qsiprep", "qsirecon", "meld_graph", "mytool"]:
    logger.warning(f"Unsupported tool {tool} for dataset {dataset}, skipping")
    continue
```

#### 6. Update Bash Completion (Optional but Recommended)

Add your tool to `ln2t_tools/completion/ln2t_tools_completion.bash`:

```bash
# Add tool name to the tools list
tools="freesurfer fmriprep qsiprep qsirecon fastsurfer meld_graph mytool"

# Add tool-specific completions
_ln2t_tools_mytool() {
    case "${prev}" in
        --my-option)
            COMPREPLY=( $(compgen -W "value1 value2 value3" -- "${cur}") )
            return 0
            ;;
    esac
    
    COMPREPLY=( $(compgen -W "--my-option --my-flag --help" -- "${cur}") )
}
```

#### 7. Create Apptainer Recipe (Optional)

For tools without pre-built containers, create a recipe in `apptainer_recipes/`:

```bash
# apptainer_recipes/mytool.def
Bootstrap: docker
From: myorg/mytool:1.0.0

%labels
    Author Your Name
    Version 1.0.0
    Description My custom tool container

%post
    # Any additional setup commands
    apt-get update && apt-get install -y curl

%runscript
    exec /opt/mytool/run.sh "$@"
```

#### 8. Test Your Tool

```bash
# Verify tool is discovered
ln2t_tools --help
# Should show: mytool - Brief help shown in ln2t_tools -h

# View tool-specific help
ln2t_tools mytool --help

# Run on a participant
ln2t_tools mytool --dataset mydata --participant-label 01 --my-option value
```

### Best Practices

1. **Follow BIDS naming**: Output directories should follow `{tool}_{version}/` pattern
2. **Validate inputs**: Check for required files in `check_requirements()`
3. **Log clearly**: Use `logger.info()` for progress, `logger.warning()` for issues
4. **Handle versions**: Use `args.version or cls.default_version` pattern
5. **Document options**: Provide clear `--help` text for all arguments

### Advanced Features

#### Custom Processing Logic

Override `process_subject()` for complex workflows:

```python
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
    """Custom processing with multi-step workflow."""
    # Step 1: Pre-processing
    # Step 2: Main processing
    # Step 3: Post-processing
    return True
```

#### HPC Script Generation

Override `generate_hpc_script()` for custom batch scripts:

```python
@classmethod
def generate_hpc_script(
    cls,
    participant_label: str,
    dataset: str,
    args: argparse.Namespace,
    **kwargs
) -> str:
    """Generate custom HPC batch script."""
    return f"""#!/bin/bash
#SBATCH --job-name={cls.name}_{participant_label}
#SBATCH --gpus={1 if cls.requires_gpu else 0}
...
"""
```

### Directory Structure

After adding a tool, your directory structure should look like:

```
ln2t_tools/tools/
├── __init__.py          # Tool registry and discovery
├── base.py              # BaseTool abstract class
├── freesurfer/          # Existing tool
│   ├── __init__.py
│   └── tool.py
├── fmriprep/            # Existing tool
│   ├── __init__.py
│   └── tool.py
└── mytool/              # Your new tool
    ├── __init__.py
    └── tool.py
```

---
