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
  --harmonize-only \
  --harmo-code H1
```

Alternatively, you can provide your own demographics CSV file:
```bash
ln2t_tools meld_graph --dataset mydataset \
  --participant-label 01 02 03 ... 20 \
  --harmonize-only \
  --harmo-code H1 \
  --demographics /path/to/demographics_H1.csv
```

The demographics CSV format (if providing your own):
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
  --skip-segmentation
```

> **Important**: `--skip-segmentation` tells MELD to skip feature extraction, not FreeSurfer recon-all. Use this only when `.on_lh.thickness.sm3.mgh` and similar files already exist from a previous MELD run.

> **Note**: When using `--use-precomputed-fs`, MELD automatically detects existing FreeSurfer outputs and skips recon-all, but still runs feature extraction to create `.sm3.mgh` files. Don't use `--skip-segmentation` unless those feature files already exist.

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
  --harmonize-only \
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

**Interpreting Results**:
- High probability clusters (red/yellow in reports) indicate potential FCD locations
- Review both prediction maps and saliency maps
- Cross-reference with clinical information
- **Remember**: This is a research tool, not a diagnostic device

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
  --harmonize-only                 Compute harmonization parameters only
  --harmo-code CODE                Harmonization code (e.g., H1, H2)
  --demographics FILE              Demographics CSV (optional - auto-generated from participants.tsv)

FreeSurfer:
  --fs-version VERSION             FreeSurfer version (default: 7.2.0, max: 7.2.0)
  --use-precomputed-fs             Use existing FreeSurfer outputs (skips recon-all, runs feature extraction)
  --skip-segmentation              Skip MELD feature extraction (only if .sm3.mgh files already exist)

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

#### Troubleshooting

**Q: Can I use FreeSurfer 7.3 or 7.4?**  
A: No, MELD Graph only works with FreeSurfer 7.2.0 or earlier due to surface format changes.

**Q: Is harmonization mandatory?**  
A: No, but highly recommended. Without harmonization, expect more false positives.

**Q: How many subjects do I need for harmonization?**  
A: Minimum 20, more is better. Must be from the same scanner/protocol.

**Q: Where are the model weights stored?**  
A: In `~/derivatives/{dataset}-derivatives/meld_graph_{version}/data/` (downloaded automatically with `--download-weights`)

**Q: Can I reuse harmonization parameters?**  
A: Yes! Once computed for a scanner (e.g., H1), use `--harmo-code H1` for all future subjects from that scanner.

**Q: MELD is taking a long time**  
A: FreeSurfer recon-all takes 6-12 hours per subject. Use `--use-precomputed-fs` if you already have FreeSurfer outputs.

**Q: What if I don't have a participants.tsv file?**  
A: Create one in your BIDS rawdata directory with the required columns (participant_id, age, sex), or provide a demographics CSV file directly with `--demographics`.

**Q: What if my participants.tsv is missing age or sex information?**  
A: ln2t_tools will show an error message indicating which columns are missing. You can either update your participants.tsv file or create a custom demographics CSV file and use `--demographics`.

**Q: Can I use a custom demographics file instead of participants.tsv?**  
A: Yes! Use `--demographics /path/to/your/demographics.csv` to provide your own file. It should have columns: ID, Harmo code, Group, Age at preoperative, Sex.

**Q: Understanding MELD's workflow - what gets computed when?**  
A: MELD has 3 steps:
1. **FreeSurfer Segmentation** (6-12 hours): Creates surfaces and parcellations. Skip with `--use-precomputed-fs` if you already ran FreeSurfer.
2. **Feature Extraction** (15-30 min): Creates `.on_lh.thickness.sm3.mgh` and similar smoothed feature files from FreeSurfer outputs. MELD needs these for prediction.
3. **Prediction/Harmonization** (5-10 min): Uses extracted features for lesion detection.

When using `--use-precomputed-fs`: MELD skips step 1 (finds existing FreeSurfer outputs) but still runs steps 2 and 3.

**Q: What does `--skip-segmentation` actually do?**  
A: Despite the name, it tells MELD to skip **feature extraction** (step 2), not FreeSurfer segmentation. Use this ONLY when `.sm3.mgh` files already exist from a previous MELD run. Don't use it with `--use-precomputed-fs` unless you've already run MELD once on that subject.

**Q: I get errors about missing `.sm3.mgh` files**  
A: Don't use `--skip-segmentation` with `--use-precomputed-fs`. MELD needs to create these feature files from your FreeSurfer outputs first.

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

If no config file is found in either location, the tool will use default values (DummyVolumes=5).

Create a JSON configuration file with the following format:

```json
{
  "DummyVolumes": 5
}
```

**DummyVolumes**: Number of dummy/discard volumes at the start of the fMRI acquisition. The StartTime will be calculated as:
```
StartTime = -(30s + (TR × DummyVolumes))
```

Where:
- 30s = GE scanner pre-recording period (hardcoded)
- TR = Repetition time from fMRI JSON metadata
- DummyVolumes = Number of dummy scans from config file
- Negative sign indicates recording started BEFORE the first trigger

Example: If TR=2.0s and DummyVolumes=5:
```
StartTime = -(30s + (2.0s × 5)) = -40.0s
```

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
  ├── meldproject.meld_graph.v2.2.3.sif
  └── phys2bids.phys2bids.latest.sif
```

**Apptainer Recipe Files**:
ln2t_tools includes Apptainer recipe files for building containers:
- `apptainer_recipes/phys2bids.def`: Recipe for phys2bids container
- Containers are automatically built on first use
- See `apptainer_recipes/README.md` for manual build instructions

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


