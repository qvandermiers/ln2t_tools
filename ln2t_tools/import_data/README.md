# BIDS Data Import Module

This module provides tools to import various types of source data into BIDS format.

## Overview

The `ln2t_tools import` subcommand provides a unified interface to convert raw data from multiple sources into BIDS-compliant format. It supports:

- **DICOM** images (via `dcm2bids` - converts MRI/fMRI scans)
- **MRS** spectroscopy data (via `spec2bids` - converts magnetic resonance spectroscopy)
- **Physio** physiological recordings (in-house processing or via `phys2bids` - converts respiratory and cardiac data)

The tool automates the entire import workflow: finding source data, applying format conversions, organizing output into BIDS structure, and optionally compressing source data and applying defacing.

## Directory Structure

Source data should be organized as follows:

```
~/sourcedata/<dataset-name>-sourcedata/
├── dicom/
│   ├── <DS_INITIALS><ID>/          # e.g., CB001, HP001SES1
│   └── ...
├── mrs/ (or pfiles/)
│   ├── <DS_INITIALS><ID>/
│   └── ...
├── physio/
│   ├── <DS_INITIALS><ID>/
│   └── ...
└── configs/ (or dcm2bids/ or spec2bids/)
    ├── dcm2bids.json
    └── spec2bids.json
```

## Usage

### Basic Import

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 003
```

This will import all available datatypes (DICOM, MRS, Physio) for the specified participants.

### DICOM Only

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --datatype dicom \
  --ds-initials CB
```

Converts DICOM files to NIfTI and organizes them into BIDS structure using your `dcm2bids.json` configuration.

**Parameters**:
- `--ds-initials CB`: Dataset initials to match source directory names (e.g., CB001, CB002)
- If omitted, the tool will try to match participant labels directly

### MRS Only

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --datatype mrs
```

Converts MR Spectroscopy data (P-files, DICOM, RDA, etc.) to BIDS format using your `spec2bids.json` configuration.

### Physio Only (In-House Processing)

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --datatype physio
```

Processes physiological recordings (respiratory, cardiac) using built-in in-house processing with your `physio.json` configuration.

### Physio with phys2bids

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --datatype physio \
  --phys2bids
```

Uses the containerized `phys2bids` tool instead of in-house processing for more advanced functionality.

### With Sessions

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --session 1 \
  --ds-initials HP \
  --datatype all
```

Imports multi-session data. Session will be added as `ses-{session}` in BIDS filenames.

### With Source Compression

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --compress-source
```

Creates `.tar.gz` archives of successfully imported source data. Original directories are preserved. Useful for archiving large DICOM datasets.

### Enable Defacing (DICOM Only)

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --datatype dicom \
  --deface
```

Runs the `pydeface` Apptainer container to anonymize anatomical images by removing facial features. Improves data sharing compliance. Requires `cbinyu/bids-pydeface:v2.0.6` container.

### Custom Virtual Environment

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --import-env ~/venvs/custom_env
```

Uses a specific Python virtual environment instead of the default. Useful if you have multiple environments with different tool versions installed.

### Skip Already-Imported Participants (Default Behavior)

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 003
```

By default, the import tool checks if a participant already has imported data (i.e., if `~/rawdata/<dataset>-rawdata/sub-XXX/` exists). If data is found, that participant is **skipped** to avoid redundant re-processing.

This is useful when:
- Extending an existing dataset with new participants
- Re-running import after fixing configuration files (previously imported data is preserved)
- Safely resuming interrupted imports

If you want to import participants that were already processed, you'll need to either:
1. Delete their directories from rawdata, or
2. Use the `--overwrite` flag (see below)

### Overwrite Existing Data

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --overwrite
```

By default, participants with existing data are skipped. Use `--overwrite` to force re-processing of all specified participants, overwriting any existing BIDS-formatted data.

**Warning**: This will replace existing derivatives, so use with caution if you have completed processing downstream of import (e.g., FreeSurfer, fMRI preprocessing).

### Custom Configuration Files

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --physio-config /path/to/custom/physio.json
```

Explicitly specify configuration file paths instead of auto-detecting from sourcedata directory.

### All Parameters

```bash
ln2t_tools import \
  --dataset <dataset> \
  --participant-label <labels> \
  --datatype <type> \
  --ds-initials <initials> \
  --session <session> \
  --overwrite \
  --compress-source \
  --deface \
  --phys2bids \
  --import-env <path> \
  --physio-config <path>
```

**Parameters**:
- `--dataset`: Dataset name (required)
- `--participant-label`: Space-separated participant IDs without 'sub-' prefix (optional if auto-discovering from DICOM)
- `--datatype`: dicom, mrs, physio, or all (default: all)
- `--ds-initials`: Dataset initials for matching source directories (optional, auto-inferred from dataset name)
- `--session`: Session label without 'ses-' prefix (optional)
- `--overwrite`: Force re-processing of participants even if they already have imported data (default: False)
- `--compress-source`: Create .tar.gz archives of source data after successful import (default: False)
- `--deface`: Run pydeface on anatomical images (default: False)
- `--phys2bids`: Use phys2bids instead of in-house physio processing (default: False)
- `--import-env`: Path to Python virtual environment with import tools (default: ~/venvs/general_purpose_env)
- `--physio-config`: Path to physiological data configuration file (default: auto-detect)

## Configuration Files

All configuration files are JSON format and should be placed in:
- **Primary location**: `~/sourcedata/<dataset>-sourcedata/configs/` (recommended)
- **Legacy location**: `~/sourcedata/<dataset>-sourcedata/<datatype>/` (still supported)

The tool searches for config files in this priority order:
1. Explicit path via CLI option (`--dicom-config`, `--mrs-config`, `--physio-config`)
2. `configs/<type>.json` (modern location)
3. `<datatype>/config.json` (legacy location)
4. Built-in defaults (if available)

---

### DICOM Configuration (`dcm2bids.json`)

**Purpose**: Define how to convert DICOM files to NIfTI and organize them into BIDS structure.

**Search paths**:
- `~/sourcedata/<dataset>-sourcedata/configs/dcm2bids.json`
- `~/sourcedata/<dataset>-sourcedata/dcm2bids/config.json`

**What it does**:
- Specifies `dcm2niix` command-line options for DICOM to NIfTI conversion
- Defines how to identify and extract different scan types (T1w, T2w, BOLD, DWI, etc.)
- Maps DICOM SeriesDescription to BIDS datatype/suffix
- Applies custom JSON sidecar modifications (e.g., adding TaskName)
- Handles multi-echo, multi-run, and multi-session acquisitions

**Example Configuration**:
```json
{
  "dcm2niixOptions": "-b y -ba y -z y -f '%3s_%f_%p_%t_%d' -d 9",
  "descriptions": [
    {
      "datatype": "anat",
      "suffix": "T1w",
      "criteria": {
        "SeriesDescription": "*3D T1 BRAVO*"
      }
    },
    {
      "datatype": "anat",
      "suffix": "T2w",
      "criteria": {
        "SeriesDescription": "*T2*"
      }
    },
    {
      "datatype": "func",
      "suffix": "bold",
      "custom_entities": "task-restingstate",
      "criteria": {
        "SeriesDescription": "fMRI*"
      },
      "sidecar_changes": {
        "TaskName": "restingstate",
        "RepetitionTime": 2.0
      }
    },
    {
      "datatype": "dwi",
      "suffix": "dwi",
      "criteria": {
        "SeriesDescription": "*DWI*"
      }
    }
  ]
}
```

**Key Fields**:
- `dcm2niixOptions`: Flags passed to `dcm2niix` binary
  - `-b y`: Save BIDS format
  - `-ba y`: Save BIDS anonymized
  - `-z y`: Compress output
  - `-f`: Filename pattern
  - `-d`: Folder depth
- `descriptions`: Array of scan type definitions
  - `datatype`: BIDS datatype (anat, func, dwi, mrs, etc.)
  - `suffix`: BIDS suffix (T1w, bold, dwi, etc.)
  - `criteria`: Dictionary matching DICOM headers (SeriesDescription, EchoTime, etc.)
  - `custom_entities`: Additional BIDS entities (task, acq, run, etc.)
  - `sidecar_changes`: JSON modifications for the sidecar file

**See**: [dcm2bids documentation](https://unfmontreal.github.io/Dcm2Bids/3.2.0/)

---

### MRS Configuration (`spec2bids.json`)

**Purpose**: Define how to convert MR Spectroscopy (MRS) data to BIDS format.

**Search paths**:
- `~/sourcedata/<dataset>-sourcedata/configs/spec2bids.json`
- `~/sourcedata/<dataset>-sourcedata/spec2bids/config.json`

**What it does**:
- Specifies scanner manufacturer (GE, Siemens, Philips, etc.)
- Defines how to identify MRS acquisitions from protocol names or headers
- Maps MRS protocols to BIDS entities (voi, acq, etc.)
- Configures output file naming and metadata

**Example Configuration**:
```json
{
  "manufacturer": "ge",
  "descriptions": [
    {
      "criteria": {
        "ProtocolName": "PRESS reference ACL"
      },
      "custom_entities": "voi-acl_acq-press",
      "sidecar_changes": {
        "RepetitionTime": 2.0,
        "EchoTime": 0.08,
        "PulseSequenceName": "PRESS"
      }
    },
    {
      "criteria": {
        "ProtocolName": "GABA ACL"
      },
      "custom_entities": "voi-acl_acq-mega",
      "sidecar_changes": {
        "RepetitionTime": 2.0,
        "EchoTime": 0.068,
        "PulseSequenceName": "MEGA-PRESS"
      }
    }
  ]
}
```

**Key Fields**:
- `manufacturer`: Scanner type (ge, siemens, philips)
- `descriptions`: Array of MRS acquisition definitions
  - `criteria`: Header matching (ProtocolName, EchoTime, etc.)
  - `custom_entities`: BIDS entities (voi=voxel-of-interest, acq=acquisition label)
  - `sidecar_changes`: JSON metadata for the acquisition

**Note**: MRS data can come from different file formats (DICOM, P-files, RDA, etc.). Ensure `spec2bids` is installed with support for your file format.

---

### Physio Configuration (`physio.json`)

**Purpose**: Define task-specific dummy volume counts for synchronizing physiological recordings with fMRI scans.

**Search paths**:
- `~/sourcedata/<dataset>-sourcedata/configs/physio.json`
- `~/sourcedata/<dataset>-sourcedata/physio/config.json`
- **Required**: Configuration file must exist with task definitions (no default values)

**What it does**:
- Specifies the number of dummy volumes acquired at the start of each fMRI scan
- Allows different dummy volume counts per task and per run
- Uses this to calculate **`StartTime`** in BIDS physiological recordings
  - Formula: `StartTime = -(30s + TR × DummyVolumes)`
  - The 30s accounts for GE scanner pre-recording before the first trigger
  - Negative StartTime indicates recording started BEFORE the first fMRI volume
- Creates BIDS-compliant physiological JSON sidecars with correct timing information

**Physiological Recording Processing**:
- Converts raw physiological signals (respiratory, cardiac/PPG) to BIDS format
- Outputs gzipped TSV files (`.tsv.gz`) with single column of measurements
- Creates accompanying JSON metadata with SamplingFrequency and StartTime
- Fixed sampling rates:
  - **Respiratory (RESP)**: 25 Hz
  - **Cardiac (PPG)**: 100 Hz

**Example Configuration**:
```json
{
  "DummyVolumes": {
    "task-rest": 5,
    "task-motor_run-01": 3,
    "task-motor_run-02": 4,
    "task-breathhold": 6,
    "_comment": "Specify DummyVolumes (dummy scans) for each task/run"
  },
  "PhysioTimeTolerance": 1.5,
  "PhysioTimeToleranceUnits": "h",
  "_description": {
    "DummyVolumes": "Map of task-specific dummy volumes. Keys should match BIDS naming: 'task-<taskname>' or 'task-<taskname>_run-<runnum>'",
    "PhysioTimeTolerance": "Time tolerance for matching physio files to exam start during pre-import (optional)",
    "PhysioTimeToleranceUnits": "Units for PhysioTimeTolerance: 's' (seconds), 'min' (minutes), or 'h' (hours, default)"
  }
}
```

**Key Fields**:
- `DummyVolumes`: Dictionary mapping task identifiers to dummy volume counts
  - Keys format: `"task-<taskname>"` or `"task-<taskname>_run-<runnum>"`
  - Values: Integer count of dummy volumes for that task/run
  - Lookup priority (most → least specific):
    1. `task-{task}_run-{run}` (if run exists)
    2. `task-{task}`
  - **Required**: All tasks/runs in your dataset must be defined

- `PhysioTimeTolerance` (Optional): Time tolerance value for matching physio files to exam start time
  - Used during **pre-import** phase
  - Numeric value (positive number)
  - If not specified, default is 1 hour with a warning message

- `PhysioTimeToleranceUnits` (Optional): Units for PhysioTimeTolerance
  - One of: `"s"` (seconds), `"min"` (minutes), `"h"` (hours)
  - Default if not specified: `"h"` (hours)
  - Example: `"PhysioTimeTolerance": 2, "PhysioTimeToleranceUnits": "min"` means 2 minutes

**Example Scenario**:
If TR=2s (repetition time) and DummyVolumes=5:
```
StartTime = -(30 + 2×5) = -40 seconds
```
This means physiological recording started 40 seconds before the first fMRI volume.

**Output Example** (BIDS sidecar JSON):
```json
{
  "SamplingFrequency": 25.0,
  "StartTime": -40.0,
  "Columns": ["respiratory"]
}
```

**Error Handling**:
- If a task in the physio data is not found in the DummyVolumes config, the import will fail with a clear error message
- Ensure all task names in your fMRI data are included in the configuration
- If PhysioTimeToleranceUnits is invalid, pre-import will fail with a clear error message

---

### Processing Configuration (`processing_config.tsv`)

**Purpose**: Specify which tool versions to use for each dataset (optional).

**Location**: `~/rawdata/<dataset>-rawdata/processing_config.tsv`

**What it does**:
- Maps datasets to specific tool versions (FreeSurfer, fMRIPrep, QSIPrep, etc.)
- Allows version control across multiple datasets in your system
- Used by downstream processing pipelines to know which version to use
- Format: Tab-separated values (TSV)

**Example Configuration**:
```tsv
dataset	freesurfer	fmriprep	qsiprep
2024-Gleaming_Lyrebird-cf70b1f72dd6	7.3.2	25.1.4	0.24.0
2024-Shining_Pheasant-a1b2c3d4e5f6		25.1.4	
2024-Bright_Falcon-f6e5d4c3b2a1	7.4.0		0.24.0
```

**Columns**:
- `dataset`: Dataset name (must match exactly)
- `freesurfer`: FreeSurfer version to use
- `fmriprep`: fMRIPrep version to use
- `qsiprep`: QSIPrep version to use
- Other columns for other processing tools as needed

**Notes**:
- Leave blank if a tool should not be run for that dataset
- Versions must match available container image versions
- This file is optional - if not present, tools use default versions

## Requirements and Dependencies

### DICOM Import Requirements
- **`dcm2bids`**: Python package for DICOM to BIDS conversion
  - Install: `pip install dcm2bids`
  - Includes `dcm2niix` binary for DICOM to NIfTI conversion
  - Runs in Python virtual environment
- **Configuration**: JSON file defining scan type mapping (see Configuration Files section)
- **Optional - Defacing**:
  - Singularity/Apptainer image: `cbinyu/bids-pydeface:v2.0.6`
  - Anonymizes anatomical images by removing facial features
  - Improves data sharing compliance

### MRS Import Requirements
- **`spec2bids`**: Python package for MRS to BIDS conversion
  - Install: `pip install spec2bids` (or clone from [GitHub](https://github.com/arovai/spec2bids))
  - Includes `spec2nii` for MRS file conversion
  - Supports multiple file formats (DICOM, P-files, RDA, etc.)
  - Runs in Python virtual environment
- **Configuration**: JSON file defining MRS protocol mapping (see Configuration Files section)

### Physio Import Requirements
- **In-house processing** (default):
  - No external dependencies
  - Built into ln2t_tools
  - Fast and lightweight
- **Optional - phys2bids**:
  - `phys2bids`: Containerized tool for physiological data conversion
  - Use with `--phys2bids` flag
  - More feature-rich but slower than in-house processing
- **Configuration**: JSON file defining dummy volume counts (see Configuration Files section)
  - Optional - uses sensible defaults if not provided

### Virtual Environment Setup

You must have Python virtual environment(s) with required tools installed:

```bash
# Create general-purpose environment for import tools
python3 -m venv ~/venvs/import_env
source ~/venvs/import_env/bin/activate

# Install DICOM tools
pip install dcm2bids

# Install MRS tools (if needed)
pip install spec2bids

# Deactivate when done
deactivate
```

The import subcommand will automatically activate the virtual environment before running conversions.

## Installation

### Setup Virtual Environment

```bash
# Create virtual environment
python3 -m venv ~/venvs/general_purpose_env
source ~/venvs/general_purpose_env/bin/activate

# Install tools
pip install dcm2bids

# Install spec2bids (if available)
pip install spec2bids
# or clone from repository
```

### Install Defacing Container (Optional)

```bash
singularity pull ~/singularities/cbinyu.bids_pydeface.v2.0.6.sif \
  docker://cbinyu/bids-pydeface:v2.0.6
```

## Workflow Example

### Full Dataset Import

```bash
# Dataset variables
DATASET="2024-Colorful_Bear-6f9c8e8bfe82"
PARTICIPANTS="001 002 003 004 005"
DS_INITIALS="CB"

# Import all data types
ln2t_tools import \
  --dataset $DATASET \
  --participant-label $PARTICIPANTS \
  --ds-initials $DS_INITIALS \
  --compress-source

# Verify import
tree ~/rawdata/${DATASET}-rawdata
```

### Multi-Session Import

```bash
DATASET="2024-Happy_Penguin-abc123"
PARTICIPANTS="001 002 003"
SESSION="1"
DS_INITIALS="HP"

ln2t_tools import \
  --dataset $DATASET \
  --participant-label $PARTICIPANTS \
  --session $SESSION \
  --ds-initials $DS_INITIALS \
  --datatype all \
  --compress-source
```

## Output Structure

After successful import, your dataset will be organized in BIDS format in the `rawdata` directory:

```
~/rawdata/<dataset-name>-rawdata/
├── sub-001/
│   ├── anat/              # Anatomical images (T1w, T2w, etc.)
│   │   ├── sub-001_T1w.nii.gz
│   │   ├── sub-001_T1w.json
│   │   ├── sub-001_T2w.nii.gz
│   │   └── sub-001_T2w.json
│   ├── func/              # Functional images and physio
│   │   ├── sub-001_task-rest_bold.nii.gz
│   │   ├── sub-001_task-rest_bold.json
│   │   ├── sub-001_task-rest_physio.tsv.gz
│   │   ├── sub-001_task-rest_physio.json
│   │   ├── sub-001_task-motor_run-01_bold.nii.gz
│   │   ├── sub-001_task-motor_run-01_bold.json
│   │   ├── sub-001_task-motor_run-01_physio.tsv.gz
│   │   ├── sub-001_task-motor_run-01_physio.json
│   │   └── ...
│   ├── dwi/               # Diffusion weighted images
│   │   ├── sub-001_dwi.nii.gz
│   │   ├── sub-001_dwi.bval
│   │   ├── sub-001_dwi.bvec
│   │   ├── sub-001_dwi.json
│   │   └── ...
│   └── mrs/               # MR Spectroscopy data
│       ├── sub-001_mrs.nii.gz
│       └── sub-001_mrs.json
├── sub-002/
│   ├── ses-01/            # Session-specific data (if applicable)
│   │   ├── anat/
│   │   │   ├── sub-002_ses-01_T1w.nii.gz
│   │   │   └── sub-002_ses-01_T1w.json
│   │   ├── func/
│   │   │   ├── sub-002_ses-01_task-rest_bold.nii.gz
│   │   │   ├── sub-002_ses-01_task-rest_bold.json
│   │   │   ├── sub-002_ses-01_task-rest_physio.tsv.gz
│   │   │   ├── sub-002_ses-01_task-rest_physio.json
│   │   │   └── ...
│   │   └── mrs/
│   │       ├── sub-002_ses-01_mrs.nii.gz
│   │       └── sub-002_ses-01_mrs.json
│   └── ses-02/
│       └── ...
├── sub-003/
│   └── ...
├── dataset_description.json    # BIDS required metadata file
├── participants.tsv            # Participant list with metadata
├── README                       # Dataset README (optional)
└── CHANGES                      # Dataset changelog (optional)
```

### BIDS Sidecar Metadata

**Anatomical images** (`sub-XXX_T1w.json`):
```json
{
  "EchoTime": 0.00456,
  "RepetitionTime": 2.3,
  "FlipAngle": 9,
  "MagneticFieldStrength": 3.0
}
```

**Functional images** (`sub-XXX_task-rest_bold.json`):
```json
{
  "TaskName": "rest",
  "RepetitionTime": 2.0,
  "EchoTime": 0.03,
  "FlipAngle": 90,
  "NumberOfVolumes": 500,
  "NumberOfVolumesDiscardedByScanner": 5,
  "NumberOfVolumesDiscardedByUser": 0
}
```

**Physiological data** (`sub-XXX_task-rest_physio.json`):
```json
{
  "SamplingFrequency": 25.0,
  "StartTime": -40.0,
  "Columns": ["respiratory"],
  "Description": "Respiratory signal during resting state fMRI"
}
```

The StartTime indicates when physiological recording began relative to the first fMRI volume (negative = before first volume).

**MRS data** (`sub-XXX_mrs.json`):
```json
{
  "EchoTime": 0.03,
  "RepetitionTime": 2.0,
  "Manufacturer": "Siemens",
  "ManufacturersModelName": "Prisma",
  "MagneticFieldStrength": 3.0
}
```

### Compressed Source Data

If you used `--compress-source`, original source directories are archived:

```bash
~/sourcedata/<dataset-name>-sourcedata/
├── dicom/
│   ├── <DS_INITIALS><ID>.tar.gz
│   ├── <DS_INITIALS><ID>/            # Original still present (not deleted)
│   └── ...
├── mrs/
│   ├── <DS_INITIALS><ID>.tar.gz
│   ├── <DS_INITIALS><ID>/
│   └── ...
└── physio/
    ├── <DS_INITIALS><ID>.tar.gz
    ├── <DS_INITIALS><ID>/
    └── ...
```

The original directories are **preserved** - only archives are created for backup/archival purposes.

### Log Files

Import logs are saved to:
```bash
~/rawdata/<dataset-name>-rawdata/.logs/
├── import_<timestamp>.log    # Overall import log
├── dcm2bids_<timestamp>.log  # DICOM conversion details
├── spec2bids_<timestamp>.log # MRS conversion details
└── physio_<timestamp>.log    # Physio processing details
```

### Validation

After import, the tool will:
1. Display a summary of successful/failed imports
2. Show the directory structure using `tree` (if available)
3. List imported files per participant
4. Report any missing or problematic data

## Troubleshooting

### Config file not found
Ensure your config files are at:
- `~/sourcedata/<dataset>-sourcedata/configs/dcm2bids.json`
- `~/sourcedata/<dataset>-sourcedata/configs/spec2bids.json`
- `~/sourcedata/<dataset>-sourcedata/configs/physio.json`

Or legacy locations:
- `~/sourcedata/<dataset>-sourcedata/dcm2bids/config.json`
- `~/sourcedata/<dataset>-sourcedata/spec2bids/config.json`
- `~/sourcedata/<dataset>-sourcedata/physio/config.json`

For physio import, the config file is **required** and must contain task-specific DummyVolumes definitions.
You can also specify a custom config location with `--physio-config /path/to/config.json`.

### Virtual environment not found
Specify custom path:
```bash
--import-env ~/path/to/venv
```

### Source directory not found
Check that source data exists at:
```bash
~/sourcedata/<dataset-name>-sourcedata/dicom/<DS_INITIALS><ID>
```

### dcm2bids fails
Check:
1. dcm2bids config syntax is valid JSON
2. SeriesDescription criteria match your DICOM headers
3. Virtual environment has dcm2bids installed

### spec2bids not found
Install spec2bids or provide path to custom installation.

## Quick Reference

### Common Command Patterns

**Standard import (all datatypes)**:
```bash
ln2t_tools import --dataset <name> --participant-label 001 002 003
```

**DICOM only with defacing**:
```bash
ln2t_tools import --dataset <name> --participant-label 001 --datatype dicom --deface
```

**MRS with custom config**:
```bash
ln2t_tools import --dataset <name> --participant-label 001 --datatype mrs --mrs-config /path/to/spec2bids.json
```

**Multi-session with compression**:
```bash
ln2t_tools import --dataset <name> --participant-label 001 002 --session 1 --compress-source
```

**Physio with phys2bids container**:
```bash
ln2t_tools import --dataset <name> --participant-label 001 --datatype physio --phys2bids
```

### Configuration File Search Order

Files are searched in this order (first found is used):

1. **Custom path** (if specified with `--*-config`):
   - `/path/to/custom/dcm2bids.json`
   - `/path/to/custom/spec2bids.json`
   - `/path/to/custom/physio.json`

2. **Standard location**:
   - `~/sourcedata/<dataset>-sourcedata/configs/dcm2bids.json`
   - `~/sourcedata/<dataset>-sourcedata/configs/spec2bids.json`
   - `~/sourcedata/<dataset>-sourcedata/configs/physio.json`

3. **Legacy locations**:
   - `~/sourcedata/<dataset>-sourcedata/dcm2bids/config.json`
   - `~/sourcedata/<dataset>-sourcedata/spec2bids/config.json`
   - `~/sourcedata/<dataset>-sourcedata/physio/config.json`

4. **Defaults**:
   - DICOM: Requires config file (fails if not found)
   - MRS: Requires config file (fails if not found)
   - Physio: Uses `DummyVolumes=5` if no config found

### Essential File Naming

**Source data directories**:
- Format: `<DS_INITIALS><PARTICIPANT_ID>` (e.g., `CB001`, `HP001SES1`)
- Must match participant labels and session (if applicable)
- Located in `~/sourcedata/<dataset>-sourcedata/{dicom,mrs,physio}/`

**Output files**:
- Anatomical: `sub-<label>_T1w.nii.gz`, `sub-<label>_T2w.nii.gz`, etc.
- Functional: `sub-<label>_task-<task>_run-<run>_bold.nii.gz`
- Physio: `sub-<label>_task-<task>_physio.tsv.gz`
- MRS: `sub-<label>_mrs.nii.gz`
- JSON sidecars for all NIfTI files

### Virtual Environment Management

Check available environments:
```bash
ls ~/venvs/
```

Create new environment:
```bash
python3 -m venv ~/venvs/import_env
source ~/venvs/import_env/bin/activate
pip install dcm2bids spec2bids
deactivate
```

Use specific environment:
```bash
ln2t_tools import --dataset <name> --participant-label 001 --import-env ~/venvs/import_env
```

## Notes

- Dataset initials (`--ds-initials`) help match source directories to participants
- If omitted, the tool tries to find matching directories by participant ID
- Compression creates `.tar.gz` archives and preserves original directories
- Defacing is optional and can be enabled with `--deface` flag
- A minimal `dataset_description.json` is automatically created if needed for defacing
- The tool cleans up `tmp_dcm2bids` directory after processing

## Future Enhancements

- [ ] Implement physio data import with phys2bids
- [ ] Add automatic BIDS validation
- [ ] Support for custom post-processing hooks
- [ ] Parallel processing for multiple participants
- [ ] Resume capability for interrupted imports
