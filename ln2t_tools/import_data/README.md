# BIDS Data Import Module

This module provides tools to import various types of source data into BIDS format.

## Overview

The import module supports three types of source data:
- **DICOM** images (via dcm2bids)
- **MRS** spectroscopy data (via spec2bids)
- **Physio** physiological recordings (via phys2bids - to be implemented)

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

This will import all available datatypes (dicom, mrs, physio) for the specified participants.

### DICOM Only

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --datatype dicom \
  --ds-initials CB
```

### With Sessions

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --session 1 \
  --ds-initials HP \
  --datatype all
```

### With Source Compression

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 002 \
  --compress-source
```

This will create `.tar.gz` archives of the source data after successful import.

### Enable Defacing

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --datatype dicom \
  --deface
```

### Custom Virtual Environment

```bash
ln2t_tools import \
  --dataset <dataset-name> \
  --participant-label 001 \
  --import-env ~/venvs/custom_env
```

## Configuration Files

### dcm2bids Configuration

Place your dcm2bids configuration at:
- `~/sourcedata/<dataset>-sourcedata/dcm2bids/config.json` or
- `~/sourcedata/<dataset>-sourcedata/configs/dcm2bids.json`

Example:
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
            "datatype": "func",
            "suffix": "bold",
            "custom_entities": "task-restingstate",
            "criteria": {
                "SeriesDescription": "fMRI*"
            },
            "sidecar_changes": {
                "TaskName": "restingstate"
            }
        }
    ]
}
```

See [dcm2bids documentation](https://unfmontreal.github.io/Dcm2Bids/3.2.0/) for details.

### spec2bids Configuration

Place your spec2bids configuration at:
- `~/sourcedata/<dataset>-sourcedata/spec2bids/config.json` or
- `~/sourcedata/<dataset>-sourcedata/configs/spec2bids.json`

Example:
```json
{
  "manufacturer": "ge",
  "descriptions": [
    {
      "criteria": {
        "ProtocolName": "PRESS reference ACL"
      },
      "custom_entities": "voi-acl_acq-press"
    },
    {
      "criteria": {
        "ProtocolName": "GABA ACL"
      },
      "custom_entities": "voi-acl_acq-mega"
    }
  ]
}
```

## Requirements

### DICOM Import
- `dcm2bids` (install in virtual environment)
- `dcm2niix` (usually installed with dcm2bids)
- Optional: Singularity/Apptainer with `cbinyu/bids-pydeface:v2.0.6` for defacing

### MRS Import
- `spec2bids` (custom tool - see https://github.com/arovai/spec2bids)
- `spec2nii` (usually installed with spec2bids)

### Physio Import
- **In-house processing** (default) - No external dependencies
- `phys2bids` (optional - use with `--phys2bids` flag)
- Configuration file: `~/sourcedata/<dataset>-sourcedata/configs/physio.json` (or `physio/config.json`)
  - See `example_physio_config.json` in repository root

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

## Output

Imported data will be placed in:
```
~/rawdata/<dataset-name>-rawdata/
├── sub-001/
│   ├── anat/
│   │   ├── sub-001_T1w.nii.gz
│   │   ├── sub-001_T1w.json
│   │   └── ...
│   ├── func/
│   ├── dwi/
│   ├── mrs/
│   └── ...
├── sub-002/
└── ...
```

## Validation

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

For physio import, if no config file is found, default values are used (DummyVolumes=5).
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
