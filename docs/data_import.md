# Data Import Guide

ln2t_tools includes import utilities to convert source data to BIDS format:

- **DICOM**: Convert DICOM files to BIDS using dcm2bids with optional defacing
- **MRS**: Convert MRS data to BIDS using spec2nii
- **Physio**: Convert GE physiological monitoring data to BIDS using phys2bids
- **MEG**

Each dataype import will use a dedicated configuration file, located by default in the source data folder.

## Global Import Options

The following options apply to all import datatypes (DICOM, MRS, Physio, MEG):

### `--only-uncompressed`

Controls how the tool handles compressed source data archives:

```bash
# Default behavior: accepts both uncompressed folders and .tar.gz archives
ln2t_tools import --dataset mydataset --datatype dicom

# Only uncompressed: ignores any .tar.gz archives, only processes folders
ln2t_tools import --dataset mydataset --datatype dicom --only-uncompressed
```

**When to use `--only-uncompressed`**:
- You have both uncompressed folders AND compressed archives for the same data
- You want to avoid processing the compressed version (to prevent duplication)
- You're intentionally working with only the extracted, uncompressed source data
- You want to ensure the tool doesn't extract large archives unnecessarily

**Data source detection logic**:

| Scenario | Default Behavior | With `--only-uncompressed` |
|----------|-----------------|-------------------------|
| Only `CB001/` folder exists | ✓ Process | ✓ Process |
| Only `CB001.tar.gz` archive exists | ✓ Extract & process | ✗ Skip (not found error) |
| Both `CB001/` and `CB001.tar.gz` exist | ✓ Use existing folder | ✓ Use existing folder |
| Neither exists | ✗ Not found error | ✗ Not found error |

This option applies to:
- DICOM participant discovery and data loading
- MRS participant discovery and data loading
- Physio and pre-import DICOM metadata extraction
- MEG source file discovery

## DICOM Import

Convert DICOM files to BIDS-compliant NIfTI format with optional defacing:

```bash
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom
```

### Automatic Participant Discovery

If you don't specify `--participant-label`, the tool will automatically discover all available participants from your DICOM source directory by scanning for folders matching your dataset initials pattern:

```bash
# Discovers all participants matching {dataset_initials}* (e.g., CB001, CB042, HP007)
ln2t_tools import --dataset mydataset --datatype dicom
```

### Handling Compressed Archives

By default, the import tool handles both uncompressed folders and compressed tarball archives:

**Standard behavior (both folders and archives)**:
```bash
# Will process both: CB001/ (folder) and CB002.tar.gz (archive)
ln2t_tools import --dataset mydataset --datatype dicom
```

**With `--only-uncompressed` option**:
```bash
# Only processes uncompressed folders: CB001/ 
# Ignores any compressed archives like CB002.tar.gz
ln2t_tools import --dataset mydataset --datatype dicom --only-uncompressed
```

#### When to Use `--only-uncompressed`

Use this option when:
- You have both uncompressed folders AND compressed archives for the same data
- You want to avoid processing the compressed version (to prevent duplication)
- You're intentionally working with only the extracted, uncompressed source data
- You want to ensure the tool doesn't extract large archives unnecessarily

#### Data Source Detection Logic

| Scenario | Default Behavior | With `--only-uncompressed` |
|----------|-----------------|-------------------------|
| Only `CB001/` folder exists | ✓ Process | ✓ Process |
| Only `CB001.tar.gz` archive exists | ✓ Extract & process | ✗ Skip (not found error) |
| Both `CB001/` and `CB001.tar.gz` exist | ✓ Use existing folder | ✓ Use existing folder |
| Neither exists | ✗ Not found error | ✗ Not found error |

### Additional DICOM Import Options

```bash
# Deface anatomical images during import
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --deface

# Keep temporary dcm2bids files (normally deleted)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --keep-tmp-files

# Specify custom virtual environment for dcm2bids
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom \
  --import-env /path/to/venv

# Multi-session import
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --session 01
```



Convert Magnetic Resonance Spectroscopy data to BIDS format:

```bash
ln2t_tools import --dataset mydataset --participant-label 01 --datatype mrs
```

## DICOM Import

Convert DICOM files to BIDS-compliant NIfTI format with optional defacing:

```bash
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom
```

### Automatic Participant Discovery

If you don't specify `--participant-label`, the tool will automatically discover all available participants from your DICOM source directory by scanning for folders matching your dataset initials pattern:

```bash
# Discovers all participants matching {dataset_initials}* (e.g., CB001, CB042, HP007)
ln2t_tools import --dataset mydataset --datatype dicom
```

For more information on handling compressed vs. uncompressed data, see the `--only-uncompressed` option in [Global Import Options](#global-import-options).

### Additional DICOM Import Options

```bash
# Deface anatomical images during import
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --deface

# Keep temporary dcm2bids files (normally deleted)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --keep-tmp-files

# Specify custom virtual environment for dcm2bids
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom \
  --import-env /path/to/venv

# Multi-session import
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom --session 01
```

## MRS Import

Convert Magnetic Resonance Spectroscopy data to BIDS format:

```bash
ln2t_tools import --dataset mydataset --participant-label 01 --datatype mrs

# Or auto-discover all participants
ln2t_tools import --dataset mydataset --datatype mrs

# With --only-uncompressed option
ln2t_tools import --dataset mydataset --datatype mrs --only-uncompressed
```

For more information on handling compressed vs. uncompressed data, see the `--only-uncompressed` option in [Global Import Options](#global-import-options).

### MRS Pre-import

Gather P-files from scanner backup locations before running the main import:

```bash
ln2t_tools import --dataset mydataset --pre-import --datatype mrs

# With --only-uncompressed
ln2t_tools import --dataset mydataset --pre-import --datatype mrs --only-uncompressed
```





## Physio Import

Convert GE physiological monitoring data (respiratory, PPG) to BIDS format with automatic fMRI matching.

**By default**, uses in-house processing (simple and fast). Optionally use `--phys2bids` for phys2bids-based processing.

```bash
# Import physio data using in-house processing (default)
# Config file will be auto-detected from sourcedata/configs/physio.json
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio

# Or with phys2bids
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio --phys2bids

# With --only-uncompressed to skip compressed DICOM archives
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio --only-uncompressed
```

For more information on handling compressed vs. uncompressed data, see the `--only-uncompressed` option in [Global Import Options](#global-import-options).

### Physio Pre-import

Gather physio files from scanner backup location before running the main import:

```bash
ln2t_tools import --dataset mydataset --pre-import --datatype physio

# With --only-uncompressed
ln2t_tools import --dataset mydataset --pre-import --datatype physio --only-uncompressed
```

The pre-import step uses DICOM metadata to match physio files by timestamp, so the `--only-uncompressed` option affects DICOM archive handling during this step.

### Configuration File

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

### In-House Processing Details

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

### phys2bids Processing (Optional)

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

## Import MEG Data

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

### MEG Source Data Structure

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

### MEG Configuration File

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
    },
    {
      "pattern": "*noise*supine*.fif",
      "task": "noise_acq-supine",
      "run_extraction": "none",
      "description": "Empty-room noise acquisition in supine position"
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
- `task`: BIDS task label, optionally followed by extra entities using `_` separators (e.g., `noise_acq-supine`)
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

### Participants Mapping

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

### Session Auto-Detection

- **Single session**: If only one date folder exists, no session label is added
- **Multiple sessions**: Automatically numbered as `ses-01`, `ses-02`, etc. based on date folder order
- **Manual override**: Use `--session` flag to specify a particular session

### Calibration Files

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

### MaxFilter Derivatives

Automatically detected suffixes:
- `_sss`, `_tsss`: Signal Space Separation
- `_mc`: Movement compensation
- `_trans`, `_quat`: Head position transforms
- `_av`, `_ave`: Averaged data

Example: `rest_mc_ave.fif` → BIDS: `sub-01_task-rest_proc-mc-ave_meg.fif` in derivatives

### MEG Metadata Features

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

## Import All Datatypes

```bash
# Import all available datatypes for a participant
ln2t_tools import --dataset mydataset --participant-label 01 --datatype all

# With multiple options
ln2t_tools import --dataset mydataset --participant-label 01 --datatype all --deface --compress-source
```

## Directory Naming Convention

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
