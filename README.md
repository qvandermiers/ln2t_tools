<div align="center">

# ln2t_tools

**Neuroimaging pipeline manager for the [LN2T](https://ln2t.ulb.be/)**

[How it works](#how-it-works) | [Installation](#intallation) | [How to use it](#how-to-use-it) | [Supported tools](#supported-tools) | [Using HPC](#using-hpc) | [Data organization](#data-organization) | [Bonuses](#bonuses)

</div>

## How it works

`ln2t_tools` is a Command Line Interface software that facilitates execution of standard neuroimaging pipelines. The core principles are:
- Data are supposed to be organized following the [data organization](#data-organization) of the lab, which itself greatly relies on the [Brain Imaging Data Structure (BIDS)](#https://bids-specification.readthedocs.io/en/stable/).
- A [selection of standard pipelines](#supported-tools) have been incorporated in `ln2t_tools` in the form of apptainer images. The installation of the software, for any valid version, is fully automated.
- Outputs are tagged with pipeline name and version number.
- The syntax of `ln2t_tools` is as follows:
  ```bash
  ln2t_tools <pipeline_name> --dataset <dataset_name> [options]
  ```
  There are two classes of options: those belonging to `ln2t_tools` and those that are directly passed to the pipeline; see below for more details and examples.
- By default, the processing is done on the local machine, but `ln2t_tools` can be used to send the work to High-Performance Computing (HPC) Clusters such as [CECI](#https://www.ceci-hpc.be/); more info in the [corresponding section](#using-hpc).
- More pipelines can be added easily thanks to the modular architecture of `ln2t_tools`, but this is not meant to be done by the standard end-user.

If you are interested in using this tool in your lab, make sure to read this documentation in full, in particular the sections describing the [data organization](#data-organization).

## Installation

If you are working on a computer of the lab, the software is already installed and you can skip this section.

To install `ln2t_tools`, we strongly recommend you use a python virtual environment:
```bash
git clone git@github.com:ln2t/ln2t_tools.git
cd ln2t_tools
python -m venv venv
source venv/bin/activate
pip install -U pip
pip install -U .
```

After installation, you can enable bash completion tools using
```bash
echo "source ~/.local/share/bash-completion/completions/ln2t_tools" >> ~/.bashrc
```
and then source `~/.bashrc`. More details in the [How it works](#how-it-works) section.

`ln2t_tools` assumes a standardized folder structure to access raw data and to save outputs; see the [corresponding section](#data-organization) for more details.

Moreover, ln2t_tools uses [Apptainer](https://apptainer.org/) containers to run neuroimaging pipelines. This ensures reproducibility and eliminates dependency conflicts.

**Installation**:
- Apptainer must be installed system-wide
- Container images are stored in `/opt/apptainer` by default

**Permissions**:
The `/opt/apptainer` directory requires write access for pulling and caching container images:
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

Finally, several pipelines requires a valid license file (free academic license available at [FreeSurfer Registration](https://surfer.nmr.mgh.harvard.edu/registration.html)).

**Default License Location**:
```bash
~/licenses/license.txt
```

To use a custom license location:
```bash
ln2t_tools freesurfer --dataset mydataset --fs-license /path/to/license.txt
```

## How to use it

Open a terminal, start typing `ln2t_tools` and try the auto-completion mechanism using the `<TAB>` key: it will show you the available tools and guide you to complete the command (including dataset name discovery!).

For instance, typing
```bash
ln2t_tools <TAB>  # show you the available tools
```
will show you the [supported tools](#supported-tools), e.g. `freesurfer` or `fmriprep`. Once the pipeline is completed, you can just continue using `<TAB>` to see what must be provided:
```bash
ln2t_tools freesurfer <TAB>  # auto-complete the next argument, in this case '--dataset'
```
This will auto-complete the mandatory argument `--dataset`. Further `<TAB>` presses will show you the datasets you have access to:
```bash
ln2t_tools freesurfer --dataset <TAB>  # show you the available datasets
```
Dataset names in the lab have the structure
```bash
YYYY-Custom_Name-abc123
```
The `YYYY` corresponds to the year of dataset creation; `Custom_Name` is an easy to remember, human-readable name (typically an adjective and an animal) and the end of the name, `abc123`, is a randomly generated sequence of characters. Againg, start typing then use `<TAB>` to auto-complete.

**Example: `FreeSurfer`**

To run `FreeSurfer` on the dataset `2025-Charming_Nightingale-9f9014dbdfae`, you can use
```bash
ln2t_tools freesurfer --dataset 2025-Charming_Nightingale-9f9014dbdfae
```
Pressing enter will:
- select the default version for `FreeSurfer`
- check that it is installed - if not, download and install it
- check that the dataset exists and discover the available subjects
- for each subject, check if `FreeSurfer` has already been run
- launch sequentially the remaining subjects
To understand where to find the outputs, make sure to read to [Data organisation](#data-organization) section.

**Important Notice:**

We **do not** recommend that you launch a tool on a whole dataset at once. Start first on a few subjects - for this, you can use the `ln2t_tools` option `--participant-label`:
```bash
ln2t_tools freesurfer --dataset 2025-Charming_Nightingale-9f9014dbdfae \
            --participant-label 001 042
```
If the processing is successful and corresponds to your needs, then you may consider launching a full dataset run by omitting the `--participant-label` option.

## Supported tools

Here is the list of currently supported tools available to the lab members - for each tool, we show also the typesetting to use when using `ln2t_tools`:

- **FreeSurfer** - `freesurfer`: Cortical reconstruction and surface-based analysis ([official docs](https://surfer.nmr.mgh.harvard.edu/))
- **fMRIPrep** - `fmriprep`: Functional MRI preprocessing ([official docs](https://fmriprep.org/))
- **QSIPrep** - `qsiprep`: Diffusion MRI preprocessing ([official docs](https://qsiprep.readthedocs.io/))
- **QSIRecon** - `qsirecon`: Diffusion MRI reconstruction ([official docs](https://qsiprep.readthedocs.io/))
- **MELD Graph** - `meld_graph`: Lesion detection ([official docs](https://meld-graph.readthedocs.io/))

Note that there is another tool, `ln2t_tools import`, designed to deal with the BIDS-ification of source data. This tool is for administrators only (if you try it it will fail).

## Using HPC

The main neuroimaging tools in `ln2t_tools` like `FreeSurfer`, `fMRIPrep`, `QSIPrep` and `QSIRecon` can be submitted to HPC clusters using SLURM (Simple Linux Utility for Resource Management) using `--hpc`. A typical command look like
```bash
ln2t_tools <pipeline> --dataset <dataset> --hpc --hpc-host <host> --hpc-user <user> --hpc-time 10:00:00
```
`ln2t_tools` will then search for pre-installed SSH keys to interact with the cluster. Data, apptainer images and code will by default be assumed to be located on `$GLOBALSCRATCH`, which is typically an environment variable defined on the cluster. Logs of `ln2t_tools` are in your cluster home folder.

## Data organization

`ln2t_tools` essentially follows the [BIDS (Brain Imaging Data Structure) specification](https://bids-specification.readthedocs.io/) for organizing neuroimaging data, and what follow is essential for the tool to work:

- **Raw data**: `~/rawdata/{dataset}-rawdata` | This is where your original data are stored. You should have read-only permissions for safety.
- **Derivatives**: `~/derivatives/{dataset}-derivatives` | This is where `ln2t_tools` will write (and in some cases, read) outputs of the selected pipeline. For instance, for `FreeSurfer` version `7.2.0`, you will find the results in `~/derivatives/{dataset}-derivatives/freesurfer_7.2.0/sub-*`.
- **Code**: `~/code/{dataset}-code` | The golden spot to put your custom code and configurations. We recommend that you keep there a `README.md` file with copies of the command lines you use - this can be very useful to keep track of your work or to re-run analyzes. Moreover, this folder is made available to the tools that require a configuration file (such a `qsirecon`).

A part from that, there is also a similar structure for the source data (data as exported from the scanner or any other recording device), but these are not generally made available to the users - all you need should be in the raw data.

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

## Bonuses

### Tool-Specific Arguments with --tool-args

`ln2t_tools` uses a pass-through argument pattern for tool-specific options. This allows the tools to be updated independently of `ln2t_tools`, and gives you access to the full range of options each tool supports.

Core arguments (dataset, participant, version, HPC options) are handled by `ln2t_tools`. Tool-specific arguments are passed verbatim to the container using `--tool-args`:

```bash
ln2t_tools <tool> --dataset mydataset --participant-label 01 --tool-args "<tool-specific-arguments>"
```
For instance, here what you shoud do to use `qsiprep` with the (mandatory) argument `--output-resolution`:
```bash
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
    --tool-args "--output-resolution 1.5"
```

### Finding Missing Participants

The `--list-missing` flag helps identify which participants in your dataset still need processing for a specific tool. This is useful when:
- Resuming incomplete pipelines after errors
- Managing large cohorts with multiple tools
- Generating copy-paste commands to process missing participants

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

## Instance Management

ln2t_tools includes built-in safeguards to prevent resource overload:

- **Default limit**: Maximum 10 parallel instances
- **Lock files**: Stored in `/tmp/ln2t_tools_locks/` with detailed JSON metadata
- **Automatic cleanup**: Removes stale lock files from terminated processes
- **Graceful handling**: Shows helpful messages when limits are reached

Each instance creates a lock file with:
- Process ID (PID)
- Dataset name(s)
- Tool(s) being run
- Participant labels
- Hostname
- Username
- Start time

## Data Import

ln2t_tools includes import utilities to convert source data to BIDS format:

- **DICOM**: Convert DICOM files to BIDS using dcm2bids with optional defacing
- **MRS**: Convert MRS data to BIDS using spec2nii
- **Physio**: Convert GE physiological monitoring data to BIDS using phys2bids
- **MEG**

Each dataype import will use a dedicated configuration file, located by default in the source data folder.

### DICOM Import

Convert DICOM files to BIDS-compliant NIfTI format with optional defacing:

```bash
ln2t_tools import --dataset mydataset --participant-label 01 --datatype dicom
```

### MRS Import

Convert Magnetic Resonance Spectroscopy data to BIDS format:

```bash
ln2t_tools import --dataset mydataset --participant-label 01 --datatype mrs
```

### Physio Import

Convert GE physiological monitoring data (respiratory, PPG) to BIDS format with automatic fMRI matching.

**By default**, uses in-house processing (simple and fast). Optionally use `--phys2bids` for phys2bids-based processing.

```bash
# Import physio data using in-house processing (default)
# Config file will be auto-detected from sourcedata/configs/physio.json
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio
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
