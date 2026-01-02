# Physiological Data Processing Changes

## Breaking Changes - DummyVolumes Configuration Refactoring

### ⚠️ Configuration File Format Changed

**Date**: January 2, 2026

The physio configuration file format has been refactored to use **task-specific DummyVolumes only** (no global default).

#### Old Format (DEPRECATED):
```json
{
  "DummyVolumes": 5,
  "DummyVolumesPerTask": {
    "task-rest": 5,
    "task-motor_run-01": 3
  }
}
```

#### New Format (REQUIRED):
```json
{
  "DummyVolumes": {
    "task-rest": 5,
    "task-motor_run-01": 3,
    "task-motor_run-02": 4
  }
}
```

#### Migration Guide

1. **Remove** the global `"DummyVolumes": <integer>` field
2. **Rename** `"DummyVolumesPerTask"` to `"DummyVolumes"`
3. **Ensure** all tasks/runs in your dataset have entries
4. **Update** all physio config files before running import

#### Behavior Changes

- **Before**: Missing task in config would use default (DummyVolumes=5)
- **After**: Missing task in config will cause import to fail with clear error message
- **Rationale**: Explicit configuration prevents silent errors from missing task definitions

#### Error Messages

If a task is not found in the new config format:
```
KeyError: Task 'motor' (with run='02') not found in DummyVolumes config. 
Available: task-motor_run-01, task-rest
```

---

## Summary

Modified the physiological data import functionality to use **in-house processing by default** instead of phys2bids. The phys2bids option is still available via the `--phys2bids` flag.

## Changes Made

### 1. New Command-Line Arguments (`cli/cli.py`)

Added two new arguments:

```python
--phys2bids              # Force use of phys2bids (default: False, use in-house)
--physio-config PATH     # Path to JSON configuration file
```

### 2. New In-House Processing Module (`import_data/physio_inhouse.py`)

Created a new module that handles physiological data processing without phys2bids:

**Key Features**:
- Processes single-column physio data files (no time column added)
- Supports RESP (25 Hz) and PPG (100 Hz) signals
- Calculates StartTime based on configuration
- Creates BIDS-compliant TSV.GZ and JSON files
- Automatic matching to fMRI runs based on timestamps

**StartTime Calculation**:
```
StartTime = -(30s + (TR × DummyVolumes))
```

Where:
- 30s = GE scanner pre-recording period (hardcoded)
- TR = From fMRI JSON metadata
- DummyVolumes = From configuration file (task-specific)
- Negative sign indicates recording started BEFORE the first trigger

### 3. Updated Main Physio Module (`import_data/physio.py`)

Modified to route between in-house and phys2bids processing:

```python
def import_physio(..., use_phys2bids=False, physio_config=None):
    if use_phys2bids:
        return import_physio_phys2bids(...)
    else:
        return import_physio_inhouse(..., config=config)
```

### 4. Configuration File

Created `example_physio_config.json` template.

**Config file location** (auto-detected in this order):
1. Explicit path via `--physio-config` argument
2. `sourcedata/{dataset}-sourcedata/configs/physio.json` (preferred)
3. `sourcedata/{dataset}-sourcedata/physio/config.json` (legacy)
4. Default values (DummyVolumes=5)

**Format**:
```json
{
  "DummyVolumes": 5,
  "_comment": "Configuration for in-house physiological data processing",
  "_description": {
    "DummyVolumes": "Number of dummy volumes at start of fMRI acquisition"
  }
}
```

### 5. Updated Main Tool (`ln2t_tools.py`)

Added parameters to pass new arguments to import function:

```python
import_success['physio'] = import_physio(
    ...,
    use_phys2bids=getattr(args, 'phys2bids', False),
    physio_config=getattr(args, 'physio_config', None),
    ...
)
```

### 6. Updated Documentation (`README.md`)

Updated physio import section with:
- In-house processing as default
- Configuration file explanation
- StartTime calculation formula
- Sampling frequency specifications (RESP: 100Hz, PPG: 25Hz)
- Option to use phys2bids with `--phys2bids` flag

## Usage Examples

### In-House Processing (Default)

```bash
# Basic usage (auto-detects config from sourcedata/configs/physio.json)
ln2t_tools import --dataset mydataset --participant-label 01 \
  --datatype physio

# With explicit config file
ln2t_tools import --dataset mydataset --participant-label 01 \
  --datatype physio --physio-config /path/to/custom_config.json

# With session
ln2t_tools import --dataset mydataset --participant-label 01 \
  --datatype physio --session 01

# With compression
ln2t_tools import --dataset mydataset --participant-label 01 \
  --datatype physio --compress-source
```

### Using phys2bids (Optional)

```bash
ln2t_tools import --dataset mydataset --participant-label 01 \
  --datatype physio --phys2bids
```

## Technical Details

### Input Files

- **Format**: Single column of numerical values
- **Naming**: `{SIGNAL}Data_{SEQUENCE}_{TIMESTAMP}`
- **Signals**: RESP (respiratory) or PPG (cardiac)
- **Trigger files**: Automatically ignored (files ending in `Trig`)

### Output Files

For each matched physio-fMRI pair:

**TSV.GZ File** (`*_physio.tsv.gz`):
- Single column of physio values
- Gzip compressed
- No headers, no time column

**JSON Sidecar** (`*_physio.json`):
```json
{
  "SamplingFrequency": 25.0,
  "StartTime": -40.0,
  "Columns": ["respiratory"]
}
```

### Matching Algorithm

1. Parse physio filenames for signal type and timestamp
2. Read fMRI metadata (AcquisitionTime, TR, volumes)
3. Calculate fMRI end time
4. Match physio to fMRI based on timestamp (35s tolerance)
5. Extract task and run from fMRI filename
6. Process matched files

### BIDS Compliance

- Recording type mapping: RESP → "respiratory", PPG → "cardiac"
- Follows BIDS specification for physiological recordings
- Compatible with downstream analysis tools expecting BIDS format

## Migration Notes

### For Existing Users

If you were using phys2bids before, add `--phys2bids` flag to maintain previous behavior:

```bash
# Old command (implicit phys2bids)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio

# New equivalent (explicit phys2bids)
ln2t_tools import --dataset mydataset --participant-label 01 --datatype physio --phys2bids
```

### For New Users

Place the config file in your sourcedata directory:

```bash
# Create configs directory
mkdir -p ~/sourcedata/mydataset-sourcedata/configs

# Create config file
cat > ~/sourcedata/mydataset-sourcedata/configs/physio.json << EOF
{
  "DummyVolumes": 5
}
EOF

# Run import (config will be auto-detected)
ln2t_tools import --dataset mydataset --participant-label 01 \
  --datatype physio
```

## Benefits of In-House Processing

1. **Simpler**: No external dependencies (phys2bids, Apptainer)
2. **Faster**: Direct processing without container overhead
3. **Transparent**: Clear calculation of StartTime from configuration
4. **Flexible**: Easy to modify for different scanner parameters
5. **BIDS-compliant**: Produces valid BIDS physiological recordings

## Files Modified

- `ln2t_tools/cli/cli.py` - Added new arguments
- `ln2t_tools/import_data/physio.py` - Router function
- `ln2t_tools/import_data/physio_inhouse.py` - New in-house processor
- `ln2t_tools/ln2t_tools.py` - Pass new arguments
- `README.md` - Updated documentation
- `example_physio_config.json` - Configuration template

## Testing Checklist

- [ ] In-house processing with valid config file
- [ ] In-house processing without config file (should use DummyVolumes=0)
- [ ] phys2bids processing with `--phys2bids` flag
- [ ] Matching algorithm with multiple physio files
- [ ] Multi-session datasets
- [ ] Compression of source data
- [ ] Verify JSON sidecar content
- [ ] Verify TSV.GZ file format (single column, no headers)
- [ ] Verify StartTime calculation with different TR and DummyVolumes values
