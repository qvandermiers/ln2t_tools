# Advanced Documentation

This directory contains detailed documentation for `ln2t_tools` developers and advanced users.

## Quick Navigation

| Document | Purpose | Audience | Time |
|----------|---------|----------|------|
| [Data Import Guide](data_import.md) | BIDS conversion for all data types | Lab administrators, data managers | 30-45 min |
| [Adding a New Tool](adding_new_tool.md) | Extend `ln2t_tools` with new pipelines | Tool developers, contributors | 45-60 min |
| [Instance Management](instance_management.md) | Control parallel processing limits | Administrators | 5-10 min |
| [MELD Graph Setup](meld_graph.md) | Advanced FCD lesion detection workflow | Researchers using MELD | 20-30 min |

## Documentation by Topic

### Data Management

#### [Data Import Guide](data_import.md)
Comprehensive guide to converting raw neuroimaging data to BIDS format. Covers:
- **DICOM**: Conversion with dcm2bids and optional defacing
- **MRS**: Magnetic Resonance Spectroscopy conversion with spec2nii
- **Physiological Data**: GE monitor data processing with phys2bids
- **MEG**: Complete MEG workflow including MaxFilter derivatives, calibration files, and head shape extraction
- Configuration file setup for each datatype
- Directory structure and naming conventions
- Participants mapping and session handling

**Use when**: Setting up data import pipelines for your lab

### Tool Development

#### [Adding a New Tool](adding_new_tool.md)
Step-by-step guide to developing and integrating new neuroimaging tools into `ln2t_tools`. Covers:
- Tool architecture and BaseTool interface
- CLI argument handling and validation
- Container image registration and configuration
- Creating custom Apptainer container recipes
- Best practices and advanced features
- HPC script generation and custom processing workflows

**Use when**: You want to add a new pipeline to `ln2t_tools`

### Administration

#### [Instance Management](instance_management.md)
Understanding and controlling parallel job limits. Learn about:
- Default limits and configuration
- Lock file system and metadata
- Automatic cleanup mechanisms
- Troubleshooting resource issues

**Use when**: Managing multiple concurrent processing jobs or debugging resource conflicts

#### [MELD Graph Setup](meld_graph.md)
Advanced configuration for MELD Graph lesion detection. Covers:
- Three-step workflow: Download weights → Harmonization → Prediction
- Scanner-specific harmonization
- FreeSurfer integration and precomputed outputs
- Feature extraction and reuse
- Demographics configuration
- Complete example workflows

**Use when**: Running MELD Graph for focal cortical dysplasia detection

## Getting Help

- **Main README**: Start with [../README.md](../README.md) for general usage
- **Specific tools**: Run `ln2t_tools <tool> --help` for tool-specific options
- **Issues**: Check the project repository for known issues and support
