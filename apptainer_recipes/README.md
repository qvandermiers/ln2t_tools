# Apptainer Recipe Files

This directory contains Apptainer/Singularity recipe files for building containers used by ln2t_tools.

## phys2bids.def

Recipe for building the phys2bids container for physiological data conversion to BIDS format.

### Manual Build

If you need to manually build the container:

```bash
cd /opt/apptainer
apptainer build phys2bids.phys2bids.latest.sif /path/to/ln2t_tools/apptainer_recipes/phys2bids.def
```

### Automatic Build

The container is automatically built when running physio import if not found:

```bash
ln2t_tools import --dataset mydataset --participant-label 001 --datatype physio
```

### Container Details

- **Base image**: Python 3.9-slim
- **phys2bids version**: 2.10.0
- **Python packages**: numpy<1.24, phys2bids
- **System dependencies**: git, build-essential

### Usage

```bash
# Show help
apptainer exec /opt/apptainer/phys2bids.phys2bids.latest.sif phys2bids --help

# Run phys2bids
apptainer exec \
  -B /data/input:/data/input \
  -B /data/output:/data/output \
  /opt/apptainer/phys2bids.phys2bids.latest.sif \
  phys2bids \
    -in /data/input/physio_file.txt \
    -indir /data/input \
    -outdir /data/output \
    -sub 001 \
    -heur /data/heur/heuristic.py \
    -tr 2.0 \
    -ntp 200
```

### Troubleshooting

**Build fails with "permission denied"**:
- Make sure you have write access to `/opt/apptainer/`
- Or specify a custom directory: `ln2t_tools import --apptainer-dir ~/apptainer ...`

**Python/numpy version conflicts**:
- The recipe uses Python 3.9 with numpy<1.24 to ensure compatibility
- This avoids the setuptools.build_meta errors seen with newer Python versions

**Container not found after build**:
- Check `/opt/apptainer/` for `phys2bids.phys2bids.latest.sif`
- Verify build completed without errors in the logs
