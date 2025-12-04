# Apptainer Recipe Files

This directory contains Apptainer/Singularity recipe files for building containers used by ln2t_tools.

## fastsurfer.def

Recipe for building the FastSurfer container for deep-learning based brain segmentation and surface reconstruction.

### Manual Build

```bash
cd /opt/apptainer
apptainer build fastsurfer_2.4.2.sif /path/to/ln2t_tools/apptainer_recipes/fastsurfer.def
```

### Container Details

- **Base image**: deepmi/fastsurfer:cuda-v2.4.2
- **FastSurfer version**: 2.4.2
- **GPU support**: CUDA (NVIDIA)
- **Requirements**: FreeSurfer license, 8GB+ RAM, 8GB+ GPU memory recommended

### Usage

```bash
# Full pipeline (segmentation + surface reconstruction)
apptainer exec --nv \
  -B /path/to/license.txt:/fs_license/license.txt:ro \
  -B /path/to/rawdata:/data:ro \
  -B /path/to/derivatives:/output \
  /opt/apptainer/fastsurfer_2.4.2.sif \
  /fastsurfer/run_fastsurfer.sh \
    --sid sub-001 \
    --sd /output/fastsurfer_2.4.2 \
    --t1 /data/sub-001/anat/sub-001_T1w.nii.gz \
    --fs_license /fs_license/license.txt

# Segmentation only (~5 min on GPU)
apptainer exec --nv \
  -B /path/to/license.txt:/fs_license/license.txt:ro \
  -B /path/to/rawdata:/data:ro \
  -B /path/to/derivatives:/output \
  /opt/apptainer/fastsurfer_2.4.2.sif \
  /fastsurfer/run_fastsurfer.sh \
    --sid sub-001 \
    --sd /output/fastsurfer_2.4.2 \
    --t1 /data/sub-001/anat/sub-001_T1w.nii.gz \
    --fs_license /fs_license/license.txt \
    --seg_only

# CPU-only processing (remove --nv, add --device cpu)
apptainer exec \
  -B /path/to/license.txt:/fs_license/license.txt:ro \
  -B /path/to/rawdata:/data:ro \
  -B /path/to/derivatives:/output \
  /opt/apptainer/fastsurfer_2.4.2.sif \
  /fastsurfer/run_fastsurfer.sh \
    --sid sub-001 \
    --sd /output/fastsurfer_2.4.2 \
    --t1 /data/sub-001/anat/sub-001_T1w.nii.gz \
    --fs_license /fs_license/license.txt \
    --device cpu
```

### Via ln2t_tools

```bash
# Full pipeline
ln2t_tools fastsurfer --dataset mydataset --participant-label 001

# Segmentation only
ln2t_tools fastsurfer --dataset mydataset --participant-label 001 --seg-only

# With 3T atlas
ln2t_tools fastsurfer --dataset mydataset --participant-label 001 --3T
```

---

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
