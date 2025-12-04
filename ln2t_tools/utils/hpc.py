"""HPC cluster job submission utilities.

Supports SLURM-based HPC clusters with configurable connection settings.
"""

import logging
import subprocess
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import tempfile

logger = logging.getLogger(__name__)


def get_ssh_command(username: str, hostname: str, keyfile: str, gateway: Optional[str] = None) -> list:
    """Get SSH command with proper key configuration and optional ProxyJump.
    
    Parameters
    ----------
    username : str
        Username for HPC cluster
    hostname : str
        Hostname for HPC cluster
    keyfile : str
        Path to SSH private key file
    gateway : Optional[str]
        ProxyJump gateway hostname (e.g., 'gwceci.ulb.ac.be')
        
    Returns
    -------
    list
        SSH command with options
    """
    cmd = [
        "ssh",
        "-i", str(Path(keyfile).expanduser()),
        "-o", "ConnectTimeout=10",
    ]
    
    if gateway:
        cmd.extend(["-J", f"{username}@{gateway}"])
    
    cmd.append(f"{username}@{hostname}")
    
    return cmd


def check_apptainer_image_exists_on_hpc(
    username: str,
    hostname: str,
    keyfile: str,
    gateway: Optional[str],
    hpc_apptainer_dir: str,
    tool: str,
    version: str
) -> bool:
    """Check whether the Apptainer image for a tool/version exists on the remote HPC.

    This performs a remote `test -e` via SSH. Returns True if the file exists.
    """
    # Map tool -> docker owner (should mirror ensure_image_exists() mapping)
    if tool == "freesurfer":
        tool_owner = "freesurfer"
    elif tool == "fastsurfer":
        tool_owner = "deepmi"
    elif tool == "fmriprep":
        tool_owner = "nipreps"
    elif tool == "qsiprep":
        tool_owner = "pennlinc"
    elif tool == "qsirecon":
        tool_owner = "pennlinc"
    elif tool == "meld_graph":
        tool_owner = "meldproject"
    else:
        logger.error(f"Unknown tool when checking HPC image: {tool}")
        return False

    # Image filename uses the version/token as provided (allow full tags like 'cuda-v2.4.2')
    image_name = f"{tool_owner}.{tool}.{version}.sif"
    remote_path = Path(hpc_apptainer_dir) / image_name

    ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"test -e {remote_path}"]

    try:
        logger.info(f"Checking for Apptainer image on HPC: {username}@{hostname}:{remote_path}")
        result = subprocess.run(ssh_cmd, capture_output=True)
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Error checking Apptainer image on HPC: {e}")
        return False


def get_hpc_image_build_command(
    username: str,
    hostname: str,
    keyfile: str,
    gateway: Optional[str],
    hpc_apptainer_dir: str,
    tool: str,
    version: str
) -> str:
    """Generate the SSH command to build an Apptainer image on the HPC.
    
    Returns a ready-to-copy-paste command string.
    """
    # Map tool -> docker owner
    tool_owners = {
        "freesurfer": "freesurfer",
        "fastsurfer": "deepmi",
        "fmriprep": "nipreps",
        "qsiprep": "pennlinc",
        "qsirecon": "pennlinc",
        "meld_graph": "meldproject",
    }
    tool_owner = tool_owners.get(tool, tool)
    
    image_name = f"{tool_owner}.{tool}.{version}.sif"
    remote_path = f"{hpc_apptainer_dir}/{image_name}"
    docker_uri = f"docker://{tool_owner}/{tool}:{version}"
    
    # Build the SSH command
    ssh_opts = f"-i {keyfile}"
    if gateway:
        ssh_opts += f" -J {username}@{gateway}"
    
    build_cmd = f"apptainer build {remote_path} {docker_uri}"
    
    return f"ssh {ssh_opts} {username}@{hostname} '{build_cmd}'"


def get_scp_command(username: str, hostname: str, keyfile: str, gateway: Optional[str] = None) -> list:
    """Get SCP command with proper key configuration and optional ProxyJump.
    
    Parameters
    ----------
    username : str
        Username for HPC cluster
    hostname : str
        Hostname for HPC cluster
    keyfile : str
        Path to SSH private key file
    gateway : Optional[str]
        ProxyJump gateway hostname (e.g., 'gwceci.ulb.ac.be')
        
    Returns
    -------
    list
        SCP command with options
    """
    cmd = [
        "scp",
        "-i", str(Path(keyfile).expanduser()),
    ]
    
    if gateway:
        cmd.extend(["-o", f"ProxyJump={username}@{gateway}"])
    
    return cmd


def validate_hpc_config(args) -> None:
    """Validate HPC configuration arguments."""
    if args.hpc:
        required_args = {
            '--hpc-username': args.hpc_username,
            '--hpc-hostname': args.hpc_hostname,
            '--hpc-keyfile': args.hpc_keyfile,
            '--hpc-apptainer-dir': args.hpc_apptainer_dir,
        }
        
        missing = [arg for arg, value in required_args.items() if not value]
        if missing:
            raise ValueError(
                f"When using --hpc, you must provide: {', '.join(missing)}\n"
                f"Example: --hpc-username arovai --hpc-hostname lyra.ulb.ac.be "
                f"--hpc-keyfile ~/.ssh/id_rsa --hpc-apptainer-dir /path/on/hpc/apptainer\n"
                f"Note: --hpc-rawdata and --hpc-derivatives are optional "
                f"(default: $GLOBALSCRATCH/rawdata and $GLOBALSCRATCH/derivatives on cluster)"
            )


def test_ssh_connection(username: str, hostname: str, keyfile: str, gateway: Optional[str] = None) -> bool:
    """Test SSH connection to HPC.
    
    Parameters
    ----------
    username : str
        Username for HPC cluster
    hostname : str
        Hostname for HPC cluster
    keyfile : str
        Path to SSH private key file
    gateway : Optional[str]
        ProxyJump gateway hostname
        
    Returns
    -------
    bool
        True if connection successful, False otherwise
    """
    try:
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + ["echo", "connected"]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15
        )
        if result.returncode == 0 and "connected" in result.stdout:
            logger.info(f"✓ SSH connection to {username}@{hostname} successful")
            return True
        else:
            logger.error(f"SSH connection failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"SSH connection to {username}@{hostname} timed out")
        return False
    except Exception as e:
        logger.error(f"SSH connection error: {e}")
        return False


def check_remote_path_exists(username: str, hostname: str, keyfile: str, gateway: Optional[str], 
                             remote_path: str) -> bool:
    """Check if a path exists on the remote HPC cluster.
    
    Parameters
    ----------
    username : str
        Username for HPC cluster
    hostname : str
        Hostname for HPC cluster
    keyfile : str
        Path to SSH private key file
    gateway : Optional[str]
        ProxyJump gateway hostname
    remote_path : str
        Path to check on remote system
        
    Returns
    -------
    bool
        True if path exists, False otherwise
    """
    try:
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"test -e {remote_path} && echo 'exists' || echo 'not_found'"
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0 and "exists" in result.stdout
    except Exception as e:
        logger.error(f"Error checking remote path {remote_path}: {e}")
        return False


def prompt_upload_data(local_path: str, remote_path: str, username: str, hostname: str, 
                      keyfile: str, gateway: Optional[str]) -> bool:
    """Prompt user to upload data to HPC and perform upload if confirmed.
    
    Parameters
    ----------
    local_path : str
        Local path to upload from
    remote_path : str
        Remote path to upload to
    username : str
        HPC username
    hostname : str
        HPC hostname
    keyfile : str
        SSH keyfile
    gateway : Optional[str]
        ProxyJump gateway
        
    Returns
    -------
    bool
        True if upload successful or user declined, False if upload failed
    """
    print(f"\n⚠️  Required data not found on HPC: {remote_path}")
    print(f"   Local path: {local_path}")
    
    response = input("\nWould you like to upload the data to the HPC? [y/N]: ").strip().lower()
    
    if response != 'y':
        print("Upload declined. Job submission cancelled.")
        return False
    
    print(f"\nUploading {local_path} to {username}@{hostname}:{remote_path}...")
    
    try:
        # Create remote directory first
        parent_dir = str(Path(remote_path).parent)
        ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"mkdir -p {parent_dir}"]
        subprocess.run(ssh_cmd, check=True, capture_output=True)
        
        # Upload data using rsync for better performance
        if gateway:
            proxy_cmd = f"ssh -i {keyfile} -J {username}@{gateway}"
        else:
            proxy_cmd = f"ssh -i {keyfile}"
        
        rsync_cmd = [
            "rsync", "-avz", "--progress",
            "-e", proxy_cmd,
            f"{local_path}/",
            f"{username}@{hostname}:{remote_path}/"
        ]
        result = subprocess.run(rsync_cmd, check=True)
        
        print(f"✓ Successfully uploaded data to {remote_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to upload data: {e}")
        print(f"\n✗ Upload failed. Please upload manually or check permissions.")
        return False


def check_required_data(tool: str, dataset: str, participant_label: str, args: Any,
                       username: str, hostname: str, keyfile: str, gateway: Optional[str],
                       hpc_rawdata: str, hpc_derivatives: str) -> bool:
    """Check if required input data exists on HPC, prompt for upload if missing.
    
    Parameters
    ----------
    tool : str
        Tool name
    dataset : str
        Dataset name
    participant_label : str
        Participant label
    args : Any
        Arguments namespace
    username : str
        HPC username
    hostname : str
        HPC hostname
    keyfile : str
        SSH keyfile path
    gateway : Optional[str]
        ProxyJump gateway
    hpc_rawdata : str
        HPC rawdata path
    hpc_derivatives : str
        HPC derivatives path
        
    Returns
    -------
    bool
        True if all required data present or uploaded, False otherwise
    """
    # Expand paths (will be done on cluster, but use for local checking)
    if not hpc_rawdata:
        hpc_rawdata_check = "$GLOBALSCRATCH/rawdata"
    else:
        hpc_rawdata_check = hpc_rawdata
        
    if not hpc_derivatives:
        hpc_derivatives_check = "$GLOBALSCRATCH/derivatives"
    else:
        hpc_derivatives_check = hpc_derivatives
    
    # For environment variable paths, we need to resolve them via SSH
    if hpc_rawdata_check.startswith('$'):
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"echo {hpc_rawdata_check}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            hpc_rawdata_check = result.stdout.strip()
    
    if hpc_derivatives_check.startswith('$'):
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"echo {hpc_derivatives_check}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            hpc_derivatives_check = result.stdout.strip()
    
    # Check rawdata
    rawdata_path = f"{hpc_rawdata_check}/{dataset}-rawdata"
    logger.info(f"Checking for rawdata on HPC: {rawdata_path}")
    
    if not check_remote_path_exists(username, hostname, keyfile, gateway, rawdata_path):
        local_rawdata = Path.home() / "rawdata" / f"{dataset}-rawdata"
        if local_rawdata.exists():
            if not prompt_upload_data(str(local_rawdata), rawdata_path, username, hostname, keyfile, gateway):
                return False
        else:
            print(f"\n✗ Rawdata not found locally at {local_rawdata}")
            print(f"   Cannot proceed without rawdata on HPC")
            return False
    else:
        logger.info(f"✓ Rawdata found on HPC: {rawdata_path}")
    
    # Check for precomputed FreeSurfer if required
    if tool in ['fmriprep', 'meld_graph'] and getattr(args, 'use_precomputed_fs', False):
        fs_version = getattr(args, 'fs_version', '7.2.0')
        fs_path = f"{hpc_derivatives_check}/{dataset}-derivatives/freesurfer_{fs_version}"
        
        logger.info(f"Checking for FreeSurfer outputs on HPC: {fs_path}")
        
        if not check_remote_path_exists(username, hostname, keyfile, gateway, fs_path):
            local_fs = Path.home() / "derivatives" / f"{dataset}-derivatives" / f"freesurfer_{fs_version}"
            if local_fs.exists():
                if not prompt_upload_data(str(local_fs), fs_path, username, hostname, keyfile, gateway):
                    return False
            else:
                print(f"\n✗ FreeSurfer outputs not found locally at {local_fs}")
                print(f"   Cannot use --use-precomputed-fs without FreeSurfer outputs on HPC")
                return False
        else:
            logger.info(f"✓ FreeSurfer outputs found on HPC: {fs_path}")
    
    # Check for QSIPrep outputs if running QSIRecon
    if tool == 'qsirecon':
        qsiprep_version = getattr(args, 'qsiprep_version', '1.0.1')
        qsiprep_path = f"{hpc_derivatives_check}/{dataset}-derivatives/qsiprep_{qsiprep_version}"
        
        logger.info(f"Checking for QSIPrep outputs on HPC: {qsiprep_path}")
        
        if not check_remote_path_exists(username, hostname, keyfile, gateway, qsiprep_path):
            local_qsiprep = Path.home() / "derivatives" / f"{dataset}-derivatives" / f"qsiprep_{qsiprep_version}"
            if local_qsiprep.exists():
                if not prompt_upload_data(str(local_qsiprep), qsiprep_path, username, hostname, keyfile, gateway):
                    return False
            else:
                print(f"\n✗ QSIPrep outputs not found locally at {local_qsiprep}")
                print(f"   Cannot run QSIRecon without QSIPrep outputs on HPC")
                return False
        else:
            logger.info(f"✓ QSIPrep outputs found on HPC: {qsiprep_path}")
    
    return True


def generate_hpc_script(
    tool: str,
    participant_label: str,
    dataset: str,
    args: Any,
    hpc_rawdata: str,
    hpc_derivatives: str,
    hpc_apptainer_dir: str
) -> str:
    """Generate HPC batch script for job submission.
    
    Parameters
    ----------
    tool : str
        Tool name (e.g., 'meld_graph', 'freesurfer', 'fmriprep')
    participant_label : str
        Subject/participant label
    dataset : str
        Dataset name
    args : argparse.Namespace
        Parsed command line arguments
    hpc_rawdata : str
        Path to rawdata on HPC (can be None to use $GLOBALSCRATCH/rawdata)
    hpc_derivatives : str
        Path to derivatives on HPC (can be None to use $GLOBALSCRATCH/derivatives)
    hpc_apptainer_dir : str
        Path to apptainer images on HPC
        
    Returns
    -------
    str
        HPC batch script content
    """
    # Determine job name
    if tool == "meld_graph" and getattr(args, 'harmonize', False):
        job_name = f"meld_harmo-{dataset}-{participant_label}"
    else:
        job_name = f"{tool}-{dataset}-{participant_label}"
    
    # Use $GLOBALSCRATCH if paths not provided
    if not hpc_rawdata:
        hpc_rawdata = "$GLOBALSCRATCH/rawdata"
    if not hpc_derivatives:
        hpc_derivatives = "$GLOBALSCRATCH/derivatives"
    
    # Get partition and resource settings
    partition = getattr(args, 'hpc_partition', None)
    time_limit = getattr(args, 'hpc_time', '24:00:00')  # Default 24 hours for most tools
    memory = getattr(args, 'hpc_mem', '32G')
    cpus = getattr(args, 'hpc_cpus', 8)
    gpus = getattr(args, 'hpc_gpus', 1)
    
    # Start building script
    script = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --cpus-per-task={cpus}
"""
    
    if partition:
        script += f"#SBATCH --partition={partition}\n"
    
    script += f"""#SBATCH --time={time_limit}
#SBATCH --mem={memory}
#SBATCH --output={job_name}_%j.out
#SBATCH --error={job_name}_%j.err
"""
    
    # Add GPU request for GPU-capable tools
    if tool in ['meld_graph'] and not getattr(args, 'no_gpu', False):
        script += f"#SBATCH --gres=gpu:{gpus}\n"
    
    script += f"""
# Print job information
echo "Job started at: $(date)"
echo "Running on node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "CPUs: {cpus}"
echo "Memory: {memory}"

# Define data paths
HPC_RAWDATA="{hpc_rawdata}"
HPC_DERIVATIVES="{hpc_derivatives}"
DATASET="{dataset}"
PARTICIPANT="sub-{participant_label}"
"""
    
    # Tool-specific command generation
    if tool == "freesurfer":
        version = getattr(args, 'version', '7.3.2')
        fs_license = getattr(args, 'hpc_fs_license', '$HOME/licenses/license.txt')
        apptainer_img = f"{hpc_apptainer_dir}/freesurfer.freesurfer.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/freesurfer_{version}"
        
        script += f"""
# FreeSurfer setup
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
mkdir -p "$OUTPUT_DIR"

# Run FreeSurfer
apptainer exec \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/output" \\
    -B "$FS_LICENSE:/opt/freesurfer/license.txt:ro" \\
    --env SUBJECTS_DIR=/output \\
    {apptainer_img} \\
    recon-all -s "$PARTICIPANT" -i "/data/$PARTICIPANT/anat/"*"_T1w.nii.gz" -all
"""
    
    elif tool == "fmriprep":
        version = getattr(args, 'version', '25.1.4')
        fs_license = getattr(args, 'hpc_fs_license', '$HOME/licenses/license.txt')
        apptainer_img = f"{hpc_apptainer_dir}/nipreps.fmriprep.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/fmriprep_{version}"
        
        output_spaces = getattr(args, 'output_spaces', ['MNI152NLin2009cAsym:res-2'])
        if isinstance(output_spaces, list):
            output_spaces_str = ' '.join(output_spaces)
        else:
            output_spaces_str = output_spaces
        
        fs_reconall = "--fs-no-reconall" if getattr(args, 'fs_no_reconall', False) else ""
        
        script += f"""
# fMRIPrep setup
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
WORK_DIR="$OUTPUT_DIR/work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Run fMRIPrep
apptainer exec \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/work" \\
    -B "$FS_LICENSE:/opt/freesurfer/license.txt:ro" \\
    --env FS_LICENSE=/opt/freesurfer/license.txt \\
    --cleanenv \\
    {apptainer_img} \\
    /data /out participant \\
    --participant-label {participant_label} \\
    --output-spaces {output_spaces_str} \\
    -w /work \\
    --skip-bids-validation \\
    {fs_reconall}
"""
    
    elif tool == "qsiprep":
        version = getattr(args, 'version', '1.0.1')
        apptainer_img = f"{hpc_apptainer_dir}/pennlinc.qsiprep.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/qsiprep_{version}"
        output_resolution = getattr(args, 'output_resolution', None)
        denoise_method = getattr(args, 'denoise_method', 'dwidenoise')
        
        if not output_resolution:
            raise ValueError("--output-resolution is required for QSIPrep")
        
        dwi_only = "--dwi-only" if getattr(args, 'dwi_only', False) else ""
        anat_only = "--anat-only" if getattr(args, 'anat_only', False) else ""
        
        script += f"""
# QSIPrep setup
OUTPUT_DIR="{output_dir}"
WORK_DIR="$OUTPUT_DIR/work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Run QSIPrep
apptainer exec \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/work" \\
    --cleanenv \\
    {apptainer_img} \\
    /data /out participant \\
    --participant-label {participant_label} \\
    --output-resolution {output_resolution} \\
    --denoise-method {denoise_method} \\
    -w /work \\
    --skip-bids-validation \\
    {dwi_only} {anat_only}
"""
    
    elif tool == "qsirecon":
        version = getattr(args, 'version', '1.1.1')
        qsiprep_version = getattr(args, 'qsiprep_version', '1.0.1')
        apptainer_img = f"{hpc_apptainer_dir}/pennlinc.qsirecon.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/qsirecon_{version}"
        qsiprep_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/qsiprep_{qsiprep_version}"
        recon_spec = getattr(args, 'recon_spec', 'mrtrix_multishell_msmt_ACT-hsvs')
        
        script += f"""
# QSIRecon setup
OUTPUT_DIR="{output_dir}"
QSIPREP_DIR="{qsiprep_dir}"
WORK_DIR="$OUTPUT_DIR/work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Run QSIRecon
apptainer exec \\
    -B "$QSIPREP_DIR:/qsiprep:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/work" \\
    --cleanenv \\
    {apptainer_img} \\
    /qsiprep /out participant \\
    --participant-label {participant_label} \\
    --recon-spec {recon_spec} \\
    -w /work \\
    --skip-bids-validation
"""
    
    elif tool == "meld_graph":
        version = getattr(args, 'version', 'v2.2.3')
        fs_license = getattr(args, 'hpc_fs_license', '$HOME/licenses/license.txt')
        apptainer_img = f"{hpc_apptainer_dir}/meldproject.meld_graph.{version}.sif"
        
        # GPU settings
        if getattr(args, 'no_gpu', False):
            gpu_flag = ""
            env_vars = "--env CUDA_VISIBLE_DEVICES=''"
        else:
            gpu_flag = "--nv"
            gpu_mem = getattr(args, 'gpu_memory_limit', 128)
            env_vars = f"--env PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:{gpu_mem} --env CUDA_LAUNCH_BLOCKING=1"
        
        # FreeSurfer binding if using precomputed
        fs_subjects_dir_bind = ""
        if getattr(args, 'use_precomputed_fs', False):
            fs_version = getattr(args, 'fs_version', '7.2.0')
            fs_subjects_dir_bind = f"-B $HPC_DERIVATIVES/$DATASET-derivatives/freesurfer_{fs_version}:/data/output/fs_outputs"
        
        # Build Python command
        if getattr(args, 'harmonize', False):
            harmo_code = getattr(args, 'harmo_code', participant_label)
            python_cmd = f"python scripts/new_patient_pipeline/new_pt_pipeline.py -harmo_code {harmo_code} -ids /data/subjects_list.txt -demos /data/demographics_{harmo_code}.csv --harmo_only"
        else:
            python_cmd = f"python scripts/new_patient_pipeline/new_pt_pipeline.py -id sub-{participant_label}"
            if getattr(args, 'harmo_code', None):
                python_cmd += f" -harmo_code {args.harmo_code}"
            if getattr(args, 'skip_feature_extraction', False):
                python_cmd += " --skip_feature_extraction"
        
        script += f"""
# MELD Graph setup
MELD_VERSION="{version}"
MELD_DATA_DIR="$HPC_DERIVATIVES/$DATASET-derivatives/meld_graph_$MELD_VERSION/data"
mkdir -p "$MELD_DATA_DIR/input" "$MELD_DATA_DIR/output/predictions_reports"

# Create MELD config files
cat > "$MELD_DATA_DIR/input/meld_bids_config.json" << 'EOF'
{{
  "T1": {{"session": null, "datatype": "anat", "suffix": "T1w"}},
  "FLAIR": {{"session": null, "datatype": "anat", "suffix": "FLAIR"}}
}}
EOF

cat > "$MELD_DATA_DIR/input/dataset_description.json" << 'EOF'
{{"Name": "{dataset}", "BIDSVersion": "1.6.0"}}
EOF

# Link rawdata
for subj_dir in $HPC_RAWDATA/$DATASET-rawdata/sub-*; do
    if [ -d "$subj_dir" ]; then
        subj=$(basename $subj_dir)
        ln -sf "$subj_dir" "$MELD_DATA_DIR/input/$subj"
    fi
done

# Run MELD
apptainer exec {gpu_flag} \\
    -B "$MELD_DATA_DIR:/data" \\
    -B "{fs_license}:/license.txt:ro" \\
    --env FS_LICENSE=/license.txt \\
    {env_vars} \\
    {fs_subjects_dir_bind} \\
    {apptainer_img} \\
    /bin/bash -c 'cd /app && {python_cmd}'
"""
    
    else:
        raise NotImplementedError(f"HPC submission for {tool} not yet implemented")
    
    script += """
echo "Job finished at: $(date)"
"""
    
    return script


def submit_hpc_job(
    tool: str,
    participant_label: str,
    dataset: str,
    args: Any
) -> Optional[str]:
    """Submit job to HPC cluster.
    
    Parameters
    ----------
    tool : str
        Tool name
    participant_label : str
        Subject/participant label
    dataset : str
        Dataset name
    args : argparse.Namespace
        Parsed command line arguments
        
    Returns
    -------
    Optional[str]
        Job ID if submission successful, None otherwise
    """
    logger.info(f"Preparing HPC job for {tool} on {participant_label}...")
    
    # Validate configuration
    validate_hpc_config(args)
    
    username = args.hpc_username
    hostname = args.hpc_hostname
    keyfile = args.hpc_keyfile
    gateway = getattr(args, 'hpc_gateway', None)
    
    # Test SSH connection
    if not test_ssh_connection(username, hostname, keyfile, gateway):
        logger.error("Cannot connect to HPC. Please check SSH configuration.")
        return None
    
    # Check required data exists on HPC
    hpc_rawdata = getattr(args, 'hpc_rawdata', None)
    hpc_derivatives = getattr(args, 'hpc_derivatives', None)
    
    if not check_required_data(tool, dataset, participant_label, args, username, hostname, 
                               keyfile, gateway, hpc_rawdata, hpc_derivatives):
        logger.error("Required data not available on HPC. Job submission cancelled.")
        return None
    
    # Generate HPC script
    script_content = generate_hpc_script(
        tool=tool,
        participant_label=participant_label,
        dataset=dataset,
        args=args,
        hpc_rawdata=hpc_rawdata,
        hpc_derivatives=hpc_derivatives,
        hpc_apptainer_dir=args.hpc_apptainer_dir
    )
    
    # Create temporary script file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
        f.write(script_content)
        local_script = f.name
    
    try:
        # Create remote directory for job scripts
        remote_dir = f"~/ln2t_hpc_jobs/{dataset}"
        ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"mkdir -p {remote_dir}"]
        subprocess.run(ssh_cmd, check=True, capture_output=True)
        
        # Copy script to HPC
        remote_script = f"{remote_dir}/{tool}_{participant_label}.sh"
        logger.info(f"Copying job script to {username}@{hostname}:{remote_script}")
        scp_cmd = get_scp_command(username, hostname, keyfile, gateway) + [
            local_script, f"{username}@{hostname}:{remote_script}"
        ]
        subprocess.run(scp_cmd, check=True, capture_output=True)
        
        # Submit job
        logger.info("Submitting job to HPC...")
        ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"cd {remote_dir} && sbatch {tool}_{participant_label}.sh"
        ]
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, check=True)
        
        # Parse job ID
        output = result.stdout.strip()
        if "Submitted batch job" in output:
            job_id = output.split()[-1]
            logger.info(f"✓ Job submitted successfully! Job ID: {job_id}")
            return job_id
        else:
            logger.error(f"Unexpected sbatch output: {output}")
            return None
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit HPC job: {e.stderr}")
        return None
    finally:
        Path(local_script).unlink(missing_ok=True)


def submit_multiple_jobs(
    tool: str,
    participant_labels: List[str],
    dataset: str,
    args: Any
) -> List[str]:
    """Submit multiple jobs in parallel for different participants.
    
    Parameters
    ----------
    tool : str
        Tool name
    participant_labels : List[str]
        List of participant labels
    dataset : str
        Dataset name
    args : Any
        Arguments namespace
        
    Returns
    -------
    List[str]
        List of job IDs for submitted jobs
    """
    job_ids = []
    
    logger.info(f"Submitting {len(participant_labels)} jobs in parallel...")
    
    for participant_label in participant_labels:
        job_id = submit_hpc_job(tool, participant_label, dataset, args)
        if job_id:
            job_ids.append(job_id)
        else:
            logger.warning(f"Failed to submit job for participant {participant_label}")
    
    return job_ids


def print_download_command(tool: str, dataset: str, args: Any, job_ids: List[str]) -> None:
    """Print command for downloading results from HPC.
    
    Parameters
    ----------
    tool : str
        Tool name
    dataset : str
        Dataset name
    args : Any
        Arguments namespace
    job_ids : List[str]
        List of job IDs
    """
    username = args.hpc_username
    hostname = args.hpc_hostname
    keyfile = args.hpc_keyfile
    gateway = getattr(args, 'hpc_gateway', None)
    hpc_derivatives = getattr(args, 'hpc_derivatives', '$GLOBALSCRATCH/derivatives')
    
    # Determine version and output directory
    version = getattr(args, 'version', {
        'freesurfer': '7.3.2',
        'fmriprep': '25.1.4',
        'qsiprep': '1.0.1',
        'qsirecon': '1.1.1',
        'meld_graph': 'v2.2.3'
    }.get(tool, ''))
    
    remote_path = f"{hpc_derivatives}/{dataset}-derivatives/{tool}_{version}/"
    local_path = f"~/derivatives/{dataset}-derivatives/{tool}_{version}/"
    
    # Build rsync command
    if gateway:
        proxy_cmd = f"-e 'ssh -i {keyfile} -J {username}@{gateway}'"
    else:
        proxy_cmd = f"-e 'ssh -i {keyfile}'"
    
    rsync_cmd = f"rsync -avz --progress {proxy_cmd} {username}@{hostname}:{remote_path} {local_path}"
    
    print("\n" + "="*80)
    print("HPC JOB SUBMISSION COMPLETE")
    print("="*80)
    print(f"\nSubmitted {len(job_ids)} job(s):")
    for job_id in job_ids:
        print(f"  - Job ID: {job_id}")
    
    print(f"\nTo download results when jobs complete, run:")
    print(f"\n{rsync_cmd}")
    print("\n" + "="*80 + "\n")


def check_job_status(job_id: str, username: str, hostname: str, keyfile: str, 
                    gateway: Optional[str] = None) -> Optional[Dict[str, str]]:
    """Check status of HPC job.
    
    Parameters
    ----------
    job_id : str
        Job ID
    username : str
        HPC username
    hostname : str
        HPC hostname
    keyfile : str
        SSH keyfile path
    gateway : Optional[str]
        ProxyJump gateway
        
    Returns
    -------
    Optional[Dict[str, str]]
        Job status information or None if error
    """
    try:
        ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"squeue -j {job_id} --format='%T|%M|%L'"
        ]
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                state, time_used, time_left = lines[1].split('|')
                return {
                    'state': state,
                    'time_used': time_used,
                    'time_left': time_left
                }
        return None
    except Exception as e:
        logger.error(f"Error checking job status: {e}")
        return None
