"""HPC cluster job submission utilities.

Supports SLURM-based HPC clusters with configurable connection settings.
"""

import atexit
import logging
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import tempfile

logger = logging.getLogger(__name__)

# Global SSH ControlMaster state
_ssh_control_path: Optional[str] = None
_ssh_control_process: Optional[subprocess.Popen] = None


def _get_control_path() -> str:
    """Get or create the SSH ControlMaster socket path."""
    global _ssh_control_path
    if _ssh_control_path is None:
        # Create a unique socket path in /tmp
        _ssh_control_path = f"/tmp/ln2t_ssh_control_{os.getpid()}"
    return _ssh_control_path


def _cleanup_ssh_control():
    """Cleanup SSH ControlMaster connection on exit."""
    global _ssh_control_process, _ssh_control_path
    if _ssh_control_process is not None:
        try:
            _ssh_control_process.terminate()
            _ssh_control_process.wait(timeout=5)
        except Exception:
            pass
        _ssh_control_process = None
    if _ssh_control_path and Path(_ssh_control_path).exists():
        try:
            Path(_ssh_control_path).unlink()
        except Exception:
            pass


# Register cleanup on exit
atexit.register(_cleanup_ssh_control)


def start_ssh_control_master(username: str, hostname: str, keyfile: str, gateway: Optional[str] = None) -> bool:
    """Start an SSH ControlMaster connection for connection reuse.
    
    This establishes a persistent SSH connection that subsequent SSH commands
    can reuse, avoiding rate limiting issues from rapid successive connections.
    
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
        True if ControlMaster started successfully
    """
    global _ssh_control_process
    
    # If already running, check if it's still alive
    if _ssh_control_process is not None:
        if _ssh_control_process.poll() is None:
            return True  # Still running
        else:
            _ssh_control_process = None  # Died, need to restart
    
    control_path = _get_control_path()
    keyfile_expanded = str(Path(keyfile).expanduser())
    
    cmd = [
        "ssh",
        "-i", keyfile_expanded,
        "-o", "ConnectTimeout=15",
        "-o", "ControlMaster=yes",
        "-o", f"ControlPath={control_path}",
        "-o", "ControlPersist=300",  # Keep connection alive for 5 minutes
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-N",  # Don't execute remote command, just hold connection
    ]
    
    if gateway:
        cmd.extend(["-J", f"{username}@{gateway}"])
    
    cmd.append(f"{username}@{hostname}")
    
    try:
        logger.debug(f"Starting SSH ControlMaster: {' '.join(cmd)}")
        _ssh_control_process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        # Give it a moment to establish
        time.sleep(1)
        
        # Check if it's still running (didn't fail immediately)
        if _ssh_control_process.poll() is not None:
            stderr = _ssh_control_process.stderr.read().decode() if _ssh_control_process.stderr else ""
            logger.warning(f"SSH ControlMaster failed to start: {stderr}")
            _ssh_control_process = None
            return False
        
        logger.info(f"✓ SSH ControlMaster established to {username}@{hostname}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to start SSH ControlMaster: {e}")
        return False


def stop_ssh_control_master():
    """Stop the SSH ControlMaster connection."""
    _cleanup_ssh_control()


def get_ssh_command(username: str, hostname: str, keyfile: str, gateway: Optional[str] = None) -> list:
    """Get SSH command with proper key configuration and optional ProxyJump.
    
    If an SSH ControlMaster is active, the command will reuse that connection.
    
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
    control_path = _get_control_path()
    
    cmd = [
        "ssh",
        "-i", str(Path(keyfile).expanduser()),
        "-o", "ConnectTimeout=10",
    ]
    
    # If ControlMaster socket exists, use it
    if Path(control_path).exists():
        cmd.extend([
            "-o", f"ControlPath={control_path}",
        ])
    
    if gateway:
        cmd.extend(["-J", f"{username}@{gateway}"])
    
    cmd.append(f"{username}@{hostname}")
    
    return cmd


def resolve_hpc_env_var(
    var_path: str,
    username: str,
    hostname: str,
    keyfile: str,
    gateway: Optional[str] = None
) -> str:
    """Resolve environment variable path on HPC to its actual value.
    
    Parameters
    ----------
    var_path : str
        Path that may contain environment variables (e.g., '$GLOBALSCRATCH/rawdata')
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
    str
        Resolved path with environment variables expanded
    """
    if not var_path or '$' not in var_path:
        return var_path
    
    # Use login shell to resolve environment variables
    ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
        f"bash -l -c 'echo {var_path}'"
    ]
    
    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            # Take last line to skip shell init output
            resolved = result.stdout.strip().split('\n')[-1]
            if resolved and not resolved.startswith('$'):
                logger.debug(f"Resolved '{var_path}' to '{resolved}'")
                return resolved
    except Exception as e:
        logger.warning(f"Failed to resolve HPC path '{var_path}': {e}")
    
    # Return original if resolution failed
    return var_path


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
    elif tool == "cvrmap":
        tool_owner = "ln2t"
    else:
        logger.error(f"Unknown tool when checking HPC image: {tool}")
        return False

    # Image filename uses the version/token as provided (allow full tags like 'cuda-v2.4.2')
    image_name = f"{tool_owner}.{tool}.{version}.sif"
    remote_path = f"{hpc_apptainer_dir}/{image_name}"

    # Use login shell to expand environment variables like $GLOBALSCRATCH
    ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"bash -l -c 'test -e {remote_path}'"]

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
        "cvrmap": "ln2t",
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


def get_tool_owner(tool: str) -> str:
    """Get Docker Hub owner for a tool.
    
    Parameters
    ----------
    tool : str
        Tool name
        
    Returns
    -------
    str
        Docker Hub owner/organization
    """
    tool_owners = {
        "freesurfer": "freesurfer",
        "fastsurfer": "deepmi",
        "fmriprep": "nipreps",
        "qsiprep": "pennlinc",
        "qsirecon": "pennlinc",
        "meld_graph": "meldproject",
        "cvrmap": "ln2t",
    }
    return tool_owners.get(tool, tool)


def generate_apptainer_build_script(
    tool: str,
    version: str,
    hpc_apptainer_dir: str,
    dataset: str
) -> str:
    """Generate SLURM script for building an Apptainer image on HPC.
    
    Parameters
    ----------
    tool : str
        Tool name
    version : str
        Tool version/tag
    hpc_apptainer_dir : str
        Path to apptainer images directory on HPC
    dataset : str
        Dataset name (for job naming and output paths)
        
    Returns
    -------
    str
        SLURM batch script content
    """
    tool_owner = get_tool_owner(tool)
    image_name = f"{tool_owner}.{tool}.{version}.sif"
    remote_path = f"{hpc_apptainer_dir}/{image_name}"
    docker_uri = f"docker://{tool_owner}/{tool}:{version}"
    
    job_name = f"apptainer_build_{tool}_{version}".replace(".", "_")
    
    script = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --cpus-per-task=4
#SBATCH --time=4:00:00
#SBATCH --mem=32G
#SBATCH --output={job_name}_%j.out
#SBATCH --error={job_name}_%j.err

# Apptainer Build Job
# ===================
# Tool: {tool}
# Version: {version}
# Docker URI: {docker_uri}
# Output: {remote_path}

echo "Job started at: $(date)"
echo "Running on node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"

# Create output directory if needed
mkdir -p {hpc_apptainer_dir}

# Set temporary directory for build (use local scratch if available)
if [ -d "$LOCALSCRATCH" ]; then
    export APPTAINER_TMPDIR="$LOCALSCRATCH"
    echo "Using LOCALSCRATCH for temp: $APPTAINER_TMPDIR"
elif [ -d "/tmp" ]; then
    export APPTAINER_TMPDIR="/tmp/$USER/apptainer_build_$$"
    mkdir -p "$APPTAINER_TMPDIR"
    echo "Using /tmp for temp: $APPTAINER_TMPDIR"
fi

# Build the image
echo ""
echo "Building Apptainer image..."
echo "  Source: {docker_uri}"
echo "  Target: {remote_path}"
echo ""

apptainer build {remote_path} {docker_uri}

BUILD_STATUS=$?

# Cleanup temp directory
if [ -n "$APPTAINER_TMPDIR" ] && [ -d "$APPTAINER_TMPDIR" ]; then
    rm -rf "$APPTAINER_TMPDIR"
fi

if [ $BUILD_STATUS -eq 0 ]; then
    echo ""
    echo "✓ Build completed successfully!"
    echo "  Image saved to: {remote_path}"
    ls -lh {remote_path}
else
    echo ""
    echo "✗ Build failed with exit code: $BUILD_STATUS"
fi

echo ""
echo "Job finished at: $(date)"

exit $BUILD_STATUS
"""
    return script


def prompt_apptainer_build(
    tool: str,
    version: str,
    dataset: str,
    args: Any
) -> bool:
    """Prompt user to build missing Apptainer image on HPC.
    
    If user accepts, submits a build job to the HPC.
    If user declines, saves the job script locally and prints manual instructions.
    
    Parameters
    ----------
    tool : str
        Tool name
    version : str
        Tool version
    dataset : str
        Dataset name
    args : Any
        Arguments namespace with HPC configuration
        
    Returns
    -------
    bool
        True if build job was submitted or image was pushed, False otherwise (user can re-run after build)
    """
    username = args.hpc_username
    hostname = args.hpc_hostname
    keyfile = args.hpc_keyfile
    gateway = getattr(args, 'hpc_gateway', None)
    hpc_apptainer_dir = getattr(args, 'hpc_apptainer_dir', None) or "$GLOBALSCRATCH/apptainer"
    
    tool_owner = get_tool_owner(tool)
    image_name = f"{tool_owner}.{tool}.{version}.sif"
    remote_path = f"{hpc_apptainer_dir}/{image_name}"
    docker_uri = f"docker://{tool_owner}/{tool}:{version}"
    
    # Check if local image exists
    local_apptainer_dir = Path(getattr(args, 'apptainer_dir', '/opt/apptainer'))
    local_image_path = local_apptainer_dir / image_name
    local_image_exists = local_image_path.exists()
    
    print("\n" + "="*70)
    print("⚠️  MISSING APPTAINER IMAGE ON HPC")
    print("="*70)
    print(f"\nThe required Apptainer image was not found on the cluster:")
    print(f"  Tool: {tool}")
    print(f"  Version: {version}")
    print(f"  Expected path: {remote_path}")
    print("")
    
    # Show options based on whether local image exists
    if local_image_exists:
        print("Options:")
        print("  [1] Push local image to HPC (recommended - faster)")
        print(f"      Local image found: {local_image_path}")
        print("  [2] Submit a batch job to build the image on HPC")
        print("  [3] Cancel and build manually")
        print("")
        response = input("Choose an option [1/2/3]: ").strip()
    else:
        print("Building Apptainer images can be memory-intensive and may fail in")
        print("interactive sessions. We recommend submitting a batch job to build it.")
        print("")
        response = input("Would you like to submit a job to build the image? [y/N]: ").strip().lower()
        # Map y/n to 2/3 for unified handling
        if response == 'y':
            response = '2'
        else:
            response = '3'
    
    # Option 1: Push local image to HPC
    if response == '1' and local_image_exists:
        print(f"\nPushing local image to HPC...")
        print(f"  Source: {local_image_path}")
        print(f"  Destination: {username}@{hostname}:{remote_path}")
        print("")
        print("This may take a while depending on the image size and network speed...")
        
        try:
            # Create remote directory if it doesn't exist (use login shell for env var expansion)
            ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"bash -l -c 'mkdir -p {hpc_apptainer_dir}'"]
            subprocess.run(ssh_cmd, check=True, capture_output=True)
            
            # Resolve remote path for scp (need to expand $GLOBALSCRATCH)
            if hpc_apptainer_dir.startswith('$'):
                resolve_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"bash -l -c 'echo {remote_path}'"]
                result = subprocess.run(resolve_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0 and result.stdout.strip():
                    remote_path = result.stdout.strip().split('\n')[-1]
            
            # Push the image using scp
            scp_cmd = get_scp_command(username, hostname, keyfile, gateway) + [
                str(local_image_path), f"{username}@{hostname}:{remote_path}"
            ]
            result = subprocess.run(scp_cmd, capture_output=False)
            
            if result.returncode == 0:
                print("\n" + "="*70)
                print("✓ IMAGE PUSHED SUCCESSFULLY")
                print("="*70)
                print(f"\n  Image now available at: {remote_path}")
                print("")
                print("You can now re-run your original command.")
                print("="*70 + "\n")
                return True
            else:
                print(f"\n✗ Failed to push image (exit code {result.returncode})")
                print("Falling back to manual instructions...")
                response = '3'
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to push image: {e}")
            print(f"\n✗ Failed to push image. Error: {e.stderr if hasattr(e, 'stderr') else e}")
            response = '3'
        except Exception as e:
            logger.error(f"Error pushing image: {e}")
            print(f"\n✗ Error: {e}")
            response = '3'
    
    # Generate the build script (needed for options 2 and 3)
    script_content = generate_apptainer_build_script(
        tool=tool,
        version=version,
        hpc_apptainer_dir=hpc_apptainer_dir,
        dataset=dataset
    )
    
    # Prepare paths for saving
    code_dir = Path.home() / "code" / f"{dataset}-code"
    code_dir.mkdir(parents=True, exist_ok=True)
    local_script_path = code_dir / f"build_apptainer_{tool}_{version.replace('.', '_')}.sh"
    
    # Option 2: Submit build job
    if response == '2':
        # Submit the job
        print(f"\nSubmitting Apptainer build job...")
        
        try:
            # Create temporary script file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
                f.write(script_content)
                temp_script = f.name
            
            # Create remote directory for job scripts
            remote_dir = f"~/ln2t_hpc_jobs/apptainer_builds"
            ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [f"mkdir -p {remote_dir}"]
            subprocess.run(ssh_cmd, check=True, capture_output=True)
            
            # Copy script to HPC
            remote_script = f"{remote_dir}/build_{tool}_{version.replace('.', '_')}.sh"
            scp_cmd = get_scp_command(username, hostname, keyfile, gateway) + [
                temp_script, f"{username}@{hostname}:{remote_script}"
            ]
            subprocess.run(scp_cmd, check=True, capture_output=True)
            
            # Submit job
            ssh_cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
                f"cd {remote_dir} && sbatch build_{tool}_{version.replace('.', '_')}.sh"
            ]
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, check=True)
            
            # Parse job ID
            job_id = None
            for text in [result.stdout.strip(), result.stderr.strip()]:
                match = re.search(r'Submitted batch job (\d+)', text)
                if match:
                    job_id = match.group(1)
                    break
            
            # Save script locally for reference
            with open(local_script_path, 'w') as f:
                f.write(script_content)
            
            print("\n" + "="*70)
            print("✓ APPTAINER BUILD JOB SUBMITTED")
            print("="*70)
            if job_id:
                print(f"\n  Job ID: {job_id}")
            print(f"  Remote script: {remote_script}")
            print(f"  Local copy: {local_script_path}")
            print("")
            print("Next steps:")
            print(f"  1. Monitor the job: ssh {username}@{hostname} 'squeue -u {username}'")
            print(f"  2. Check job output: ssh {username}@{hostname} 'cat ~/ln2t_hpc_jobs/apptainer_builds/*.out'")
            print(f"  3. Once complete, re-run your original command")
            print("")
            print("="*70 + "\n")
            
            # Cleanup
            Path(temp_script).unlink(missing_ok=True)
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to submit build job: {e}")
            print(f"\n✗ Failed to submit job. Error: {e.stderr if hasattr(e, 'stderr') else e}")
            # Fall through to manual instructions
            response = '3'
        except Exception as e:
            logger.error(f"Error submitting build job: {e}")
            print(f"\n✗ Error: {e}")
            response = '3'
    
    # Option 3: Manual instructions (or fallback from failed options)
    # Save script locally
    with open(local_script_path, 'w') as f:
        f.write(script_content)
    
    print("\n" + "="*70)
    print("📋 MANUAL BUILD INSTRUCTIONS")
    print("="*70)
    print(f"\nThe SLURM job script has been saved to:")
    print(f"  {local_script_path}")
    print("")
    
    # If local image exists, show push option in manual instructions
    if local_image_exists:
        print("Option A - Push local image:")
        scp_opts = f"-i {keyfile}"
        if gateway:
            scp_opts += f" -o ProxyJump={username}@{gateway}"
        print(f"  scp {scp_opts} {local_image_path} {username}@{hostname}:{remote_path}")
        print("")
        print("Option B - Submit build job:")
    else:
        print("To submit the build job manually:")
    
    print("")
    print(f"  1. Copy the script to the HPC:")
    
    # Build scp command for display
    scp_opts = f"-i {keyfile}"
    if gateway:
        scp_opts += f" -o ProxyJump={username}@{gateway}"
    print(f"     scp {scp_opts} {local_script_path} {username}@{hostname}:~/")
    print("")
    print(f"  2. SSH to the HPC and submit the job:")
    ssh_opts = f"-i {keyfile}"
    if gateway:
        ssh_opts += f" -J {username}@{gateway}"
    print(f"     ssh {ssh_opts} {username}@{hostname}")
    print(f"     sbatch ~/build_apptainer_{tool}_{version.replace('.', '_')}.sh")
    print("")
    print("Alternatively, build interactively (if you have enough memory):")
    print(f"     apptainer build {remote_path} {docker_uri}")
    print("")
    print("Once the image is built, re-run your original command.")
    print("="*70 + "\n")
    
    return False


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
    """Validate HPC configuration arguments and set defaults."""
    if args.hpc:
        # Set default for hpc_apptainer_dir if not provided
        if not getattr(args, 'hpc_apptainer_dir', None):
            args.hpc_apptainer_dir = "$GLOBALSCRATCH/apptainer"
        
        required_args = {
            '--hpc-username': args.hpc_username,
            '--hpc-hostname': args.hpc_hostname,
            '--hpc-keyfile': args.hpc_keyfile,
        }
        
        missing = [arg for arg, value in required_args.items() if not value]
        if missing:
            raise ValueError(
                f"When using --hpc, you must provide: {', '.join(missing)}\n"
                f"Example: --hpc-username arovai --hpc-hostname lyra.ulb.ac.be "
                f"--hpc-keyfile ~/.ssh/id_rsa\n"
                f"Note: --hpc-apptainer-dir, --hpc-rawdata, and --hpc-derivatives are optional "
                f"(defaults: $GLOBALSCRATCH/apptainer, $GLOBALSCRATCH/rawdata, $GLOBALSCRATCH/derivatives on cluster)"
            )


def test_ssh_connection(username: str, hostname: str, keyfile: str, gateway: Optional[str] = None) -> bool:
    """Test SSH connection to HPC and establish ControlMaster for connection reuse.
    
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
    # First, start the ControlMaster for connection reuse
    if not start_ssh_control_master(username, hostname, keyfile, gateway):
        logger.warning("Could not establish SSH ControlMaster, will use individual connections")
    
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
        # Quote the remote_path to avoid remote shell word-splitting/expansion issues
        remote_test = f"test -e '{remote_path}' && echo 'exists' || echo 'not_found'"
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + [remote_test]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )

        # If the test failed, log the command and returned output to help debugging
        if not (result.returncode == 0 and "exists" in result.stdout):
            try:
                logger.debug(f"SSH command for remote check: {' '.join(cmd)}")
            except Exception:
                logger.debug("SSH command for remote check (could not join cmd list)")
            logger.debug(f"Remote check stdout: {result.stdout!r}")
            logger.debug(f"Remote check stderr: {result.stderr!r}")

        return result.returncode == 0 and "exists" in result.stdout
    except Exception as e:
        logger.error(f"Error checking remote path {remote_path}: {e}")
        return False


def prompt_upload_data(local_path: str, remote_path: str, username: str, hostname: str, 
                      keyfile: str, gateway: Optional[str], participant_label: str = "") -> bool:
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
    participant_label : str
        Participant label for logging context
        
    Returns
    -------
    bool
        True if upload successful or user declined, False if upload failed
    """
    participant_info = f"[sub-{participant_label}] " if participant_label else ""
    print(f"\n⚠️  {participant_info}Required data not found on HPC: {remote_path}")
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
    logger.info(f"Checking required data for participant sub-{participant_label}...")
    
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
    # Use login shell (-l) to ensure environment variables like $GLOBALSCRATCH are set
    if hpc_rawdata_check.startswith('$'):
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"bash -l -c 'echo {hpc_rawdata_check}'"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            hpc_rawdata_check = result.stdout.strip().split('\n')[-1]  # Take last line (skip any shell init output)
    
    if hpc_derivatives_check.startswith('$'):
        cmd = get_ssh_command(username, hostname, keyfile, gateway) + [
            f"bash -l -c 'echo {hpc_derivatives_check}'"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            hpc_derivatives_check = result.stdout.strip().split('\n')[-1]  # Take last line (skip any shell init output)
    
    # Check rawdata
    rawdata_path = f"{hpc_rawdata_check}/{dataset}-rawdata"
    logger.info(f"  [sub-{participant_label}] Checking rawdata on HPC: {rawdata_path}")
    if not check_remote_path_exists(username, hostname, keyfile, gateway, rawdata_path):
        logger.warning(f"[sub-{participant_label}] Required data not found on HPC: {rawdata_path}")
        local_rawdata = Path.home() / "rawdata" / f"{dataset}-rawdata"
        if local_rawdata.exists():
            if not prompt_upload_data(str(local_rawdata), rawdata_path, username, hostname, keyfile, gateway, participant_label):
                return False
        else:
            print(f"\n✗ [sub-{participant_label}] Rawdata not found locally at {local_rawdata}")
            print(f"   Cannot proceed without rawdata on HPC")
            return False
    else:
        logger.info(f"  [sub-{participant_label}] ✓ Rawdata found on HPC")
    
    # Check for precomputed FreeSurfer if required
    # fMRIPrep now requires pre-computed FreeSurfer outputs by default
    if tool == 'fmriprep':
        allow_fs_reconall = getattr(args, 'fmriprep_reconall', False)
        
        if not allow_fs_reconall:
            # FreeSurfer is required unless user explicitly allows reconstruction
            from ln2t_tools.utils.defaults import DEFAULT_FMRIPREP_FS_VERSION
            fs_version = DEFAULT_FMRIPREP_FS_VERSION
            fs_base_path = f"{hpc_derivatives_check}/{dataset}-derivatives/freesurfer_{fs_version}"
            fs_subject_path = f"{fs_base_path}/sub-{participant_label}"
            
            logger.info(f"  [sub-{participant_label}] Checking FreeSurfer outputs on HPC (required by default): {fs_subject_path}")
            
            if not check_remote_path_exists(username, hostname, keyfile, gateway, fs_subject_path):
                logger.warning(f"[sub-{participant_label}] Required FreeSurfer outputs not found on HPC: {fs_subject_path}")
                local_fs = Path.home() / "derivatives" / f"{dataset}-derivatives" / f"freesurfer_{fs_version}"
                if local_fs.exists():
                    if not prompt_upload_data(str(local_fs), fs_base_path, username, hostname, keyfile, gateway, participant_label):
                        return False
                else:
                    print(f"\n✗ [sub-{participant_label}] FreeSurfer outputs not found on HPC: {fs_subject_path}")
                    print(f"   fMRIPrep now requires pre-computed FreeSurfer outputs by default.")
                    print(f"   Either:")
                    print(f"     1. Run FreeSurfer first: ln2t_tools freesurfer --dataset {dataset} --participant-label {participant_label}")
                    print(f"     2. Use --fmriprep-reconall to allow fMRIPrep to run FreeSurfer reconstruction")
                    return False
            else:
                logger.info(f"  [sub-{participant_label}] ✓ FreeSurfer outputs found on HPC")
        else:
            logger.info(f"  [sub-{participant_label}] --fmriprep-reconall enabled, will allow FreeSurfer reconstruction if needed")
    
    # Check for precomputed FreeSurfer if meld_graph requires it
    elif tool == 'meld_graph' and getattr(args, 'use_precomputed_fs', False):
        fs_version = getattr(args, 'fs_version', '7.2.0')
        fs_base_path = f"{hpc_derivatives_check}/{dataset}-derivatives/freesurfer_{fs_version}"
        fs_subject_path = f"{fs_base_path}/sub-{participant_label}"
        
        logger.info(f"  [sub-{participant_label}] Checking FreeSurfer outputs on HPC: {fs_subject_path}")
        
        if not check_remote_path_exists(username, hostname, keyfile, gateway, fs_subject_path):
            logger.warning(f"[sub-{participant_label}] Required FreeSurfer outputs not found on HPC: {fs_subject_path}")
            local_fs = Path.home() / "derivatives" / f"{dataset}-derivatives" / f"freesurfer_{fs_version}"
            if local_fs.exists():
                if not prompt_upload_data(str(local_fs), fs_base_path, username, hostname, keyfile, gateway, participant_label):
                    return False
            else:
                print(f"\n✗ [sub-{participant_label}] FreeSurfer outputs not found locally at {local_fs}")
                print(f"   Cannot use --use-precomputed-fs without FreeSurfer outputs on HPC")
                return False
        else:
            logger.info(f"  [sub-{participant_label}] ✓ FreeSurfer outputs found on HPC")
    
    # Check for QSIPrep outputs if running QSIRecon
    if tool == 'qsirecon':
        from ln2t_tools.utils.defaults import DEFAULT_QSIPREP_VERSION
        # QSIRecon requires QSIPrep v1.1.1
        qsiprep_version = getattr(args, 'qsiprep_version', DEFAULT_QSIPREP_VERSION)
        qsiprep_path = f"{hpc_derivatives_check}/{dataset}-derivatives/qsiprep_{qsiprep_version}"
        
        logger.info(f"  [sub-{participant_label}] Checking QSIPrep outputs on HPC: {qsiprep_path}")
        
        if not check_remote_path_exists(username, hostname, keyfile, gateway, qsiprep_path):
            logger.warning(f"[sub-{participant_label}] Required QSIPrep outputs not found on HPC: {qsiprep_path}")
            local_qsiprep = Path.home() / "derivatives" / f"{dataset}-derivatives" / f"qsiprep_{qsiprep_version}"
            if local_qsiprep.exists():
                if not prompt_upload_data(str(local_qsiprep), qsiprep_path, username, hostname, keyfile, gateway, participant_label):
                    return False
            else:
                print(f"\n✗ [sub-{participant_label}] QSIPrep outputs not found locally at {local_qsiprep}")
                print(f"   Cannot run QSIRecon without QSIPrep outputs on HPC")
                return False
        else:
            logger.info(f"  [sub-{participant_label}] ✓ QSIPrep outputs found on HPC")
    
    # Check for fMRIPrep outputs if running CVRmap
    if tool == 'cvrmap':
        from ln2t_tools.utils.defaults import DEFAULT_CVRMAP_FMRIPREP_VERSION
        fmriprep_version = getattr(args, 'fmriprep_version', DEFAULT_CVRMAP_FMRIPREP_VERSION)
        fmriprep_path = f"{hpc_derivatives_check}/{dataset}-derivatives/fmriprep_{fmriprep_version}"
        
        logger.info(f"  [sub-{participant_label}] Checking fMRIPrep outputs on HPC: {fmriprep_path}")
        
        if not check_remote_path_exists(username, hostname, keyfile, gateway, fmriprep_path):
            logger.warning(f"[sub-{participant_label}] Required fMRIPrep outputs not found on HPC: {fmriprep_path}")
            local_fmriprep = Path.home() / "derivatives" / f"{dataset}-derivatives" / f"fmriprep_{fmriprep_version}"
            if local_fmriprep.exists():
                if not prompt_upload_data(str(local_fmriprep), fmriprep_path, username, hostname, keyfile, gateway, participant_label):
                    return False
            else:
                print(f"\n✗ [sub-{participant_label}] fMRIPrep outputs not found locally at {local_fmriprep}")
                print(f"   Cannot run CVRmap without fMRIPrep outputs on HPC")
                return False
        else:
            logger.info(f"  [sub-{participant_label}] ✓ fMRIPrep outputs found on HPC")
    
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
    
    This function generates SLURM batch scripts with tool_args pass-through.
    Tool-specific arguments should be provided via --tool-args on the CLI.
    
    Parameters
    ----------
    tool : str
        Tool name (e.g., 'meld_graph', 'freesurfer', 'fmriprep')
    participant_label : str
        Subject/participant label
    dataset : str
        Dataset name
    args : argparse.Namespace
        Parsed command line arguments (includes tool_args)
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
    # Get tool_args for pass-through
    tool_args = getattr(args, 'tool_args', '') or ''
    
    # Determine job name
    job_name = f"{tool}-{dataset}-{participant_label}"
    
    # Paths should be resolved by caller - these are fallbacks
    if not hpc_rawdata:
        logger.warning("hpc_rawdata not provided to generate_hpc_script - using $GLOBALSCRATCH fallback")
        hpc_rawdata = "$GLOBALSCRATCH/rawdata"
    if not hpc_derivatives:
        logger.warning("hpc_derivatives not provided to generate_hpc_script - using $GLOBALSCRATCH fallback")
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
    # fastsurfer: GPU strongly recommended for deep learning segmentation
    # meld_graph: GPU required for inference
    if tool == 'fastsurfer':
        device = getattr(args, 'device', 'auto')
        if device != 'cpu':
            script += f"#SBATCH --gres=gpu:{gpus}\n"
    elif tool == 'meld_graph' and not getattr(args, 'no_gpu', False):
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
TOOL_ARGS="{tool_args}"
"""
    
    # Tool-specific command generation
    if tool == "freesurfer":
        version = getattr(args, 'version', '7.3.2')
        fs_license = getattr(args, 'hpc_fs_license', None) or '$HOME/licenses/license.txt'
        apptainer_img = f"{hpc_apptainer_dir}/freesurfer.freesurfer.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/freesurfer_{version}"
        
        script += f"""
# FreeSurfer setup
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
mkdir -p "$OUTPUT_DIR"

# Setup temp directory for FreeSurfer (required)
export TMPDIR="${{LOCALSCRATCH:-/tmp}}/${{SLURM_JOB_ID:-$$}}"
mkdir -p "$TMPDIR"

# Find T1w image for this participant
T1W_FILE=$(find "$HPC_RAWDATA/$DATASET-rawdata/$PARTICIPANT" -name "*_T1w.nii.gz" | head -1)
if [ -z "$T1W_FILE" ]; then
    echo "ERROR: No T1w file found for $PARTICIPANT"
    exit 1
fi
echo "Using T1w file: $T1W_FILE"

# Convert host path to container path
T1W_CONTAINER_PATH="/data/$PARTICIPANT/anat/$(basename "$T1W_FILE")"

# Run FreeSurfer
apptainer exec \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/output" \\
    -B "$FS_LICENSE:/usr/local/freesurfer/.license:ro" \\
    -B "$TMPDIR:/tmp" \\
    --env SUBJECTS_DIR=/output \\
    --env TMPDIR=/tmp \\
    --env FS_LICENSE=/usr/local/freesurfer/.license \\
    {apptainer_img} \\
    recon-all -s "$PARTICIPANT" -i "$T1W_CONTAINER_PATH" -all $TOOL_ARGS

# Cleanup temp directory
rm -rf "$TMPDIR"
"""
    
    elif tool == "fastsurfer":
        version = getattr(args, 'version', 'v2.4.2')
        fs_license = getattr(args, 'hpc_fs_license', None) or '$HOME/licenses/license.txt'
        apptainer_img = f"{hpc_apptainer_dir}/deepmi.fastsurfer.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/fastsurfer_{version}"
        
        # GPU support - check for --device cpu or --no-gpu in tool_args
        gpu_flag = "--nv"
        if '--device cpu' in tool_args or 'device=cpu' in tool_args:
            gpu_flag = ""
        
        script += f"""
# FastSurfer setup
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
mkdir -p "$OUTPUT_DIR"

# Find T1w image for this participant
T1W_FILE=$(find "$HPC_RAWDATA/$DATASET-rawdata/$PARTICIPANT" -name "*_T1w.nii.gz" | head -1)
if [ -z "$T1W_FILE" ]; then
    echo "ERROR: No T1w file found for $PARTICIPANT"
    exit 1
fi
echo "Using T1w file: $T1W_FILE"

# Run FastSurfer
apptainer exec {gpu_flag} \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/output" \\
    -B "$FS_LICENSE:/opt/freesurfer/license.txt:ro" \\
    --env FS_LICENSE=/opt/freesurfer/license.txt \\
    {apptainer_img} \\
    /fastsurfer/run_fastsurfer.sh \\
    --sd /output \\
    --sid $PARTICIPANT \\
    --t1 "$T1W_FILE" \\
    --fs_license /opt/freesurfer/license.txt \\
    $TOOL_ARGS
"""
    
    elif tool == "fmriprep":
        from ln2t_tools.utils.defaults import DEFAULT_FMRIPREP_FS_VERSION
        version = getattr(args, 'version', '25.1.4')
        fs_version = DEFAULT_FMRIPREP_FS_VERSION
        fs_license = getattr(args, 'hpc_fs_license', None) or '$HOME/licenses/license.txt'
        allow_fs_reconall = getattr(args, 'fmriprep_reconall', False)
        apptainer_img = f"{hpc_apptainer_dir}/nipreps.fmriprep.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/fmriprep_{version}"
        fs_output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/freesurfer_{fs_version}"
        
        # Handle FreeSurfer inputs based on --fmriprep-reconall flag
        if allow_fs_reconall:
            # User allows fMRIPrep to run reconstruction
            script += f"""
# fMRIPrep setup - allowing FreeSurfer reconstruction
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
WORK_DIR="$OUTPUT_DIR/work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Run fMRIPrep (will run FreeSurfer if needed)
apptainer run \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/work" \\
    -B "$FS_LICENSE:/opt/freesurfer/license.txt:ro" \\
    --env FS_LICENSE=/opt/freesurfer/license.txt \\
    --cleanenv \\
    {apptainer_img} \\
    /data /out participant \\
    --participant-label {participant_label} \\
    -w /work \\
    --skip-bids-validation \\
    $TOOL_ARGS
"""
        else:
            # FreeSurfer is pre-computed and required
            script += f"""
# fMRIPrep setup - using pre-computed FreeSurfer
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
FS_SUBJECTS_DIR="{fs_output_dir}"
WORK_DIR="$OUTPUT_DIR/work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Check that FreeSurfer subject directory exists
if [ ! -d "$FS_SUBJECTS_DIR/$PARTICIPANT" ]; then
    echo "ERROR: FreeSurfer outputs not found for $PARTICIPANT at $FS_SUBJECTS_DIR/$PARTICIPANT"
    echo "fMRIPrep now requires pre-computed FreeSurfer outputs by default."
    echo "Either run FreeSurfer first or use --fmriprep-reconall to allow reconstruction."
    exit 1
fi

# Run fMRIPrep with pre-computed FreeSurfer outputs
apptainer run \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/work" \\
    -B "$FS_SUBJECTS_DIR:/fsdir" \\
    -B "$FS_LICENSE:/opt/freesurfer/license.txt:ro" \\
    --env FS_LICENSE=/opt/freesurfer/license.txt \\
    --env SUBJECTS_DIR=/fsdir \\
    --cleanenv \\
    {apptainer_img} \\
    /data /out participant \\
    --participant-label {participant_label} \\
    -w /work \\
    --fs-subjects-dir /fsdir \\
    --skip-bids-validation \\
    $TOOL_ARGS
"""
    
    elif tool == "qsiprep":
        version = getattr(args, 'version', '1.0.1')
        apptainer_img = f"{hpc_apptainer_dir}/pennlinc.qsiprep.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/qsiprep_{version}"
        
        script += f"""
# QSIPrep setup
OUTPUT_DIR="{output_dir}"
WORK_DIR="$GLOBALSCRATCH/qsiprep_work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Run QSIPrep
apptainer run \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/tmp/work" \\
    --cleanenv --containall \\
    {apptainer_img} \\
    /data /out participant \\
    --participant-label {participant_label} \\
    --skip-bids-validation \\
    --work-dir /tmp/work/work \\
    $TOOL_ARGS

"""
    
    elif tool == "qsirecon":
        from ln2t_tools.utils.defaults import DEFAULT_QSIPREP_VERSION
        
        version = getattr(args, 'version', '1.1.1')
        fs_license = getattr(args, 'hpc_fs_license', None) or '$HOME/licenses/license.txt'
        apptainer_img = f"{hpc_apptainer_dir}/pennlinc.qsirecon.{version}.sif"
        output_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/qsirecon_{version}"
        qsiprep_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/qsiprep_{DEFAULT_QSIPREP_VERSION}"
        code_dir = f"$GLOBALSCRATCH/code/$DATASET-code"
        
        script += f"""
# QSIRecon setup
FS_LICENSE="{fs_license}"
OUTPUT_DIR="{output_dir}"
QSIPREP_DIR="{qsiprep_dir}"
CODE_DIR="{code_dir}"
WORK_DIR="$OUTPUT_DIR/work"
mkdir -p "$OUTPUT_DIR" "$WORK_DIR"

# Run QSIRecon
apptainer run --containall --writable-tmpfs \\
    -B "$FS_LICENSE:/opt/freesurfer/license.txt" \\
    -B "$QSIPREP_DIR:/data:ro" \\
    -B "$OUTPUT_DIR:/out" \\
    -B "$WORK_DIR:/work" \\
    -B "$CODE_DIR:/code:ro" \\
    {apptainer_img} \\
    /data /out participant \\
    --participant-label {participant_label} \\
    --fs-license-file /opt/freesurfer/license.txt \\
    -w /work \\
    $TOOL_ARGS
"""
    
    elif tool == "meld_graph":
        version = getattr(args, 'version', 'v2.2.3')
        fs_license = getattr(args, 'hpc_fs_license', None) or '$HOME/licenses/license.txt'
        apptainer_img = f"{hpc_apptainer_dir}/meldproject.meld_graph.{version}.sif"
        
        # GPU settings - check for --no-gpu in tool_args
        gpu_flag = "--nv"
        env_vars = "--env PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128 --env CUDA_LAUNCH_BLOCKING=1"
        if '--no-gpu' in tool_args:
            gpu_flag = ""
            env_vars = "--env CUDA_VISIBLE_DEVICES=''"
        
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
    {apptainer_img} \\
    python scripts/new_patient_pipeline/new_pt_pipeline.py -id sub-{participant_label} $TOOL_ARGS
"""
    
    elif tool == "cvrmap":
        # CVRmap for cerebrovascular reactivity mapping
        # Requires fMRIPrep preprocessed data
        from ln2t_tools.utils.defaults import DEFAULT_CVRMAP_FMRIPREP_VERSION
        
        version = getattr(args, 'version', '4.3.1')
        apptainer_img = f"{hpc_apptainer_dir}/ln2t.cvrmap.{version}.sif"
        # Bind the full derivatives directory (not just cvrmap output) so that
        # files in other subdirectories (e.g., vesseldensitymaps) are accessible
        # via /derivatives/ paths in --tool-args
        derivatives_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives"
        output_label = f"cvrmap_{version}"
        fmriprep_dir = f"$HPC_DERIVATIVES/$DATASET-derivatives/fmriprep_{DEFAULT_CVRMAP_FMRIPREP_VERSION}"
        
        script += f"""
# CVRmap setup
DERIVATIVES_DIR="{derivatives_dir}"
OUTPUT_LABEL="{output_label}"
FMRIPREP_DIR="{fmriprep_dir}"
mkdir -p "$DERIVATIVES_DIR/$OUTPUT_LABEL"

# Run CVRmap
apptainer run \\
    -B "$HPC_RAWDATA/$DATASET-rawdata:/data:ro" \\
    -B "$DERIVATIVES_DIR:/derivatives" \\
    -B "$FMRIPREP_DIR:/fmriprep:ro" \\
    {apptainer_img} \\
    /data /derivatives/$OUTPUT_LABEL participant \\
    --participant-label {participant_label} \\
    --derivatives fmriprep=/fmriprep \\
    $TOOL_ARGS
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
    
    # Get HPC paths and resolve environment variables
    # SLURM batch jobs don't have access to login shell environment variables like $GLOBALSCRATCH,
    # so we resolve them now to their actual paths
    hpc_rawdata = getattr(args, 'hpc_rawdata', None) or '$GLOBALSCRATCH/rawdata'
    hpc_derivatives = getattr(args, 'hpc_derivatives', None) or '$GLOBALSCRATCH/derivatives'
    hpc_apptainer_dir = args.hpc_apptainer_dir or '$GLOBALSCRATCH/apptainer'
    
    # Resolve environment variables to actual paths
    hpc_rawdata = resolve_hpc_env_var(hpc_rawdata, username, hostname, keyfile, gateway)
    hpc_derivatives = resolve_hpc_env_var(hpc_derivatives, username, hostname, keyfile, gateway)
    hpc_apptainer_dir = resolve_hpc_env_var(hpc_apptainer_dir, username, hostname, keyfile, gateway)
    
    if not check_required_data(tool, dataset, participant_label, args, username, hostname, 
                               keyfile, gateway, hpc_rawdata, hpc_derivatives):
        logger.error("Required data not available on HPC. Job submission cancelled.")
        return None
    
    # Generate HPC script with resolved paths
    script_content = generate_hpc_script(
        tool=tool,
        participant_label=participant_label,
        dataset=dataset,
        args=args,
        hpc_rawdata=hpc_rawdata,
        hpc_derivatives=hpc_derivatives,
        hpc_apptainer_dir=hpc_apptainer_dir
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
        
        # Parse job ID - check both stdout and stderr since output may vary
        output = result.stdout.strip()
        stderr = result.stderr.strip()
        logger.debug(f"sbatch stdout: {output!r}")
        logger.debug(f"sbatch stderr: {stderr!r}")
        
        # Look for job ID in stdout first, then stderr
        # Format can be "Submitted batch job 224780" or "Submitted batch job 224780 on cluster lyra"
        job_id = None
        for text in [output, stderr]:
            if "Submitted batch job" in text:
                # Extract job ID - it's the number after "Submitted batch job"
                match = re.search(r'Submitted batch job (\d+)', text)
                if match:
                    job_id = match.group(1)
                    break
        
        if job_id:
            logger.info(f"✓ Job submitted successfully! Job ID: {job_id}")
            
            # Save job information for status tracking
            try:
                from ln2t_tools.utils.hpc_status import JobInfo, save_job_info
                from datetime import datetime
                
                job_info = JobInfo(
                    job_id=job_id,
                    tool=tool,
                    dataset=dataset,
                    participant=participant_label,
                    submit_time=datetime.now().isoformat(),
                    state="SUBMITTED"
                )
                save_job_info(job_info)
                logger.debug(f"Saved job information for tracking")
            except Exception as e:
                logger.debug(f"Could not save job information: {e}")
            
            return job_id
        else:
            logger.error(f"Could not parse job ID from sbatch output. stdout: {output!r}, stderr: {stderr!r}")
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
    args: Any,
    submission_delay: float = 0.5
) -> List[str]:
    """Submit multiple jobs for different participants with staggered timing.
    
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
    submission_delay : float
        Delay in seconds between job submissions to avoid resource conflicts
        (default: 0.5 seconds)
        
    Returns
    -------
    List[str]
        List of job IDs for submitted jobs
    """
    job_ids = []
    
    logger.info(f"Submitting {len(participant_labels)} jobs (with {submission_delay}s delay between submissions)...")
    
    for i, participant_label in enumerate(participant_labels):
        job_id = submit_hpc_job(tool, participant_label, dataset, args)
        if job_id:
            job_ids.append(job_id)
        else:
            logger.warning(f"Failed to submit job for participant {participant_label}")
        
        # Add delay between submissions to stagger job starts and avoid file locking issues
        if i < len(participant_labels) - 1:  # No delay after last submission
            time.sleep(submission_delay)
    
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
    hpc_derivatives = getattr(args, 'hpc_derivatives', None) or '$GLOBALSCRATCH/derivatives'
    
    # Resolve environment variables in the path
    hpc_derivatives = resolve_hpc_env_var(hpc_derivatives, username, hostname, keyfile, gateway)
    
    # Determine version and output directory
    from ln2t_tools.utils.defaults import DEFAULT_QSIPREP_VERSION
    version = getattr(args, 'version', {
        'freesurfer': '7.3.2',
        'fmriprep': '25.1.4',
        'qsiprep': DEFAULT_QSIPREP_VERSION,
        'qsirecon': '1.1.1',
        'meld_graph': 'v2.2.3',
        'cvrmap': '4.3.1'
    }.get(tool, ''))
    
    remote_path = f"{hpc_derivatives}/{dataset}-derivatives/{tool}_{version}/"
    local_path = f"~/derivatives/{dataset}-derivatives/{tool}_{version}/"
    
    # Build rsync command with actual keyfile path
    keyfile_expanded = str(Path(keyfile).expanduser())
    if gateway:
        proxy_cmd = f"-e 'ssh -i {keyfile_expanded} -J {username}@{gateway}'"
    else:
        proxy_cmd = f"-e 'ssh -i {keyfile_expanded}'"
    
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
