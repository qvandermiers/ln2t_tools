# Instance Management

ln2t_tools includes built-in safeguards to prevent resource overload when running multiple parallel jobs:

- **Default limit**: Maximum 10 parallel instances
- **Lock files**: Stored in `/tmp/ln2t_tools_locks/` with detailed JSON metadata
- **Automatic cleanup**: Removes stale lock files from terminated processes
- **Graceful handling**: Shows helpful messages when limits are reached

## Lock File Structure

Each active instance creates a lock file with metadata:

- **Process ID (PID)**: System process identifier
- **Dataset name(s)**: Which dataset(s) are being processed
- **Tool(s)**: Which pipeline(s) are running
- **Participant labels**: Which subjects are being processed
- **Hostname**: Which machine is running the job
- **Username**: Which user started the job
- **Start time**: When the job was initiated

## Configuring Limits

To change the maximum number of parallel instances, modify the `MAX_INSTANCES` setting in `ln2t_tools/utils/utils.py`:

```python
MAX_INSTANCES = 10  # Adjust this value as needed
```

Higher values allow more simultaneous jobs but consume more system resources.

## Troubleshooting

**"Maximum parallel instances reached" error:**
- Check running instances: `ls -la /tmp/ln2t_tools_locks/`
- View lock file details: `cat /tmp/ln2t_tools_locks/*.json`
- Wait for jobs to complete, or manually remove stale lock files if processes have terminated

**Lock files not cleaning up:**
- Stale lock files are automatically removed on next run if their process ID no longer exists
- Manual cleanup: `rm /tmp/ln2t_tools_locks/*` (use with caution)

## Monitoring Concurrent Jobs

```bash
# View all active ln2t_tools instances
ls -la /tmp/ln2t_tools_locks/

# View details of a specific instance
cat /tmp/ln2t_tools_locks/lock_*.json

# Count active instances
ls /tmp/ln2t_tools_locks/ | wc -l
```
