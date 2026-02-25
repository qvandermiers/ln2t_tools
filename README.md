<div align="center">

# ln2t_tools

**Neuroimaging pipeline manager for the [LN2T](https://ln2t.ulb.be/)**

[How it works](#how-it-works) | [Installation](#installation) | [How to use it](#how-to-use-it) | [Supported tools](#supported-tools) | [Using HPC](#using-hpc) | [Bonuses](#bonuses)

</div>

## How it works

`ln2t_tools` is a Command Line Interface software that facilitates execution of standard neuroimaging pipelines. The core principles are:
- Data are supposed to be organized following the [data organization of the lab](https://github.com/ln2t/ln2t_data). It is of utmost importance that you read this before going any further.
- A [selection of standard pipelines](#supported-tools) have been incorporated in `ln2t_tools`.
- Outputs are tagged with pipeline name and version number, following in particular the recommendations of the [data organization of the lab](https://github.com/ln2t/ln2t_data) (I said read it!).
- The syntax of `ln2t_tools` is as follows:
  ```bash
  ln2t_tools <pipeline_name> --dataset <dataset_name> [options]
  ```
  There are two classes of options: those belonging to `ln2t_tools` and those that are directly passed to the pipeline; see below for more details and examples.
- By default, the processing is done on the local machine, but `ln2t_tools` can be used to send the work to High-Performance Computing (HPC) Clusters such as [CECI](https://www.ceci-hpc.be/); more info in the [corresponding section](#using-hpc).
- More pipelines can be added easily thanks to the modular architecture of `ln2t_tools`, but this is not meant to be done by the standard end-user.

If you are interested in using this tool in your lab, make sure to read this documentation in full.

## Installation

If you are working on a computer of the lab, the software is already installed and you can skip this section.

To install `ln2t_tools`, we strongly recommend you use a python virtual environment:
```bash
git clone git@github.com:ln2t/ln2t_tools.git
cd ln2t_tools
python -m venv venv
source venv/bin/activate
pip install -U pip && pip install -U .
```

After installation, you can enable bash completion tools using
```bash
echo "source ~/.local/share/bash-completion/completions/ln2t_tools" >> ~/.bashrc
```
and then source `~/.bashrc`. More details in the [How it works](#how-it-works) section.

`ln2t_tools` assumes a standardized folder structure to access raw data and to save outputs; see the [data organization rules of the lab](https://github.com/ln2t/ln2t_data) for more details.

Moreover, `ln2t_tools` uses [Apptainer](https://apptainer.org/) containers to run neuroimaging pipelines. This ensures reproducibility and eliminates dependency conflicts.

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

Finally, several pipelines require a valid license file (free academic license available at [FreeSurfer Registration](https://surfer.nmr.mgh.harvard.edu/registration.html)).

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
will show you the [supported tools](#supported-tools), e.g. `freesurfer` or `fmriprep`. Once the pipeline name is auto-completed, you can just continue using `<TAB>` to see what must be provided:
```bash
ln2t_tools freesurfer <TAB>  # auto-completes the next argument, in this case '--dataset'
```
This will auto-complete the mandatory argument `--dataset`. Further `<TAB>` presses will show you the datasets you have access to:
```bash
ln2t_tools freesurfer --dataset <TAB>  # show you the available datasets
```
The dataset names are set according to the [data organization rules of the lab](https://github.com/ln2t/ln2t_data). But you read it already twice, I know.

**Example: `FreeSurfer`**

To run `FreeSurfer` on the dataset `2020-Big_Dog-e7765b3826bd`, you can use
```bash
ln2t_tools freesurfer --dataset 2020-Big_Dog-e7765b3826bd
```
Pressing enter will:
- select the default version for `FreeSurfer`
- check that it is installed - if not, download and install it
- check that the dataset exists and discover the available subjects
- for each subject, check if `FreeSurfer` has already been run*
- launch sequentially the remaining subjects
To understand where to find the outputs, make sure to read the [data organization rules of the lab](https://github.com/ln2t/ln2t_data) (the what?!).

* *Warning*: this check is very basic, as it simply test if the participant output folder exists. Any left-over from a previous run, even if incomplete, will not be diagnosed by `ln2t_tools`.

**Important Notice:**

We **do not** recommend that you launch a tool on a whole dataset at once. Start first on a few subjects - for this, you can use the `ln2t_tools` option `--participant-label`:
```bash
ln2t_tools freesurfer --dataset 2020-Big_Dog-e7765b3826bd \
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

Note that there are other tools. Some are only for administrators or simply under development. Moreover, if you have a specific need, you might reach out to us (or open an issue, we're on GitHub after all).

## Using HPC

High Performance Computation (HPC) Clusters are sometimes more suited to run large number of large tasks. If you are from our lab, you can request access to the [CECI](https://www.ceci-hpc.be/), a network of clusters in Belgium. While we recommend you read their nice documentation, it is not necessary to master all aspects of job specification, submission and monitoring as most of the tasks are fully automatized by `ln2t_tools`. The only set-up you have to do is to install SSH keys to allow `ln2t_tools` to access the clusters. For this, follow the corresponding page on the [CECI documentation](https://support.ceci-hpc.be/doc/QuickStart/ConnectingToTheClusters/FromAUnixComputer/).

Once this is done, you may directly attempt the following command:
```bash
ln2t_tools <pipeline> --dataset <dataset> --hpc --hpc-host <host> --hpc-user <user> --hpc-time 10:00:00
```
The `<host>` is the name of the cluster you are willing to submit your jobs (typically: `lyra`, at least if you followed the official documentation). The username is your cluster username (provided by the CECI admins). Finally, the (optional) argument `--hpc-time` sets the required time for your job (default value is `02:00:00`). More options can be passed in HPC mode; please see the output of `ln2t_tools <pipeline> --dataset <dataset> --hpc --help` for more details.

The above command will:
1. Ensure that `ln2t_tools` can connect to the clusters using your SSH key.
2. Verify that the required pipeline is installed on the cluster. If not, `ln2t_tools` will ask you if you want to submit a job to install the software.
3. Check that the data are available on the clusters. If not, `ln2t_tools` will give you the possibility to upload them automatically.
4. Once all jobs are submitted, you will have to wait for them to be completed. This depends on many parameters (cluster workload, queue length, required resources, etc).
5. `ln2t_tools` will suggest a command line to retrieve the outputs from the clusters and store them exactly as a local run of `ln2t_tools` would have done. You can copy/paste this command (and, potentially, tune it to your specific needs).

Please keep in mind that this functionality is still under testing, but it should work for `FreeSurfer`, `fMRIPrep`, `QSIPrep` and `QSIRecon`.

**Note:**

- You can check job logs on the clusters, at your home directory.
- For storage space reasons, paths on the clusters differ from the local structure. In short, most of the paths pointing to your home directory are replaced, on the clusters, by the variable `GLOBALSCRATCH`.
- Check `ln2t_tools` help for further functionality like remote job monitoring, etc.

## Bonuses

### Tool-Specific Arguments with --tool-args

`ln2t_tools` uses a pass-through argument pattern for tool-specific options. This allows the tools to be updated independently of `ln2t_tools`, and gives you access to the full range of options each tool supports.

Core arguments (dataset, participant, version, HPC options) are handled by `ln2t_tools`. Tool-specific arguments are passed verbatim to the container using `--tool-args`:

```bash
ln2t_tools <tool> --dataset mydataset --participant-label 01 --tool-args "<tool-specific-arguments>"
```
For instance, here's what you should do to use `qsiprep` with the (mandatory) argument `--output-resolution`:
```bash
ln2t_tools qsiprep --dataset mydataset --participant-label 01 \
    --tool-args "--output-resolution 1.5"
```

### Finding Missing Participants

The `--list-missing` flag helps identify which participants in your dataset still need processing for a specific tool. This is useful when:
- Resuming incomplete pipelines after errors
- Managing large cohorts with multiple tools
- Generating copy-paste commands to process missing participants

## Advanced Documentation

For advanced topics and detailed guides, see the [docs/](docs/index.md) folder:

- [Data Import Guide](docs/data_import.md) - Convert raw neuroimaging data to BIDS format (DICOM, MRS, Physio, MEG)
- [Adding a New Tool](docs/adding_new_tool.md) - Develop and integrate new neuroimaging pipelines with custom Apptainer recipes
- [Instance Management](docs/instance_management.md) - Control parallel processing limits and resource usage
- [MELD Graph Setup](docs/meld_graph.md) - Advanced configuration for focal cortical dysplasia detection
