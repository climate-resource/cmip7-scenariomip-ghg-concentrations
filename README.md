# CMIP7 ScenarioMIP GHG Concentrations

Generation of GHG concentration inputs (i.e. forcings) for CMIP7's ScenarioMIP.

<!--

We recommend having a status line in your repo to tell anyone who stumbles
on your repository where you're up to. Some suggested options:

- prototype: the project is just starting up and the code is all prototype
- development: the project is actively being worked on
- finished: the project has achieved what it wanted and is no longer being
  worked on, we won't reply to any issues
- dormant: the project is no longer worked on but we might come back to it, if
  you have questions, feel free to raise an issue
- abandoned: this project is no longer worked on and we won't reply to any
  issues

-->

## Status

- development: the project is actively being worked on

## Installation

We do all our environment management using [pixi](https://pixi.sh/latest).
To get started, you will need to make sure that pixi is installed
([instructions here](https://pixi.sh/latest),
we found that using the pixi provided script was best on a Mac).

To create the virtual environment, run

```sh
pixi install
pixi run pre-commit install
```

These steps are also captured in the `Makefile` so if you want a single
command, you can instead simply run `make virtual-enviroment`.

Having installed your virtual environment, you can now run commands in your
virtual environment using

```sh
pixi run <command>
```

For example, to run Python within the virtual environment, run

```sh
pixi run python
```

As another example, to run a notebook server, run

```sh
pixi run jupyter lab
```

## Creating the files

### Process

#### In short

1. Receive data from the emissions team
1. Update `scripts/create-latest-set-of-concentration-files.sh`
    - Likely you will need to update `--emissions-file`, `--run-id`, `--esgf-version` and `--input4mips-cvs-source`
1. Commit
1. Start your prefect server in a separate terminal, `pixi run prefect server start`
1. Run
1. [TODO: update] Send data to the publication team using `scripts/upload-to-llnl.py`

#### In long

[TODO: update]
1. Receive markers from the emissions team
    - the markers are defined in `scripts/generate-concentration-files.py`.
      If there are changes, make sure you update this variable.
1. Receive emissions from the emissions team
    - they should send two files.
      They produce these files with the script
      [here](https://github.com/iiasa/emissions_harmonization_historical/blob/main/scripts/extract-for-ghg-concs.py).
      The two files are:
        1. the emissions for each scenario,
           except for emissions of species
           that we derive from our inversions of sources like WMO (2022)
           (where we use only a single concentration projection,
           rather than having variation across scenarios)
        1. emissions for each scenario at the fossil/biosphere level.
           This is used for some extrapolations of latitudinal gradients.
           It's the same data as above, just at slightly higher sectoral detail.
1. Put the received emissions in `data/raw/input-scenarios`
1. Update the emissions file you use for your run.
   There are two options for how to do this:
    1. specify this from the command line via the `--emissions-file` option
    1. change the value of the `emissions_file` variable in `scripts/generate-concentration-files.py`
1. Run with a new run ID and ESGF version (using the command line argument `--run-id` and `--esgf-version`).
   Pick whatever makes sense here (we don't have strong rules about our versioning yet)
    - This will also require creating entries for the controlled vocabularies (CVs).
      This requires updating [this file](https://github.com/PCMDI/input4MIPs_CVs/blob/main/CVs/input4MIPs_source_id.json)
      to include source IDs of the form "CR-scenario-esgf-version".
      In practice, simply copy the existing "CR-scenario-esgf-version"
      entries and update their version to match the ESGF version you used above.
      Then push this to GitHub.
    - When you run, you will need to update the value of `--input4mips-cvs-source`.
      You can do this either
      via the command-line argument `--input4mips-cvs-source`
      or just update the value in `scripts/generate-concentration-files.py`.
      The value should be of the form `"gh:[commit-id]"`
      e.g. `"gh:c75a54d0af36dbedf654ad2eeba66e9c1fbce2a2"`.
1. When the run is finished, upload the results for the publication team with
   `pixi run python scripts/upload-to-llnl.py --unique-upload-id-dir <unique-value-here> output-bundles/<run-id>/data/processed/esgf-ready/input4MIPs`
   e.g. `pixi run python scripts/upload-to-llnl.py --unique-upload-id-dir cr-scenario-concs-20250701-1 output-bundles/v0.1.0a2/data/processed/esgf-ready/input4MIPs`
1. Tell the publication team that the results are uploaded and the folder in which to find them i.e. the value of `--unique-upload-id-dir`

#### Uploading to Nersc
- raw docs are pretty good: https://docs.nersc.gov/services/scp/
- command is something like `scp -r output-bundles/0.1.0/data/processed/esgf-ready/input4MIPs zrjn@dtn01.nersc.gov:/global/u2/z/zrjn/`
    - `-r`: recursive i.e. upload the folder and its structure
    - `output-bundles/0.1.0/data/processed/esgf-ready/input4MIPs`: the directory you want to upload
    - `zrjn`: zeb's username, yours will be something like fb.
      You can get this by logging into jupyter then looking at the start of your shell.
    - `dtn01.nersc.gov:`: where we want to upload to
      get this from the docs https://docs.nersc.gov/services/scp/
    - `/global/u2/z/zrjn/`: the path we want to upload to. This is just my home directory
- move files to `/global/cfs/projectdirs/m4931/zrjn-tmp`, the 'staging' area effectively
- update permissions
    - make all directories readable by anyone: `find /global/cfs/projectdirs/m4931/zrjn-tmp/input4MIPs/ -type d -exec chmod 755 {} \;`
    - make all files readable by anyone: `find /global/cfs/projectdirs/m4931/zrjn-tmp/input4MIPs/ -type f -exec chmod 644 {} \;`
- send an email to Sasha (I'll give you email separately) to say, "Hi, these files are ready to be published"

#### Parallelisation

By default, this all runs serially.
You can add extra cores with the flags below:

- `--n-workers`: the number of threaded (i.e. parallel) workers to use for submitting jobs
    - note: this doesn't result in true parallelism. A full explanation is beyond the scope of this document
      (but if you want to google, explore the difference between multiprocessing with threads compared to processes in python)
- `--n-workers-multiprocessing`: the number of multiprocessing (i.e. parallel) workers to use, excluding any tasks that require running MAGICC
- `--n-workers-multiprocessing-magicc`: the number of multiprocessing (i.e. parallel) workers to use for tasks that run MAGICC
- `--n-workers-per-magicc-notebook`: the number of MAGICC workers to use in each MAGICC-running task.
    - note: the total number of MAGICC workers is the product of `--n-workers-multiprocessing-magicc` and `--n-workers-per-magicc-notebook`

In general, you want:

- `--n-workers`: equal to the number of cores on your CPU (or more)
- `--n-workers-multiprocessing`: equal to the number of cores on your CPU (or more)
- `--n-workers-multiprocessing-magicc`, `--n-workers-per-magicc-notebook`: the product should be equal to equal to the number of cores on your CPU (or more)

For example, for an eight core machine you might do something like

```sh
pixi run python scripts/generate-concentration-files.py --n-workers 8 --n-workers-multiprocessing 8 --n-workers-multiprocessing-magicc 2 --n-workers-per-magicc-notebook 4
```

#### Specific gases

If you need/want to run only for a specific gas, you can use the `--ghg` flag as shown below.

```sh
pixi run python scripts/generate-concentration-files.py --ghg ccl4 --ghg cfc113
```

## Development

TODO: update this section as we add:

- tests
- anything else

Install and run instructions are the same as the above
(this is a simple repository,
without tests etc. so there are no development-only dependencies).

### Contributing

TODO: update as we figure out the structure

### Repository structure

TODO: update as we figure out the structure

We have a basic `Makefile` which captures key commands in one place
(for more thoughts on why this makes sense, see
[general principles: automation](https://gitlab.com/znicholls/mullet-rse/-/blob/main/book/general-principles/automation.md)).
For an introduction to `make`, see
[this introduction from Software Carpentry](https://swcarpentry.github.io/make-novice/).
Having said this, if you're not interested in `make`, you can just copy the
commands out of the `Makefile` by hand and you will be 90% as happy.

### Tools

In this repository, we use the following tools:

- git for version-control (for more on version control, see
  [general principles: version control](https://gitlab.com/znicholls/mullet-rse/-/blob/main/book/theory/version-control.md))
    - for these purposes, git is a great version-control system so we don't
      complicate things any further. For an introduction to Git, see
      [this introduction from Software Carpentry](http://swcarpentry.github.io/git-novice/).
- [Pixi](https://pixi.sh/latest/) for environment management
  (for more on environment management, see
  [general principles: environment management](https://gitlab.com/znicholls/mullet-rse/-/blob/main/book/theory/environment-management.md))
    - there are lots of environment management systems.
      Pixi works well in our experience and,
      for projects that need conda,
      it is the only solution we have tried that worked really well.
    - we track the `pixi.lock` file so that the environment
      is completely reproducible on other machines or by other people
      (e.g. if you want a colleague to take a look at what you've done)
- [pre-commit](https://pre-commit.com/) with some very basic settings to get some
  easy wins in terms of maintenance, specifically:
    - code formatting with [ruff](https://docs.astral.sh/ruff/formatter/)
    - basic file checks (removing unneeded whitespace, not committing large
      files etc.)
    - (for more thoughts on the usefulness of pre-commit, see
      [general principles: automation](https://gitlab.com/znicholls/mullet-rse/-/blob/main/book/general-principles/automation.md)
    - track your notebooks using
    [jupytext](https://jupytext.readthedocs.io/en/latest/index.html)
    (for more thoughts on the usefulness of Jupytext, see
    [tips and tricks: Jupytext](https://gitlab.com/znicholls/mullet-rse/-/blob/main/book/tips-and-tricks/managing-notebooks-jupytext.md))
        - this avoids nasty merge conflicts and incomprehensible diffs
- [prefect](https://docs.prefect.io/v3/get-started) for workflow orchestration

### General background

- relationship between this repo and https://github.com/PCMDI/input4MIPs_CVs
    - this repo pulls information from the 'source ID' fine in input4MIPs_CVs,
      this file: https://github.com/PCMDI/input4MIPs_CVs/blob/main/CVs/input4MIPs_source_id.json
    - in there, it is looking for keys like 'CR-*', to make sure that the 'source ID' (think unique ID) we use is 'registered'/known to in input4MIPs_CVs
    - the trick we play is that we can point to a specific commit or branch of input4MIPs_CVs, and then this repo is still happy.
    - the idea of input4MIPs_CVs is make sure that the wider forcings team is aware of what is coming and can manage some of the metadata around all of these different contributions (that come from different people)
    - we write the files using this information, so we can't really get it wrong but doing it this way means this metadata is defined in one spot, so it's a bit easier to manage

## Original template

This project was generated from this template:
[basic python repository](https://gitlab.com/openscm/copier-basic-python-repository).
[copier](https://copier.readthedocs.io/en/stable/) is used to manage and
distribute this template.
