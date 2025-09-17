- FB to figure out VSCode fun
- FB to README and think, "Is there enough information here that I can understand what to run to generate the files within 20 minutes?"
    - if the answer is no, what do we need to add
    - relationship between this repo and https://github.com/PCMDI/input4MIPs_CVs
        - this repo pulls information from the 'source ID' fine in input4MIPs_CVs,
          this file: https://github.com/PCMDI/input4MIPs_CVs/blob/main/CVs/input4MIPs_source_id.json
        - in there, it is looking for keys like 'CR-*', to make sure that the 'source ID'
          (think unique ID) we use is 'registered'/known to in input4MIPs_CVs
        - the trick we play is that we can point to a specific commit or branch of input4MIPs_CVs,
          and then this repo is still happy.
        - the idea of input4MIPs_CVs is make sure that the wider forcings team is aware of what is coming
          and can manage some of the metadata around all of these different contributions
          (that come from different people)
        - we write the files using this information, so we can't really get it wrong
          but doing it this way means this metadata is defined in one spot,
          so it's a bit easier to manage

- uploading to NERSC
    - raw docs are pretty good: https://docs.nersc.gov/services/scp/
    - command is something like `scp -r src zrjn@dtn01.nersc.gov:/global/u2/z/zrjn/`
        - `-r`: recursive i.e. upload the folder and its structure
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

Next steps:

1. I'll leave this running on my machine
1. FB to see if you can get the `create-latest...` script to run on your machine
1. Tomorrow, we'll do the upload steps and pre-publish steps together
    - if you want to try uploading in advance, that may save us some time
    - the thing to upload will be `output-bundles/0.1.0/data/processed/esgf-ready/input4MIP`
