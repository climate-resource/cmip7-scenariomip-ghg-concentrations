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
