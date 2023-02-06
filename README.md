# Pushing changes to GH
    '''
    1. manually select file to stage
         or
       "git add ITR\data\template.py"
         or (stage everything)
       "git add -A"
    2) git commit -m "Adding fields for bonds and ETFs to indicators_list.py"
    3) git push
    '''

# check url or remote: 
git config --get remote.origin.url

# update local content from Github: 
git pull origin main

# Virtual Environments for Windows:
## Conda:
  View all our virtual environments: conda env list 
  Create virtual environment: conda create -n alpaca_env 
  Activate environment: conda activate alpaca_env 
      In order to be able to install packages inside the environment, we need to instal pip inside the environment: conda install pip 
          If the above didn't work, try like this: go to base environment and do "conda install -n alpaca_env python"
      Install needed package: pip install -r requirements.txt
      Check which package are installed inside the environment: conda list 
  Exit the environment: conda deactivate 
  Delete the environment: conda env remove -n alpaca_env

  I got a problem when in activated environment in VS Code I do "shift+Enter" (Interactive Window) for a specific line of code...
  ... it doesn't execute it in selected environment, but in "base", where needed packages are NOT installed.
  To solve this: 
    Option (A):
      (1) Press Ctrl + Shift + P and select Terminal Configuration
      (2) Search for "python.conda", and paste your conda path: "C:\ProgramData\Anaconda3\Scripts\conda.exe"
    Option (B):
      (1) Go to Anaconda Prompt; 
      (2) Do "conda activate alpaca_env"; 
      (3) Do "code" - this will open VS Code in alpaca_env; 
      (4) Now Shift+Enter will send a command to alpaca_env
