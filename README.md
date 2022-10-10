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
  Check available v env: conda info --envs
  Create venv: conda create -n NAMEOFENV
  Activate env: conda activate NAMEOFENV
  Deactivate env: conda deactivate
  Delete env: conda env remove -n NAMEOFENV

  
