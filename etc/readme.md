### Create Conda enviroment
conda env create -f environment.yml 

### Activate Conda enviroment
conda activate cortex_gen_ai_workplace_policies

### Update Conda enviroment if you change environment.yml
conda deactivate
conda env update --file environment.yml --prune
conda activate cortex_gen_ai_workplace_policies

### Remove Conda enviroment
conda deactivate
conda remove -n cortex_gen_ai_workplace_policies --all