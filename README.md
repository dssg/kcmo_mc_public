Evaluating the Effectiveness and Equity of Court Interventions to Reduce Involvement with the Criminal Justice System
=========================================================================================================================
Repository for DSSG 2022 for Kansas City, MO Municipal Courts Project.

# Background

KCMO-MC is the 16th Judicial Circuit court of Missouri and deals with low level ordinance violations, such as trespassing and petty theft, with probation being the most common sentence issued by the court. However, the probation terms assigned to probationers, such as community service and anti-theft classes, are often left incomplete and a large number of probationers subsequently return to the court with new cases. In this project we aimed to help the court develop mechanisms to evaluate of the outcomes and effectiveness of their interventions in order to reduce individuals' future involvement with the criminal justice system. 

Our approach to tackling this issue was two-fold: 1) setting up an infrastructure that allows the court to experiment with various probation conditions to test the effectiveness of their practices, and 2) building a machine learning pipeline that makes it possible to compare outcomes of pilot programs across different risk groups to evaluate the equity of the program. Here, we focus on predicting the risk of individuals receiving low intensity probation sentences returning to the court with a new case. Together, these components make it possible for the court to determine which interventions work best for which individuals (or alternatively, do not work), and make the necessary adjustments to improve outcomes for the individuals in the system to increase probation completion rates and decrease recidivism. 
This repository hosts the code for loading and cleaning data, and training and evaluating the models.

# Requirements
To use the `KCMO-MC` repository, you will need:

- [Python](https://www.python.org/) = 3.10
- [PostgreSQL](https://www.postgresql.org/) = 14.3

## Create a virtual environment
Once the requirements above are installed and you have cloned the repository from Git follow the steps outlined below to set up and activate a Python virtual environment with the required configuration. 

1. We recommend starting with a new python virtual environment and pip installing the requirements needed:

```bash
$ pip install virtualenv
$ virtualenv kcmo-mc
$ source ./kcmo-mc/bin/activate
$ pip install -r requirements.txt
```

## Set the PYTHONPATH
Modify the default search path for module files:
```bash
$ export PYTHONPATH=$PWD
```

## Set enviornmental variables
Modify the enviornmental variables to include your database credentials:
```bash
$export PGUSER=<username>
$export PGPASSWORD=<database_password>
$export PGHOST=<database_host_address>
$export PGDATABASE=<database_name>
$export PGPORT=5432
```

# Data Loading (ETL)
For an overview of the data used for this project and how to load it, see [data.md](data.md)

# Pipeline 
## Running the pipeline
The entire process of training and evaluating the models can be executed by navigating to 'src/pipeline' and running the 'main.py' file:
```bash
$ cd src/pipeline
$ python main.py
```

### Configuration of Modeling Experiments
Before runing the pipeline, edit the configuration file [config/config.yaml](config/config.yaml) to specify the desired cohort, time-splitter, feature, label, and modeling configurations for the experiment:

- Cohort: Predictions are assumed to be made daily based on the court docket list, and includes individuals who are given a suspended imposition of sentence (SIS) probation for non-violent charges on that day. The cohort specification can be changed in the file [src/pipeline/cohort/test_cohort.sql]( src/pipeline/cohort/test_cohort.sql).

- Time splitter: Specify the earliest and oldest available dates for training the model, the label window and unit (e.g., 12 months), the training window (e.g., 3 years), the model update frequency (e.g., 3 months), and the maximum number of splits.  Due to changes in the court’s data storage practices and updates in definitions and codes of statute ordinances, we recommend that the oldest available date set for training is no earlier than 2012.

- Feature configuration: Currently, the available feature groups include violation events history, probation events history, charge related features, demographics, and day counts since various events (e.g., days since last case). For violation events and probation events related features, specify intervals of interest as well as the date of interest (e.g., violation date). 

- Models: A baseline model that uses the number of cases as the only predictor, decision trees, logistic regression, random forests, TabNet deep neural network for structured and tabular data, and boosted forests are the current model types run by the pipeline. The modeling_config section of the config.yaml file contains relevant hyperparameter specifications for each model type.

- Note: For a quick run using two timesplits and simple model configurations, update the 'read_yaml' function in [src/utils.py](src/utils.py) to use [config/config_simple.yaml](config/config_simple.yaml)

## Modeling results, evaluation, and model selection

The pipeline saves modeling results to the database and server, and these tables are read into the modeling selection pipeline.  Currently the best models by AUC and precision at the top 10% (representing the highest risk defendants) are processed in the post modeling. The full model selection process can be found in the model_selection.ipynb notebook.


### Post-Modeling

The model_inspection.ipynb notebook contains code for post-modeling inspection of a selected model. To use the notebook, you will need to inter the model_id for the model you’re interested in inspecting into the ‘Input’ section. Running the notebook will produce:

1. Precision-recall curves
2. Score distribution plot
3. Feature importance plot
4. Cross Tab report
5. Bias audition on race and sex using aequitas


### Jupyter Notebooks and Descriptives

The code used to create the bar charts and compute various statistics about probation term outcomes is contained in the following two Jupyter notebooks in the notebooks folder:	

SIS_SES_probation_term_outcomes.ipynb

race_probation_outcomes.ipynb

In order to run these notebooks, first open the SIS_SES_probation_term_outcomes.ipynb notebook, uncomment the SQL queries and run in a database tool such as DBeaver to create the ordvpost2012 table. Once this table is created, either notebook can be run to generate the statistics of interest as well as the plots. 


