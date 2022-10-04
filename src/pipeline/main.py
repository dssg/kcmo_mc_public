
import pandas as pd
import logging
import joblib
from itertools import product
import datetime

from src.utils.utils import read_yaml, get_model_set_id, get_model_id, model_set_to_db, model_to_db, predictions_to_db, metrics_to_db, plot_precision_recall, feature_importances_to_db, run_id_to_db, model_id_already_run, splits_to_server, model_set_id_already_run
from src.utils.sql_utils import *
from src.pipeline.timesplit.timesplitter import time_splitter
from src.pipeline.cohort.cohort import cohort_builder
from src.pipeline.labels.label_creator import create_labels
from src.pipeline.features.feature_builder import *
from src.pipeline.model.matrix_creator import add_matrices_to_splits
from src.pipeline.model.modeling import Model

#########
# SETUP #
#########

# load in configuration
yaml_config = read_yaml()
role_name = yaml_config['role_name']
pipeline_schema_name = yaml_config['pipeline_schema_name']
modeling_schema_name = yaml_config['modeling_schema_name']
cohort_table_name = yaml_config['cohort_config']['cohort_table_name']
label_table_name = yaml_config['label_config']['label_table_name']
gen_feature_config = yaml_config['feature_config']
feature_table_names = gen_feature_config['feature_groups']
modeling_config = yaml_config['modelling_config']
random_seed = yaml_config['random_seed']

# start Logging
logging.basicConfig(filename='pipeline.txt', filemode='w', level=logging.INFO) 

############
# PIPELINE #
############

# initialize database connection and schema
conn = get_db_conn()
set_role(conn, role_name)
create_schema(conn, pipeline_schema_name)

# create time splits
time_splitter_config = yaml_config['time_splitter_config']
splits = time_splitter(newest_available_date = time_splitter_config["newest_available_date"], 
                       oldest_available_date = time_splitter_config["oldest_available_date"], 
                       label_window = time_splitter_config["label_window"], 
                       training_window = time_splitter_config["training_window"], 
                       model_update_freq = time_splitter_config["model_update_freq"],
                       max_splits = time_splitter_config["max_splits"])


# create cohort table

drop_table(conn, pipeline_schema_name, yaml_config['cohort_config']['cohort_table_name'])
# For now, we take each date from the oldest to the newest date we possibly want in a time split 
start_date = time_splitter_config["oldest_available_date"] 
end_date = time_splitter_config["newest_available_date"]
cohort_builder(conn, start_date, end_date, yaml_config['cohort_config'], pipeline_schema_name)

# create labels table
drop_table(conn, pipeline_schema_name, label_table_name)
create_labels(conn,  yaml_config['label_config'], pipeline_schema_name, cohort_table_name, time_splitter_config["label_window"], time_splitter_config["label_window_unit"])


# create features tables
build_charges_features_table(conn, pipeline_schema_name)

for feature_group in gen_feature_config['feature_groups']:
    feature_config = gen_feature_config[feature_group]
    if (feature_group == "probation_events_history") | (feature_group == "violation_events_history"):
        build_court_history_feature_table(conn, feature_config, cohort_table_name, pipeline_schema_name, feature_group)
    elif (feature_group == "demographics") | (feature_group == "days_since_last"):
        build_gen_feature_table(conn, feature_config, cohort_table_name,pipeline_schema_name, feature_group)


# build splits and matrices dictionaries
splits = add_matrices_to_splits(conn, pipeline_schema_name, cohort_table_name, label_table_name, feature_table_names, splits)

############
# MODELING #
############

# Initialize database connection and schema
mdl_conn = get_db_conn()
set_role(mdl_conn, role_name)
create_schema(mdl_conn, modeling_schema_name)

###### Model Run Governance #####
run_id = datetime.datetime.now()
run_id_to_db(run_id, mdl_conn, modeling_schema_name, yaml_config['modelling_config']['pipeline_run_table_name'])
splits_to_server(run_id, splits, yaml_config['modelling_config']['matrices_dir'])
#####

# go over all model_types specified in config
for model_type in modeling_config['model_types']:
    all_params = modeling_config['model_params'][model_type]
    all_hyperparams = all_params.get('hyperparameters')
    hyperparam_names = list(all_hyperparams.keys())

    # go over all possible combinations of hyperparameters for the model
    for hyperparam_vals in product(*list(all_hyperparams.values())):
        model_hyperparams = {name: val for name,val in zip(hyperparam_names, hyperparam_vals)}

        # ##### Model Set Governance #####
        # write model set metadata to database
        model_set_id = get_model_set_id(yaml_config, model_type, model_hyperparams)
        if not model_set_id_already_run(mdl_conn, modeling_schema_name, yaml_config['modelling_config']['model_set_metadata_table_name'], model_set_id):
            model_set_to_db(model_set_id, model_type, run_id, model_hyperparams, time_splitter_config,
                            mdl_conn, modeling_schema_name, yaml_config['modelling_config']['model_set_metadata_table_name'])
        # #####

        # run models for each timesplit
        for split in splits:
            model_id = get_model_id(model_set_id, split)
            logging.info(str("Now on model_id " + model_id + " of type " + all_params['model_type_name'] + " with hyperparams " + str(model_hyperparams) + " with train_end_date " + split['train_end_date'] + " :)"))
            if model_id_already_run(conn, modeling_schema_name, yaml_config['modelling_config']['model_metadata_table_name'], model_id):
                # Skip re-running this model if training data, validation data, and all params are the same
                continue
            
            # get train data and validation data
            train_matrix = split['train_matrix']
            val_matrix = split['val_matrix']

            # initialize model 
            # note: we pass in train and val matrix to store them in the model object 
            model = Model(all_params['model_type_name'], model_hyperparams, random_seed, train_matrix, val_matrix)
            # train model
            started_training_at = datetime.datetime.now()
            model = model.train(train_matrix)
            finished_training_at = datetime.datetime.now()
            # calculate feature importances and scores
            model.feature_importances = model.get_feature_importances()
            feature_importances_dict = model.feature_importances
            model.scores = model.predict_score(val_matrix)
            scores = model.scores
            # clean model - remove train and val dataframes 
            model = model.clean_up_time()


            ##### Model Governance #####
            # pickle the model
            pkl_filename = yaml_config['modelling_config']['pkl_dir'] + '/' + str(model_id) + '.joblib'
            with open(pkl_filename, 'wb') as pkl_file:  
                joblib.dump(model, pkl_file)
            # write model metadata to database
            model_to_db(model_id, model_set_id, split, feature_importances_dict, pkl_filename, mdl_conn,
                         modeling_schema_name, yaml_config['modelling_config']['model_metadata_table_name'],
                         started_training_at, finished_training_at)
            # write model feature imoprtances to database 
            feature_importances_to_db(model_id, feature_importances_dict, conn, modeling_schema_name, yaml_config['modelling_config']['feature_importances_table_name'])
            # write predictions to database, and return predictions dataframe
            predictions = predictions_to_db(model_id, scores, val_matrix, conn, modeling_schema_name, yaml_config['modelling_config']['predictions_table_name'], random_seed)
            # write model evaluation metrics to database, and return metrics dataframe
            metrics = metrics_to_db(yaml_config['modelling_config']['model_metrics_table_name'], modeling_schema_name, conn, predictions)
            # plot precision - recall plot 
            plot_precision_recall(metrics, model_type, model_hyperparams, yaml_config['modelling_config']['precision_recall_dir'],)
            #####






    








    
