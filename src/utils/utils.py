# UTILS.py

from ssl import PROTOCOL_TLSv1
import yaml
import git
import os
import json
import pandas as pd
import numpy as np
import ohio.ext.pandas
import datetime
import hashlib
import seaborn as sns 
import sklearn.metrics as sk
import matplotlib.pyplot as plt
from sqlalchemy import dialects
import logging


def read_yaml(): 
    """
    Reads the project yaml file.  Neccesary to manually specify config filepath here.

    Returns:
        (dictionary (of dictionaries)): configuration for the entire pipeline
    """
    # For future pathname robustness, consider adding this line
    # print("dirname ", os.path.dirname(__file__)) 
    filepath = "config/config.yaml"
    with open(filepath, 'r') as yaml_file:
        yaml_f = yaml.safe_load(yaml_file)
    return yaml_f

def deterministic_hash(object):
    ''' Calculate hash in a deterministic way using md5.  Requires input to be 
    in a binary format, so we make the object a string and encode as utf-8.
    '''
    return hashlib.md5(str(object).encode('utf-8')).hexdigest()

def run_id_to_db(run_id, conn, schema_name, table_name):
    # Get git repository and hash
    repo = git.Repo(search_parent_directories=True)
    sha = repo.head.object.hexsha
    # Get user who is running
    run_user_id = os.getenv("PGUSER")

    run_id_df = pd.DataFrame({'run_id': [run_id],
                        'run_user_id': [run_user_id],
                        'git_sha': [sha]})
    
    with conn.begin():
        run_id_df.pg_copy_to(schema=schema_name, name = table_name, con = conn, index = False, if_exists = 'append')
                           



def get_model_set_id(yaml_config, model_type, model_hyperparams):
    # Creates a model set id by hashing the inputs

    time_splitter_string = json.dumps(yaml_config['time_splitter_config'], sort_keys = True )
    hyperparams_string = json.dumps(model_hyperparams, sort_keys = True)

    model_set_id = deterministic_hash((time_splitter_string,
                        yaml_config['cohort_config']['cohort_sql_path'],
                        yaml_config['label_config']['label_sql_path'],
                        yaml_config['feature_config'],
                        model_type,
                        hyperparams_string))

    return model_set_id

def model_set_id_already_run(conn, schema_name, table_name, model_set_id):
    try:
        query = f"""SELECT count(*) as count_star from {schema_name}.{table_name}
                WHERE model_set_id = '{model_set_id}'; """
        with conn.begin():
            df = pd.read_sql(sql=query, con=conn)
            exists = df['count_star'].values[0]
        logging.info(str("Checking if model_set_id already exists, exists is: " + str(exists)))
        if exists == 0:
            return False
        else:
            return True 

    except Exception as e:
        logging.info("model_set_id checking exception: %s", e)
        logging.info(f"{schema_name}.{table_name} does not exist yet, so could not check if model_set_id already exists.")
        return False 
    

def get_model_id(model_set_id, split):
    # Creates a model id by hashing the inputs

    model_id = deterministic_hash((model_set_id, split["train_start_date"],split["train_end_date"],split["val_start_date"],split["val_end_date"]))
    return model_id

def model_id_already_run(conn, schema_name, table_name, model_id):
    try:
        query = f"""SELECT count(*) as count_star from {schema_name}.{table_name}
                WHERE model_id = '{model_id}'; """
        with conn.begin():
            df = pd.read_sql(sql=query, con=conn)
            exists = df['count_star'].values[0]
        logging.info(str("Checking if model_id already exists, exists is: " + str(exists)))
        if exists == 0:
            return False
        else:
            return True 

    except Exception as e:
        logging.info("model_id checking exception: %s", e)
        logging.info(f"{schema_name}.{table_name} does not exist yet, so could not check if model_id already exists.")
        return False 
    


def model_set_to_db(model_set_id, model_type, run_id, model_hyperparams, temporal_params, conn, schema_name, table_name):

    meta_data = pd.DataFrame({'model_set_id': [model_set_id],
                            'run_id': [run_id],
                            'model_type': [model_type],
                            'model_hyperparams': [json.dumps(model_hyperparams)],
                            'temporal_params': [json.dumps(temporal_params)],
                            'started_training_at': datetime.datetime.now()})

    meta_data = meta_data.astype({'model_set_id': str,
                    'model_type': str,
                    'model_hyperparams': str,
                    'temporal_params': str})

    with conn.begin():
        meta_data.pg_copy_to(schema=schema_name, name = table_name, con = conn, index = False, if_exists = 'append',
                             dtype={"temporal_params":dialects.postgresql.JSONB, "model_hyperparams":dialects.postgresql.JSONB})

def splits_to_server(run_id, splits, matrices_dir):
    # add directories if they do not exist
    if not os.path.exists(matrices_dir): 
        os.mkdir(matrices_dir)
    matrices_run_dir = matrices_dir + "/" + str(run_id)
    if not os.path.exists(matrices_run_dir): 
        os.mkdir(matrices_run_dir)
    # for each split, save the matrix and add to the split dictionary the location of the save
    for i, split in enumerate(splits):
        i_str = str(i).zfill(3)
        train_matrix_path = str(matrices_run_dir + "/" + i_str + "_" + 'train' + '.csv')
        split['train_matrix'].to_csv(train_matrix_path, index=False)
        splits[i]['train_matrix_path'] = train_matrix_path
        val_matrix_path = str(matrices_run_dir + "/" + i_str + "_" + 'val' + '.csv')
        split['val_matrix'].to_csv(val_matrix_path, index=False)
        splits[i]['val_matrix_path'] = val_matrix_path


def model_to_db(model_id, model_set_id, split, feature_importances_dict, model_file_path, conn, schema_name, table_name, started_training_at, finished_training_at):
    time_to_train_seconds = (finished_training_at - started_training_at).total_seconds()
    val_size = split['val_matrix'].shape[0]
    val_count_positive = sum(split['val_matrix']['label'].values)
    train_size = split['train_matrix'].shape[0]
    train_count_positive = sum(split['train_matrix']['label'].values)   
    meta_data = pd.DataFrame({'model_id': [model_id],
                              'model_set_id': [model_set_id],
                              'split_num':[split['split_num']],
                              'train_start_date': [split['train_start_date']],
                              'train_end_date': [split['train_end_date']],
                              'val_start_date': [split['val_start_date']],
                              'val_end_date': [split['val_end_date']],
                              'train_size': [train_size],
                              'train_count_positive':[train_count_positive],
                              'val_size': [val_size],
                              'val_count_positive': [val_count_positive],
                              'features': [sorted(list(feature_importances_dict.keys()))],
                              'model_file_path': [model_file_path],
                              'train_matrix_path': [split['train_matrix_path']],
                              'val_matrix_path': [split['val_matrix_path']],
                              'started_training_at': [started_training_at],
                              'completed_training_at': [finished_training_at],
                              'time_to_train_seconds': [time_to_train_seconds]})

    with conn.begin():
        meta_data.pg_copy_to(schema=schema_name, name = table_name, con = conn, index = False, if_exists = 'append')
        # dtype={'features':dialects.postgresql.ARRAY(String())}


def feature_importances_to_db(model_id, feature_importances_dict, conn, schema_name, table_name):
    feature_names, feature_values = zip(*list(feature_importances_dict.items()))
    feature_importances = pd.DataFrame({'model_id': len(feature_names) * [model_id],
                                        'feature_name': feature_names,
                                        'feature_value': feature_values})
    #print(feature_importances.head())
    with conn.begin():
        feature_importances.pg_copy_to(schema=schema_name, name = table_name, con = conn, index = False, if_exists = 'append')



def predictions_to_db(model_id, scores, val_matrix, conn, schema_name, table_name, seed):

    np.random.seed(seed)
    predictions = pd.DataFrame({'model_id': model_id, 'person_id': val_matrix['person_id'], 'as_of_date': val_matrix['cohort_date'],'score': scores,'true_label': val_matrix['label']})
   
    predictions['random_num'] = np.random.uniform(0,1, predictions.shape[0])
    predictions = predictions.sort_values(by = ['score', 'random_num'], axis = 0, ascending=[False,True])
   
    # Add ranking and ranking method column (tiebreaking=random)
    predictions['rank'] = range(predictions['random_num'].shape[0])
    predictions['how_ranked'] = 'random_tie_break'
    
    # take out random_num column
    predictions = predictions.drop('random_num', axis = 1)

    with conn.begin():
       predictions.pg_copy_to(schema=schema_name, name = table_name, con = conn, index = False, if_exists = 'append')

    return predictions
                                

def metrics_to_db(table_name, schema_name, conn, predictions):
    if sum(predictions['true_label'].values) != 0:
        AUC = sk.roc_auc_score(y_true = predictions['true_label'], y_score = predictions['score'])
    else:
        AUC = None
        logging.info("WARNING: Validation set contains zero positive labels, AUC value set to None")
    
    model_id = predictions['model_id'].values[0]
    metrics = pd.DataFrame({'model_id': [model_id],
                            'metric_name': ['AUC'],
                            'metric_param': [None],
                            'value': [AUC]}
                            )
    precision_list = []
    recall_list = []
    for percentile in range(101):
        num_labeled_pos = np.ceil(percentile * predictions.shape[0]/100)
        pred_label = (predictions['rank'] < num_labeled_pos).values.astype(int)
        # Rayid comment: 
        # Deal with ties in different ways -- what is the variance on precision/recall, what is the mean value
    
        # precision = sum((pred_label == predictions['true_label']) & ( pred_label== 1)) / (sum((pred_label == predictions['true_label']) & ( pred_label== 1)) + sum((predictions['true_label'] == 0) & (pred_label == 1)))
        precision = sk.precision_score(y_true = predictions['true_label'], y_pred = pred_label, zero_division = 0)
        precision_list.append(precision)
        recall = sk.recall_score(y_true = predictions['true_label'], y_pred = pred_label, zero_division = 1)
        recall_list.append(recall)
    metrics = pd.concat([metrics, pd.DataFrame({'model_id': 101 * [predictions['model_id'].values[0]],
                                    'metric_name': 101 * ['precision'],
                                    'metric_param': list(range(101)),
                                    'value': precision_list})])
    metrics = pd.concat([metrics, pd.DataFrame({'model_id': 101 * [predictions['model_id'].values[0]],
                                    'metric_name': 101 * ['recall'],
                                    'metric_param': list(range(101)),
                                    'value': recall_list})])   
                    
    with conn.begin():
       metrics.pg_copy_to(schema=schema_name, name = table_name, con = conn, index = False, if_exists = 'append')

    return metrics 


def plot_precision_recall(metrics, model_type, model_hyperparams, location): 
    plt.clf()
    # exclude AUC to get df of only precision recall
    pr_metrics =  metrics[(metrics['metric_name']=='precision') | (metrics['metric_name']=='recall')]
    pr_metrics =  pr_metrics.reset_index(drop = True)
    pr_plot = sns.lineplot(data = pr_metrics, y ='value', x = 'metric_param', hue = 'metric_name')
    pr_plot.set_title(str(model_type + " model_id: " + metrics['model_id'].values[0] + "\n" + str(model_hyperparams)))
    pr_plot.set_xlabel("Percent of population labeled 1")
    name = location + '/' +'precision-recall-' + metrics['model_id'].values[0]+'.png'
    pr_plot.get_figure().savefig(name)