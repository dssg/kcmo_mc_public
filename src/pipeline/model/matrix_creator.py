# MATRIX CREATION
# Takes in Cohort, Features and Label tables and joins them

import pandas as pd
import logging
from datetime import datetime

from src.utils.sql_utils import *

def matrix_creator(conn, schema_name, cohort_table_name, label_table_name, feature_table_names, start_date, end_date):
    """ Joins the cohort, label and feature tables

    Args:
        conn (object): database connection
        schema_name (str): sql schema name
        cohort_table_name (str): name of cohort table
        label_table_name (str): name of label table
        feature_table_names (lst): list of feature table names
        start_date (str): earliest cohort date to include in matrix
        end_date (str): last cohort date to include in matrix
    """
    

    # get feature tables selection to insert into query
    feature_cols = []
    # go over all feature tables
    for feature_table_name in feature_table_names:
        col = f"INNER JOIN {schema_name}.{feature_table_name} USING (person_id ,cohort_date)"
        feature_cols.append(col)      
    
    # SQL query to join label and features for cohort within the start and end dates 
    sql_query = '''
        SELECT *
        FROM {schema_name}.{cohort_table_name} c
        INNER JOIN {schema_name}.{label_table_name} l USING (person_id ,cohort_date)
        {feature_cols}
        WHERE cohort_date BETWEEN '{start_date}' AND '{end_date}'
    '''

    #Format the SQL query
    matrix_query = sql_query.format(
		schema_name = schema_name, 
		cohort_table_name = cohort_table_name, 
		label_table_name = label_table_name,
        feature_cols = '\n'.join(feature_cols),
        start_date = start_date,
        end_date = end_date)
    
    #read in the joined data     
    matrix = pd.read_sql_query(matrix_query, conn)

    # for now, drop rows with any nan value
    matrix = matrix.dropna()

    return matrix


def get_matrices(conn, schema_name, cohort_table_name, label_table_name, feature_table_names, split):
    """Creates separate matrices for train and test data 

    Args:
        conn (object): database connection
        schema_name (str): sql schema name
        cohort_table_name (str): name of cohort table
        label_table_name (str): name of label table
        feature_table_name (str): name of feature table
        split (dict): start and end dates for the train and validation data
    """

    train_matrix = matrix_creator(conn, schema_name, cohort_table_name, label_table_name,
     feature_table_names, split['train_start_date'], split['train_end_date'])
    val_matrix = matrix_creator(conn, schema_name, cohort_table_name, label_table_name,
     feature_table_names, split['val_start_date'], split['val_end_date'])
    
    logging.info("train_matrix created. Matrix shape is: %s", train_matrix.shape)
    logging.info("val_matrix created. Matrix shape is: %s", val_matrix.shape)
 
    return (train_matrix, val_matrix)


def add_matrices_to_splits(conn, schema_name, cohort_table_name, label_table_name, feature_table_names, splits):
    for i, split in enumerate(splits):
        train_matrix, val_matrix = get_matrices(conn, schema_name, cohort_table_name, label_table_name, feature_table_names, split)
        splits[i]['train_matrix'] = train_matrix.reset_index(drop=True)
        splits[i]['val_matrix'] = val_matrix.reset_index(drop=True)
        splits[i]['split_num'] = i
    return splits


   
