# LABEL CREATION
# Load in packages 
import pandas as pd 
import logging
from src.utils.sql_utils import *

def create_labels(conn, config, schema_name,cohort_table_name, label_window, label_window_unit):
	""" 
	Create a table with labels

	Args:
		conn (SQLAlchemy Connection): Connection to the database established beforehand
		dates (List): List of cohort dates 
		config (dictionary): dictionary containing configuration for label building,
							 especially path to appropriate filtering sql file 
		schema_name (string): schema to store the table in
		cohort_table_name: The cohort table for which labels will be created 
	"""

	# Get config parameters
	sql_file_path = config['label_sql_path']
	table_name = config['label_table_name']
	label_window = str(label_window) + " " + label_window_unit
	# Get query template from file
	query_template = ""
	with open(sql_file_path, "r") as f:
		query_template = f.read()

	# Fill in the sql query with 
	table_query = query_template.format(
		schema_name = schema_name, 
		cohort_table_name = cohort_table_name, 
		label_window = label_window)
		
	if check_if_table_exists(get_db_engine(), schema_name, table_name):
		logging.info("Label table already exists, inserting into table")
		insert_into_table(conn, schema_name, table_name, table_query)
	else:
		logging.info(f"Creating label table in {schema_name}.{table_name}")
		create_table(conn, schema_name, table_name, table_query)


