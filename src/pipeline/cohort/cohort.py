# COHORT CREATION
# FIRST PART OF PIPELINE!

# Load in packages 
import pandas as pd 
import logging
from src.utils.sql_utils import *

def cohort_builder(conn, start_date, end_date, config, schema_name):
	""" 
	Append cohort ids and dates to the cohort table.

	Args:
		conn (SQLAlchemy Connection): Connection to the database established beforehand
		dates (List): List of dates 
		config (dictionary): dictionary containing configuration for cohort building,
							 especially path to appropriate filtering sql file 
		schema_name (string): schema to store the table in 
	"""

	# Get query template from file
	query_template = ""
	with open(config['cohort_sql_path'], "r") as f:
		query_template = f.read()

	# Fill in the sql query with 
	table_query = query_template.format(start_date = start_date, end_date = end_date)
	logging.debug(table_query)
		
	create_table(conn, schema_name, config['cohort_table_name'], table_query)

	# Logging and debugging of query result
	query = f"select * from {schema_name}.{config['cohort_table_name']};"
	df = pd.read_sql(query,conn)
	logging.info(f"Cohort added onto, saved in {schema_name}.{config['cohort_table_name']}")
	logging.info("Records now included in cohort: %s", df.shape[0])
	logging.info(df)


