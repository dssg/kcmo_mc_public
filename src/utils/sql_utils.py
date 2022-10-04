import os
import logging
from sqlalchemy import create_engine, inspect


def get_db_engine():
    """
    Sets up SQL engine
    """

    # get credentials from environment variables
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    database = os.getenv("PGDATABASE")

    # configure connection to postgres
    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(
            user,
            password,
            host,
            port,
            database,
        )
    )

    return engine


def get_db_conn():
    """
    Connects to the sql database
    Returns db connection
    """

    engine = get_db_engine()

    connection = engine.connect()

    return connection


def create_schema(db_conn, schema_name):
    """Create schema if not exists
    Args:
        db_conn (object): Database connection
        schema_name (_type_): Name of schema that should be created
    """

    query = f"create schema if not exists {schema_name}"

    try:
        db_conn.execute(query)
        logging.info(f"Schema {schema_name} created (if not exists)")
    except:
        logging.info(f"{schema_name} creation failed")


def set_role(db_conn, role_name):
    """Sets the role for the postgres database
    Args:
        db_conn (object): database connection
        role (str, optional): Role name used for database connection. Defaults to 'acdhs-housing'.
    """

    query = f"set role '{role_name}';"

    try:
        db_conn.execute(query)
        logging.info(f"Role set to '{role_name}'")  
    except:
        logging.info("Role setting failed.")


def drop_table(db_conn, schema_name, table_name):
    """Drops sql table if exists
    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name to be dropped
    """
    query = f"drop table if exists {schema_name}.{table_name}"

    try:
        db_conn.execute(query)
        logging.info(
            f"Table {schema_name}.{table_name} dropped (if exists)"
        )  
    except:
        logging.info(f"Failed to drop table {schema_name}.{table_name}")


def check_if_table_exists(db_engine, schema_name, table_name):
    """Checks whether a table with {table_name} exists in schema {schema_name}
    Args:
        db_engine (object): Database engine
        schema_name (str): Schema name
        table_name (str): Table name
    """

    ins = inspect(db_engine)
    exists = ins.dialect.has_table(db_engine.connect(), table_name, schema_name)
    if exists:
        #logging.info(f"Table {schema_name}.{table_name} already exists.")
        pass
    else:
        logging.info(f"Table {schema_name}.{table_name} does not exist yet.")
    return exists


def create_table(db_conn, schema_name, table_name, table_query=None):
    """Creates table if not exists
    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name to be created
        table_query (str, optional): If table_query is specified, it populates the table with query.
        Defaults to generating an empty table
    """
    if table_query is not None:
        query = f"create table {schema_name}.{table_name} as ({table_query});"
    else:
        query = f"create table {schema_name}.{table_name} ();"

    try:
        db_conn.execute(query)
        # logging.info(f"Table {schema_name}.{table_name} has been created")  
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logging.info(message)
        logging.info(f"Failed to create table {schema_name}.{table_name}")


def insert_into_table(db_conn, schema_name, table_name, table_query):
    """Inserts into existing table
    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name to be inserted into
        table_query (str): SQL query that will be inserted into table.
    """

    query = f"insert into {schema_name}.{table_name} ({table_query});"

    try:
        db_conn.execute(query)
        # logging.info(f"SQL query has been inserted into {schema_name}.{table_name}") 
    except:
        logging.info(f"Failed to insert sql query into table {schema_name}.{table_name}")


# Attempt at modifying function to ignore duplicate entries -- save as reference -Wesley
# def insert_into_table_if_nonduplicate(db_conn, schema_name, table_name, table_query):
#     """Inserts into existing table
#     Args:
#         db_conn (object): Database connection
#         schema_name (str): Schema name
#         table_name (str): Table name to be inserted into
#         table_query (str): SQL query that will be inserted into table.
#     """

#     query = f"insert into {schema_name}.{table_name} ({table_query}) ON CONFLICT DO NOTHING;"

#     try:
#         db_conn.execute(query)
#         #logging.info(f"SQL query has been inserted into {schema_name}.{table_name}")  # change to logging
#     except Exception as e:
#         logging.info(f"Failed to insert if non-duplicate sql query into table {schema_name}.{table_name}")
#         logging.info(e)


def create_index(db_conn, schema_name, table_name, index_columns):
    """Function to set the index columns in a SQL DB
    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name
        index_columns (list): List of column names
    """

    if not isinstance(index_columns, list):
        raise TypeError("Expected list for index_columns")

    if len(index_columns) > 1:
        query = (
            f"create index on {schema_name}.{table_name}({', '.join(index_columns)});"
        )
    else:
        query = f"create index on {schema_name}.{table_name}({''.join(index_columns)});"

    try:
        db_conn.execute(query)
        logging.info(
            f"Created index for column(s) {index_columns} in table {schema_name}.{table_name}"
        )  # change to logging
    except:
        logging.info(
            f"Failed to create index for column(s) {index_columns} in table {schema_name}.{table_name}"
        )
