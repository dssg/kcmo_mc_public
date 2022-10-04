# Python script to load data
# Example: load_data.py court raw
# (uploads court data csvs to schema raw in the database and at
#  the file path defined in constants.py)


# Import neccesary libraries
import numpy as np
import pandas as pd
import sqlalchemy
import ohio.ext.pandas
import pathlib
import argparse
import logging
from ast import Str

# Imports from our project
import src.utils.constants as constants
from src.utils.file_processing import tidy_name

# Set up logging
logging.basicConfig(filename="data_loading.log", filemode="w", level=logging.INFO)

# Take in arguments for data type and schema to upload
# import sys
# print(sys.argv)
parser = argparse.ArgumentParser(description="Load one type of data to one schema.")
parser.add_argument(
    "data", type=str, default="court", help="name of the data, either ccl or court"
)
parser.add_argument("schema", type=str, default="tmp_raw", help="name of the schema")
args = parser.parse_args()
logging.info("args.data: %s", args.data)
logging.info("args.schema: %s", args.schema)

# Check if schema exists and create otherwise
# Start engine connection
engine = sqlalchemy.create_engine(constants.ENGINE_PATH)
inspector = sqlalchemy.inspect(engine)
if args.schema not in inspector.get_schema_names():
    with engine.connect() as conn:
        with conn.begin():
            conn.execute('Set role "kcmo-mc-role";')
            conn.execute("CREATE SCHEMA " + args.schema + ";")


# IF CCL DATA
if args.data == "ccl":

    # Grab appropriate paths
    ccl_paths = constants.CCL_PATHS
    xls = [pd.ExcelFile(ccl_path) for ccl_path in ccl_paths]

    # Inspect sheet names, check match
    sheet_names = xls[0].sheet_names
    logging.info(["There are ", len(sheet_names), " sheets in the file"])
    logging.info("Sheet_names: %s", sheet_names)
    for xl in xls:
        if xls[0].sheet_names != xl.sheet_names:
            logging.warning("Excel files have different sheets")

    # For each sheet name, combine components across the excel files
    for sheet_name in sheet_names:
        logging.info("Now on sheet named: %s", sheet_name)
        # For each ccl_path, get the corresponding dataframe
        df_list = [xl.parse(sheet_name) for xl in xls]
        # the following is error-handeling module for xls that fail to upload
        try:
            df = pd.concat(df_list).reset_index()
        except Exception as e:
            logging.error("ERROR IN DATAFRAME CONCAT: %s", e)

        # Update the column names
        df.columns = [tidy_name(c) for c in df.columns.values.tolist()]
        # Create clean sheet name
        clean_sheet_name = tidy_name(sheet_name)

        # Display cleaned sheet name and dataframe
        logging.info("Cleaned sheet name: %s", clean_sheet_name)

        # Upload combined dataframe to database as sql table using Ohio
        with engine.connect() as conn:
            with conn.begin():
                conn.execute('Set role "kcmo-mc-role";')
                df.pg_copy_to(
                    schema=args.schema,
                    name=clean_sheet_name,
                    con=conn,
                    index=False,
                    if_exists="replace",
                )

# IF COURTS DATA
elif args.data == "court":

    def get_filename(path):
        # Function to create a list with names of all files in a folder
        # list to store files
        files = []
        # Grab the full filepath for every csv in the directory and its subdirectories
        rootdir = pathlib.Path(path)
        files = [f for f in rootdir.glob("**/*") if f.is_file() and f.suffix == ".csv"]
        return files

    def mass_upload(files):
        # Function to upload court files in bulk
        for file in files:
            logging.info("Uploading %s", file)
            try:
                clean_csv_name = tidy_name(file.name)
                try:
                    # Make everything a string so that all of the column types
                    # are the same between files of the same table
                    df = pd.read_csv(file, dtype=str)
                except Exception as e:
                    logging.error("Failed on %s", clean_csv_name)
                    logging.error("Error message %s", e)
                    logging.info("Uploading file skipping lines %s", file)
                    df = pd.read_csv(file, on_bad_lines="skip", dtype=str)

                df.columns = [tidy_name(c) for c in df.columns.values.tolist()]
                # Copy the dataframe to postgres (to the right schema and tablename)
                with engine.connect() as conn:
                    with conn.begin():
                        conn.execute('Set role "kcmo-mc-role";')
                        df.pg_copy_to(
                            schema=args.schema,
                            name=f"{clean_csv_name}",
                            con=conn,
                            index=False,
                            if_exists="replace",
                        )
            except Exception as e:
                logging.error("Failed completely to upload file: %s", file)
                logging.error("Error message: %s", e)

    # Get path from constants.py
    # get_path()
    path = constants.COURT_PATH
    files = get_filename(path)
    mass_upload(files)

else:
    print("Invalid data argument: must be either 'ccl' or 'court'")
    print("Use -h flag to get info on argument options.")
