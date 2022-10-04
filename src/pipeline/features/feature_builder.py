
# FEATURE CREATION

# Load in packages 
import pandas as pd 
import logging
from src.utils.sql_utils import *
import numpy as np
from sqlalchemy import create_engine

def get_feature_cols(feature_config):
    
    feature_cols = []

    for name, val in feature_config['feature_values'].items():
        for interval in feature_config['intervals']:
            for func in feature_config['funcs']:
                col = f"{func} ({feature_config['param']} {val}) FILTER (WHERE cohort_date > {feature_config['event_date']} AND {feature_config['event_date']} BETWEEN cohort_date - INTERVAL '{interval}' AND cohort_date) AS {name}_{interval}_{func.lower()}"
                feature_cols.append(col)

    return feature_cols
    
    
def build_court_history_feature_table(conn, config, cohort_table_name, schema_name, table_name):    
    #Get feature selection columns to insert into query
    feature_cols = get_feature_cols(config)
    
    # Get query template from file
    query_template = ""
    with open(config['feature_sql_path'], "r") as f:
        query_template = f.read()

    # Fill in the sql query 
    table_query = query_template.format(
        schema_name  = schema_name,
        cohort_table_name = cohort_table_name,
        feature_cols = '\n,'.join(feature_cols), 
        from_arg = config['from_arg'])

    logging.debug(table_query)

    drop_table(conn, schema_name, table_name)
    create_table(conn, schema_name, table_name, table_query)

    #log features table
    features = pd.read_sql_query(table_query , conn)
    logging.info(f"Total of {features.shape[1]} features and {features.shape[0]} records saved in {schema_name}.{table_name}")
    logging.info(features)

def build_gen_feature_table(conn, config, cohort_table_name, schema_name, table_name):
      # Get query template from file
    query_template = ""
    with open(config['feature_sql_path'], "r") as f:
        query_template = f.read()

    # Fill in the sql query 
    table_query = query_template.format(
        schema_name  = schema_name,
        cohort_table_name = cohort_table_name)

    logging.debug(table_query)

    drop_table(conn, schema_name, table_name)
    create_table(conn, schema_name, table_name, table_query)

    #log features table
    features = pd.read_sql_query(table_query , conn)
    logging.info(f"Total of {features.shape[1]} features and {features.shape[0]} records saved in {schema_name}.{table_name}")
    logging.info(features)

def build_charges_features_table(conn, schema_name): 

    # Read in table of counts of statute ordinances
    q_dat = """
    with data as(
    select 
        c.*,
        d.disp_date::date,
        d.statute_ord 
    from pipeline.cohort c
    left join clean.dispositions d
    on c.person_id = d.person_id
        and c.cohort_date::date = d.disp_date::date
    )
    select 
        statute_ord,
        count(*)
    from data
    group by statute_ord
    order by count desc;
    """
    with conn.begin():
        dat = pd.read_sql(q_dat, conn)
    
    # Add a proportion column
    counts_sum = dat['count'].values.sum()
    dat['proportion'] = dat['count']/counts_sum 

    # Read in table with columns person_id, disp_date, statute_ord 
    # and charge_desc 
    q_dat_disp = """
    select 
        c.person_id, 
        c.cohort_date,
        d.statute_ord, 
        d.chrg_desc as charge_desc
    from pipeline.cohort c
    left join clean.dispositions d
    on c.person_id = d.person_id
        and c.cohort_date::date = d.disp_date::date
    """
    with conn.begin():
        disp = pd.read_sql(q_dat_disp, conn)
    # Create the list of top 20 statute ordinances 
    top20 = list(dat['statute_ord'][dat.index < 20])

    # Populate a column statute_ord_top20 with the ordinances in the top 20, and code
    # all else as 'other'
    disp['statute_ord_top20'] = disp['statute_ord'].where(disp['statute_ord'].isin(top20))
    disp['statute_ord_top20'] = disp['statute_ord_top20'].fillna('other')
    top20.append('other') # append to the end of the top 20 list
    # For each statute ordinance in the top 20 (21) list, create a column of 0-1s
    for statute in top20:
        disp['{}'.format(statute)] = np.where(disp['statute_ord_top20']==statute, 1, 0)

    # Create a dictionary of statute ordinance categories composed of phrases obtained from the 
    # charge descriptions
    keywords = {
    "kw_minor" : ["minor", "age", "child", "chld", "school", "schl"],
    "kw_liquor" : ["alcohol", "alc", "liq", "liquor", "intox", "retail alco"
                "intoxicated", "intoxication"],
    "kw_animal" : ["animal", "cat", "dog", "fowl", "livestock", "pigs",
                    "anml", 'neutering', 'breeding', 'pit bull'],
    "kw_traffic" : ["improper passing","impr pass cutting in", "incr speed","drove left", 
                    "no pass zone","traffic", "speed", "sped", "yield", "stopsign", 
                    "mph", "drove", "drvr", "driving", "MV", "operating mv", "mtr", "follow too close",
                    "chng lan", "rdce spd", "one way sign", "right turn", "pass veh", "drove slow"
                    "fail to stop", "fail to yld", "fail to yield"],
    "kw_speed": ["speed", "sped", "mph", "racing", "speeding",  "sped const zn", "careless drive"],
    "kw_trespass" : ["tresp", "tress", "trespass", "trespas", "tresspass",
                    "tresspas"],
    "kw_housing" : ["landlord", "hous", "housing", "occupancy", "building", 
                    "build code", "roof", "elec", "waste", "sewage", "sewer", 
                    "structure", "trash"],
    "kw_weapon" : ["wpn", "weapon", "gun", "missile", "handgun", "explosive", "bomb"],
    "kw_stealing": ["larceny", "steal", "stealing", "theft", "stole", "stolen"],
    "kw_disturbance": ["peace", "loud", "noise", "music", "disturb", "disturbance", "nuisance"],
    "kw_compliance": ["impeding", "impede","obstruct", "resist", "contempt", "interfere", "comply",
                    "fail comp", "fail comply", "hinder", "failure to comply", "fail to correct",
                    "order to leave nuis", "nuisance"],
    "kw_safety": ["inspect", "maintenance", "tamper", "open burning", "open flame", "freestand",
                "incendiary burn", "unsafe"],
    "kw_prostitution": ["nude", "lewd", "indec", "indecent", "adlt entrtnmnt", "unclothed", 
                        "sex", "prost", "prostitution"],
    "kw_smoking": ["smoking", "smoke", "smok", "vape", "vaping", "tobacco", "individual cig", "poss substance"]
    }

    # Loop over the categories of statute ordinances (the keys), place 1s in rows of a new column (named that category)
    # where the any of the values in the list corresponding to that category appear in the charge description text.
    for kw in keywords.keys():
        disp[kw] = np.where([any(name in str(row).lower() for name in keywords[kw]) for row in disp['charge_desc']], 1,0)
   
    # Group by person id and disposition date and collapse rows such that each of the new charge features
    # refer to the total number of cases a specific charge was received
    disp_grouped = pd.DataFrame(disp).groupby(['person_id', 'cohort_date']).sum()
    disp_grouped = disp_grouped.reset_index()

    # Write to a table
    with conn.begin(): 
        disp_grouped.pg_copy_to(schema = schema_name, con = conn, name = "charge_features_table",
                        index = False, if_exists = 'replace')


     #log features table
    features = disp_grouped
    logging.info(f"Total of {features.shape[1]} features and {features.shape[0]} records saved in {schema_name}.charge_features_table")
    logging.info(features)
