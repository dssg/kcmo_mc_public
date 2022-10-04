# TIME SPLITTER

# Load in packages 
import pandas as pd 
import logging
from src.utils.sql_utils import *
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta

def time_splitter(newest_available_date, oldest_available_date, 
                    label_window, training_window, model_update_freq, max_splits):
    """_summary_

    Args:
        newest_available_date (string): the newest date that can be used for 
            training, in format"yyyy-m-d"
        oldest_available_date (string): oldest date in the dataset, in format 
            "yyyy-m-d"
        label_window (int): (in months) the window for prediction, i.e., the window
            of time we are looking for a new charge
        training_window (int): (in years) the window of time within which we can 
            move the label window.Equivalently, the number of years we have available 
            for training.
        model_update_freq (int): (in months) how long the particular model should be 
            used for, the duration with which the time 'slide' should occur. 
    """


    # Change parameters to appropriate date format
    newest_available_date = datetime.strptime(newest_available_date, '%Y-%m-%d')
    oldest_available_date = datetime.strptime(oldest_available_date, '%Y-%m-%d')
    label_window = relativedelta(months = label_window)
    training_window = relativedelta(years = training_window)
    model_update_freq = relativedelta(months = model_update_freq)

    # For now, set validation_duration equal to model_update_freq
    validation_duration = model_update_freq

    # Work backwards from the validation end date
    val_end_date = newest_available_date - label_window
    val_start_date = val_end_date - validation_duration

    # Compute the training end and start dates 
    train_end_date = val_start_date - label_window 
    train_start_date = train_end_date - training_window

    split = {'train_start_date':train_start_date,
             'train_end_date':train_end_date,
             'val_start_date':val_start_date,
             'val_end_date':val_end_date}

    # Append first split 
    splits = []
    splits.append(split)

    # While it is possible to make another split,
    while (splits[-1]['train_start_date'] > (model_update_freq + oldest_available_date)) & (len(splits) < max_splits):
        # Add a split
        prev_split = splits[-1]
        next_split = {'train_start_date':prev_split['train_start_date'] - model_update_freq,
                      'train_end_date':prev_split['train_end_date'] - model_update_freq,
                      'val_start_date':prev_split['val_start_date'] - model_update_freq,
                      'val_end_date':prev_split['val_end_date'] - model_update_freq}
        splits.append(next_split)

    for split in splits:
        for key in split.keys():
            split[key] = split[key].strftime('%Y-%m-%d')
            
    # Log summary info 
    logging.info("Total splits: " + str(len(splits)))
    logging.info("split 0")
    logging.info(splits[0])
    logging.info("split 1")
    logging.info(splits[1])
    logging.info("split -1 (last split)")
    logging.info(splits[-1])

    return splits



