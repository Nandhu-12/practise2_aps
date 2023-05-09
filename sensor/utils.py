import numpy as np
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import pandas as pd
import os,sys
import yaml



def get_collection_as_dataframe(db_name:str, col_name:str)->pd.DataFrame:
    try:
        logging.info(f"reading from {db_name} database and {col_name} collection")
        df = pd.DataFrame(mongo_client[db_name][col_name].find())

        logging.info(f"found {df.columns} columns")
        if "_id" in df.columns:

            logging.info("dropping id column")
            df = df.drop("_id",axis = 1)

        logging.info(f"{df.shape} is the number of rows and columns in df")
        return df

    except Exception as e:
        raise SensorException(e,sys)


def write_yaml_file(data : dict, file_path):
    try:
        logging.info(f"creating file dir")
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok = True)

        with open(file_path,'w') as file_writer:
            logging.info(f"dumping message in file dir")
            yaml.dump(data,file_writer)
    except Exception as e:
        raise SensorException(e,sys)


def convert_col_float(df : pd.DataFrame, exclude_columns : list)->pd.DataFrame:
    try:
        for col in df.columns:
            if col not in exclude_columns:
                logging.info(f"converting dtype of all columns to float except target column")
                df[col] = df[col].astype('float')
        return df
    except Exception as e:
        raise SensorException(e,sys)
