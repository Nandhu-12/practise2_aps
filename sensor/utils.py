import numpy as np
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import pandas as pd
import os,sys
import yaml
import dill



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

#like pickle -- serialization
def save_object(file_path : str, obj : object)-> None:
    try:
        logging.info("Entered the save_object method of utils")

        os.makedirs(os.path.dirname(file_path),exist_ok = True)
        with open (file_path,"wb") as file_obj:
            dill.dump(obj,file_obj)
        logging.info("Exited the save_object method of utils")

    except Exception as e:
        raise SensorException(e,sys)
    
    
    """
    Save numpy array data to file
    file_path: str location of file to save
    array: np.array data to save
    """
def save_numpy_array_data(file_path : str, array : np.array):
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok = True)
        with open (file_path,"wb") as file_obj:
            np.save(file_obj,array)

    except Exception as e:
        raise SensorException(e,sys)


