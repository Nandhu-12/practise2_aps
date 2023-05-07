import numpy as np
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import pandas as pd
import os,sys



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