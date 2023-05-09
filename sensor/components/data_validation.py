from sensor.logger import logging
from sensor.exception import SensorException
from scipy.stats import ks_2samp
import pandas as pd
import numpy as np
import os,sys
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor import utils
from typing import Optional
from sensor.config import TARGET_COLUMN


class DataValidation:
    def __init__ (self, data_validation_config : config_entity.DataValidationConfig,
                        data_ingestion_artifact : artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'>>'*20} Data Validation {'<<'*20}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.validation_error = dict()
        except Exception as e:
            raise SensorException(e, sys)

    """
        This 'missing_values_drop' function will drop column which contains missing value more than specified threshold

        df: Accepts a pandas dataframe
        threshold: Percentage criteria to drop a column
        =====================================================================================
        returns Pandas DataFrame if atleast a single column is available after missing columns drop else None
    """

    def missing_values_drop(self, df : pd.DataFrame, report_key_name : str)-> Optional[pd.DataFrame]:
        try:
            threshold = self.data_validation_config.missing_threshold

            # percentage of na values
            logging.info(f"dropping columns which has missing values above {threshold}")
            null_report = df.isna().sum()/df.shape[0]
            drop_column_names = null_report[null_report > threshold].index
            self.validation_error[report_key_name] = list(drop_column_names)
            df.drop(list(drop_column_names),axis = 1, inplace = True)

            # return none if no columns left
            logging.info(f"return none if no columns left")
            if len(df.columns) == 0:
                return None
            return df

        except Exception as e:
            raise SensorException(e, sys)    

    def is_required_columns_exist(self, base_df : pd.DataFrame, current_df : pd.DataFrame, report_key_name : str)->bool:
        try:
            base_columns = base_df.columns
            current_columns = current_df.columns

            missing_columns = []
            for col in base_columns:
                if col not in current_columns:
                    # appending missing olumn in base_columns
                    logging.info(f"{col} : missing col")
                    missing_columns.append(col)

            #checking for no of missing values
            if len(missing_columns)>0:
                self.validation_error[report_key_name] = missing_columns
                return False
            return True
        except Exception as e:
            raise SensorException(e, sys)


    def data_drift(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str):
        try:
            drift_report = dict()
            base_columns = base_df.columns
            current_columns = current_df.columns

            for col in base_columns:
                base_data, current_data = base_df[col], current_df[col]

                #Null hypothesis is that both column data drawn from same distrubtion
                logging.info(f"Hypothesis {base_columns}: {base_data.dtype}, {current_data.dtype} ")
                same_distribution = ks_2samp(base_data,current_data)

                if same_distribution.pvalue > 0.05:
                    #We accept Null Hypothesis
                    drift_report[col] = {
                        "pvalues" : float(same_distribution.pvalue),
                        "same_distribution" : True
                    }
                else:
                    #We reject Null Hypothesis
                    drift_report[col] = {
                        "pvalues" : float(same_distribution.pvalue),
                        "same_distribution" : False
                    }

            self.validation_error[report_key_name] = drift_report
        except Exception as e:
            raise SensorException(e, sys)


    def initiate_data_validation(self)->artifact_entity.DataValidationArtifact:
        try:
            # reading base_df
            logging.info(f"reading base_df")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace({"na" : np.NaN},inplace = True)

            # dropping missing values in base_df
            logging.info(f"dropping missing values in base_df")
            base_df = self.missing_values_drop(df = base_df, report_key_name = "missing_values_within_base_dataset")

            # reading train and test df
            logging.info(f"reading train and test df")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_filepath)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_filepath)

            # dropping missing values in train and test df
            logging.info(f"dropping missing values in train and test df")
            train_df = self.missing_values_drop(df = train_df, report_key_name = "missing_values_within_train_dataset")
            test_df = self.missing_values_drop(df = test_df, report_key_name = "missing_values_within_test_dataset")

            exclude_column = [TARGET_COLUMN]
            base_df = utils.convert_col_float(df = base_df, exclude_columns = exclude_column)
            train_df = utils.convert_col_float(df = train_df, exclude_columns = exclude_column)
            test_df = utils.convert_col_float(df = test_df, exclude_columns = exclude_column)

            #checking for required columns
            logging.info(f"checking for required columns in train and test df")
            train_df_columns_status = self.is_required_columns_exist(base_df = base_df, current_df = train_df, report_key_name = "missing_columns_within_train_dataset")
            test_df_columns_status = self.is_required_columns_exist(base_df = base_df, current_df = test_df, report_key_name = "missing_columns_within_test_dataset")
            
            if train_df_columns_status:
                logging.info(f"As all column are available in train df hence detecting data drift")
                self.data_drift(base_df = base_df, current_df = train_df, report_key_name = "data_drift_within_train_dataset")
            if test_df_columns_status:
                logging.info(f"As all column are available in test df hence detecting data drift")
                self.data_drift(base_df = base_df, current_df = test_df, report_key_name = "data_drift_within_test_dataset")

            #write the report
            logging.info("Write report in yaml file")
            utils.write_yaml_file(data = self.validation_error, file_path = self.data_validation_config.report_file_path)

            # prepare artifact
            logging.info(f"prepare artifact")
            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path = self.data_validation_config.report_file_path)

            return data_validation_artifact

        except Exception as e:
            raise SensorException(e, sys)