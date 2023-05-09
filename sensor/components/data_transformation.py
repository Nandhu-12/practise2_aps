from sensor.logger import logging
from sensor.exception import SensorException
import pandas as pd
import numpy as np
import os,sys
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor import utils
from sklearn.preprocessing import LabelEncoder
from imblearn.combine import SMOTETomek
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler
from sensor.config import TARGET_COLUMN
from sklearn.pipeline import Pipeline


class DataTransformation:
    def __init__ (self, data_transformation_config : config_entity.DataTransformationConfig,
                        data_ingestion_artifact : artifact_entity.DataIngestionArtifact):

        try:
            logging.info(f"{'>>'*20} Data Transformation {'<<'*20}")
            self.data_transformation_config = data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise SensorException(e, sys)

    
    @classmethod
    def get_data_transformer_object(cls)->Pipeline:
        try:
            logging.info(f"first imputing nan values then doing scaling using robust scaler as there are outliers")
            simple_imputer = SimpleImputer(strategy = 'constant', fill_value = 0)
            robust_scaler = RobustScaler()
            pipeline = Pipeline(steps = [('Imputer',simple_imputer),('RobustScaler',robust_scaler)])
            return pipeline
        except Exception as e:
            raise SensorException(e, sys)


    def initiate_data_transformation (self)->artifact_entity.DataTransformationArtifact:
        try:
            #reading train and test file
            logging.info("reading train and test file")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_filepath)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_filepath)

            #selecting input and target feature
            logging.info("selecting input and target feature")
            input_feature_train_df = train_df.drop(TARGET_COLUMN,axis = 1)
            input_feature_test_df = test_df.drop(TARGET_COLUMN,axis = 1)
            target_feature_train_df = train_df[TARGET_COLUMN]
            target_feature_test_df = test_df[TARGET_COLUMN]

            #label_encoder on target feature
            logging.info("label_encoder")
            label_encoder = LabelEncoder()
            label_encoder.fit(target_feature_train_df)
            target_feature_train_arr = label_encoder.transform(target_feature_train_df)
            target_feature_test_arr = label_encoder.transform(target_feature_test_df)

            #transforming input features
            logging.info("transforming input features")
            transforming_pipeline = DataTransformation.get_data_transformer_object()
            transforming_pipeline.fit(input_feature_train_df)
            input_feature_train_arr = transforming_pipeline.transform(input_feature_train_df)
            input_feature_test_arr = transforming_pipeline.transform(input_feature_test_df)

            #handling imbalanced data using SMOTETomek
            smt = SMOTETomek(sampling_strategy = "minority")
            logging.info("handling imbalanced data using SMOTETomek")
            logging.info(f"Before resampling in training set Input: {input_feature_train_arr.shape} Target:{target_feature_train_arr.shape}")

            input_feature_train_arr,target_feature_train_arr = smt.fit_resample(input_feature_train_arr,target_feature_train_arr)
            
            logging.info(f"After resampling in training set Input: {input_feature_train_arr.shape} Target:{target_feature_train_arr.shape}")
            logging.info(f"Before resampling in testing set Input: {input_feature_test_arr.shape} Target:{target_feature_test_arr.shape}")
            
            input_feature_test_arr,target_feature_test_arr = smt.fit_resample(input_feature_test_arr,target_feature_test_arr)

            logging.info(f"After resampling in testing set Input: {input_feature_test_arr.shape} Target:{target_feature_test_arr.shape}")

            #concatenating to store in single file
            logging.info("concatenating to store in single file")
            train_arr = np.c_[input_feature_train_arr,target_feature_train_arr]
            test_arr = np.c_[input_feature_test_arr,target_feature_test_arr]

            #save numpy array
            logging.info("save numpy array")
            utils.save_numpy_array_data(file_path = self.data_transformation_config.transformed_train_path, array = train_arr)
            utils.save_numpy_array_data(file_path = self.data_transformation_config.transformed_test_path, array = test_arr)

            #save object
            logging.info("save object")
            utils.save_object(file_path = self.data_transformation_config.transformer_file_path, obj = transforming_pipeline)

            #save encoder
            logging.info("save encoder")
            utils.save_object(file_path = self.data_transformation_config.target_encoder_path, obj = label_encoder)

            #prepare artifact
            logging.info("prepare artifact")
            data_transformation_artifact = artifact_entity.DataTransformationArtifact(
                transformer_file_path = self.data_transformation_config.transformer_file_path, 
                transformed_train_path = self.data_transformation_config.transformed_train_path, 
                transformed_test_path = self.data_transformation_config.transformed_test_path, 
                target_encoder_path = self.data_transformation_config.target_encoder_path)

            logging.info(f"Data transformation object {data_transformation_artifact}")
            return data_transformation_artifact

        except Exception as e:
            raise SensorException(e, sys)


