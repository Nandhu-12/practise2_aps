import os,sys
from sensor.exception import SensorException
from sensor.logger import logging
from datetime import datetime


FILE_NAME = "sensor2.csv"
TRAIN_FILE_NAME = "train.csv"
TEST_FILE_NAME = "test.csv"
TRANSFORMER_OBJECT_FILE_PATH = "transformer.pkl"
TARGET_ENCODER_OBJECT_FILE_PATH = "target_encoder.pkl"

class TrainingPipelineConfig:
    def __init__(self):
        try:
            #here we store each output in artifact folder and we have timestamp folder inside artifact folder
            self.artifact_dir = os.path.join(os.getcwd(),"artifact",f"{datetime.now().strftime('%m%d%Y__%H%M%S')}")
        except Exception as e:
            raise SensorException(e,sys)

class DataIngestionConfig:
    def __init__(self, training_pipeline_config : TrainingPipelineConfig):
        try:
            self.db_name = "aps2"
            self.col_name = "sensor2"

            self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir, "data_ingestion")
            self.feature_store_filepath = os.path.join(self.data_ingestion_dir, "feature store",FILE_NAME)
            self.train_filepath = os.path.join(self.data_ingestion_dir,"dataset",TRAIN_FILE_NAME)
            self.test_filepath = os.path.join(self.data_ingestion_dir,"dataset",TEST_FILE_NAME)
            self.test_size = 0.2

        except Exception as e:
            raise SensorException(e, sys)

    def to_dict(self,)->dict:
        try:
            return self.__dict__
        except Exception as e:
            raise SensorException(e, sys)


class DataValidationConfig:
    def __init__ (self, training_pipeline_config : TrainingPipelineConfig):
        try:
            self.data_validation_dir = os.path.join(training_pipeline_config.artifact_dir, "data_validation")
            self.report_file_path = os.path.join(self.data_validation_dir, "report_yaml")
            self.base_file_path = os.path.join("aps_failure_training_set1.csv")
            self.missing_threshold:float = 0.2
        except Exception as e:
            raise SensorException(e, sys)
            

class DataTransformationConfig:
    def __init__ (self,training_pipeline_config = TrainingPipelineConfig):
        try:
            self.data_transformation_dir = os.path.join(training_pipeline_config.artifact_dir, "data_transformation")
            self.transformer_file_path = os.path.join(self.data_transformation_dir, "transformer",TRANSFORMER_OBJECT_FILE_PATH)
            #npz in numpy array format
            self.transformed_train_path = os.path.join(self.data_transformation_dir, "transformed",TRAIN_FILE_NAME.replace("csv","npz"))
            self.transformed_test_path = os.path.join(self.data_transformation_dir, "transformed",TEST_FILE_NAME.replace("csv","npz"))
            self.target_encoder_path = os.path.join(self.data_transformation_dir, "target_encoder",TARGET_ENCODER_OBJECT_FILE_PATH)
        except Exception as e:
            raise SensorException(e, sys)


class ModelTrainerConfig:...
class ModelEvaluationConfig:...
class ModelPusherConfig:...