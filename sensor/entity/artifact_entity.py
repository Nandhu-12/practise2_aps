from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    feature_store_filepath : str
    train_filepath : str
    test_filepath : str

@dataclass
class DataValidationArtifact:
    report_file_path : str

@dataclass
class DataTransformationArtifact:
    transformer_file_path : str
    transformed_train_path : str
    transformed_test_path : str
    target_encoder_path : str


class ModelTrainerArtifact:...
class ModelEvaluationArtifact:...
class ModelPusherArtifact:...