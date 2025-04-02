import pandas as pd
from sqlalchemy import create_engine
import os
import boto3
from io import StringIO
import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.estimator import Estimator
from sagemaker.analytics import TrainingJobAnalytics
import mlflow

import boto3

sm_client = boto3.client("sagemaker")
studio_domain_id = sagemaker.session.Session().sagemaker_client.list_app_image_configs()["AppImageConfigs"][0]["AppImageConfigName"]
tracking_uri = f"https://{studio_domain_id}.studio.{boto3.Session().region_name}.sagemaker.aws/mlflow"

mlflow.set_tracking_uri(tracking_uri)

role = sagemaker.get_execution_role()
region = boto3.Session().region_name


train_input = TrainingInput(
    s3_data='s3://euroleague-model-training/train/train.csv',
    content_type='text/csv',
    s3_data_type='S3Prefix'
)

validation_input = TrainingInput(
    s3_data='s3://euroleague-model-training/validation/validation.csv',
    content_type='text/csv',
    s3_data_type='S3Prefix'
)


xgb_estimator = Estimator(
    image_uri=sagemaker.image_uris.retrieve(
        framework='xgboost',
        region=region,
        version='latest'
    ),
    role=role,
    instance_count=1,
    instance_type='ml.m5.large',
    hyperparameters={
        'objective': 'reg:linear',
        'num_round': '100',
        'eta': '0.1',
        'max_depth': '3',
        'eval_metric': 'mae'
    },
    output_path='s3://euroleague-model-training/models', 
    sagemaker_session=sagemaker.Session()
)

def load_data_frame():
    db_url: str = os.getenv("AWS_POSTGRESQL_DB_URL")
    engine = create_engine(db_url)

    with open('queries/select_train_data.sql', 'r') as query:
        df: pd.DataFrame = pd.read_sql_query(query, con=engine)
    return df

def preprocess_pipeline(df):
    # Fill missing target values with season average per player
    def get_season_average_player_valuation(season, player_id):
        return df.loc[
            (df['season'] == season) &
            (df['player_id'] == player_id) &
            (df['next_game_valuation'].notnull())
        ]['next_game_valuation'].mean()

    def fill_missing_target(row):
        if isinstance(row['next_game_valuation'], float):
            return row['next_game_valuation']
        return get_season_average_player_valuation(row['season'], row['player_id'])

    df['next_game_valuation'] = df.apply(fill_missing_target, axis=1)

    # Fill other missing data
    df['win_ratio_ratio'] = df['win_ratio_ratio'].fillna(0)

    # Convert boolean to int if any
    bool_cols = df.select_dtypes(include='bool').columns
    df[bool_cols] = df[bool_cols].astype(int)

    # Drop unwanted columns
    columns_to_exclude = ['id', 'game_id', 'team_id', 'win_ind']
    df = df.drop(columns=columns_to_exclude, errors='ignore')

    # Move target column to first position
    y = df.pop('next_game_valuation')
    df.insert(0, 'next_game_valuation', y)

    # Split into train/test
    test_data = df[df['season'] == '2024']
    train_data = df[df['season'] != '2024']

    return train_data, test_data

def write_dataframe_to_csv_on_s3(dataframe, bucket, filename):
    print("Writing {} records to {}".format(len(dataframe), filename))
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())


def train_pipeline():
    df = load_data_frame()
    train_data, test_data = preprocess_pipeline(df)
    write_dataframe_to_csv_on_s3(train_data, "euroleague-model-training", "train/train.csv")
    write_dataframe_to_csv_on_s3(test_data, "euroleague-model-training", "validation/validation.csv")
    xgb_estimator.fit({'train': train_input, 'validation': validation_input})


def log_experiment_to_mlflow(estimator):

    model_artifact_s3_uri = estimator.model_data
    print("Logging experiment to MLflow...")
    
    # Start a new MLflow run
    with mlflow.start_run() as run:
        # Log hyperparameters used in training
        mlflow.log_param("objective", "reg:linear")
        mlflow.log_param("num_round", 100)
        mlflow.log_param("eta", 0.1)
        mlflow.log_param("max_depth", 3)
        mlflow.log_param("eval_metric", "mae")
        

        job_name = xgb_estimator.latest_training_job.name
        metrics_df = TrainingJobAnalytics(training_job_name=job_name).dataframe()
        mae = metrics_df[metrics_df['metric_name'] == 'validation:mae'].tail()['value'][1]
        mlflow.log_metric("train_mae", mae)
        
        # Download the model artifact locally and log it to MLflow
        local_model_path = "model.tar.gz"
        s3_client = boto3.client("s3")
        if model_artifact_s3_uri.startswith("s3://"):
            s3_path = model_artifact_s3_uri[5:]
            bucket, key = s3_path.split("/", 1)
            s3_client.download_file(bucket, key, local_model_path)
            mlflow.log_artifact(local_model_path, artifact_path="model")
            print(f"Logged model artifact from {model_artifact_s3_uri} to MLflow.")
        else:
            print("Model artifact path is not a valid S3 URI.")
        
        print("MLflow run completed with run_id:", run.info.run_id)


def deploy_model_directly():

    print("Deploying model directly with SageMaker...")
    predictor = xgb_estimator.deploy(
        initia_instance_count=1,
        instance_type='ml.m5.large'
    )
    print("Endpoint deployed. Endpoint name:", predictor.endpoint_name)
    return predictor

def train_flow():
    
    train_pipeline()
    log_experiment_to_mlflow(xgb_estimator)
    deploy_model_directly()