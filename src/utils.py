import omegaconf
from copy import deepcopy
from google.cloud import storage
import os
from okcredit_ml.cloud_connectors import gcp
import pandas as pd

def create_config_from_template(config_template:omegaconf.DictConfig, name) -> omegaconf.DictConfig:
    """
    > It takes a config template and a name, and returns a new config with the basetables,
    featuretables, and sink updated to include the name
    
    :param config_template: the template config file
    :type config_template: omegaconf.DictConfig
    :param name: The name of the experiment. This will be used to create the tables in the database
    :return: A dictionary of the config file
    """
    
    def add_sufix(string, suffix):
        return string + '_' + suffix
    
    config = deepcopy(config_template)
    
    for basetable in config.basetables:
        config.basetables[basetable] = add_sufix(config.basetables[basetable], name)
        
    for featuretable in config.featuretables:
        config.featuretables[featuretable] = add_sufix(config.featuretables[featuretable], name)
        
    config.sink = add_sufix(config.sink, name)
    
    return config

def cleanup_tables(project, config:omegaconf.DictConfig) -> None:
    """
    > Delete all the tables in the `basetables` and `featuretables` sections of the config file
    
    :param gcp_bq: the GCP BigQuery object
    :param config: This is the configuration file that we created earlier
    :type config: omegaconf.DictConfig
    """
    
    gcp_bq = gcp.BQPy(project_id=project)
    
    for basetable in config.basetables:
        gcp_bq.delete_table(config.basetables[basetable])
        
    for featuretable in config.featuretables:
        gcp_bq.delete_table(config.featuretables[featuretable])
        
        
def exists_in_gcs(project, bucket_name, blob_path):
    """
    > It returns True if the blob exists in the bucket, and False otherwise
    
    :param project: The name of your Google Cloud project
    :param bucket_name: The name of the bucket you want to upload to
    :param blob_path: The path to the file in GCS
    :return: A boolean value.
    """
    storage_client = storage.Client(project)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.exists()


def create_folder_in_gcs(project, bucket_name, folder_name):
    """
    > It creates a folder in GCS if it doesn't already exist
    
    :param project: The name of your Google Cloud project
    :param bucket_name: The name of the bucket you want to create
    :param folder_name: The name of the folder you want to create in GCS
    """
    
    storage_client = storage.Client(project)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(folder_name)
    
    if blob.exists():
        folder_path = f'gs://{project}/{bucket_name}/{folder_name}'
        print(f"Folder {folder_path} already exists in gcs")
        return
    
    blob.upload_from_string('')
    print(f"Folder {folder_name} created successfully in gcs")
    
def copy_from_local_to_gcs(project, bucket_name, gcs_file_path, local_file_path):
    """
    > The function takes a project name, a bucket name, a GCS file path, and a local file path as
    arguments, and uploads the local file to the GCS file path
    
    :param project: the name of your GCP project
    :param bucket_name: The name of the bucket you created in the previous step
    :param gcs_file_path: The path to the file in GCS
    :param local_file_path: The path to the file on your local machine
    """

    storage_client = storage.Client(project)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_file_path)
    
    print(f'File {local_file_path} uploaded to {gcs_file_path}')
    
def download_files_from_gcs_folder(project, bucket_name, gcs_folder_path, local_save_path):
    """
    > It downloads all the files in a GCS folder to a local folder
    
    :param project: The name of your GCP project
    :param bucket_name: The name of the bucket you want to download from
    :param gcs_folder_path: The path to the folder in GCS that you want to download
    :param local_save_path: The local path where you want to save the files
    """
    storage_client = storage.Client(project=project)
    blobs = storage_client.list_blobs(bucket_or_name=bucket_name, prefix=gcs_folder_path, delimiter='/')
    for blob in blobs:
        blob_name = blob.name
        file_name = blob_name.split("/")[-1]
        file_save_path = os.path.join(local_save_path, file_name)
        if blob_name != gcs_folder_path:
            blob.download_to_filename(file_save_path)
            print(f'File {file_name} downloaded to {file_save_path}')
            
            
def read_multiple_dfs(base_path, file_names):
    """
    It takes a list of file names and a base path, and returns a single dataframe by reading and appending all dataframes.
    Also adds a column called file_name referring to the name of the file.
    
    :param base_path: The path to the folder where the files are stored
    :param file_names: A list of file names to read
    :return: A dataframe with all the data from the files in the list.
    """

    if type(file_names) == str:
        file_names = [file_names]
    
    df_list = []
    for file_name in file_names:
        if file_name.endswith('.csv'):
            file_path = os.path.join(base_path, file_name)
            df = pd.read_csv(file_path)
            df['file_name'] = file_name[:-4] # To remove .csv from the file name
            
        df_list.append(df)
        
    df = pd.concat(df_list)
    
    if 'run_date' in df.columns:
        df['run_date'] = pd.to_datetime(df['run_date'])
    
    return df