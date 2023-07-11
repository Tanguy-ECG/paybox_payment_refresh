from google.cloud import bigquery
from google.cloud import storage
from google.cloud import error_reporting
import google.cloud.logging as cloudlogging
import logging
from google.oauth2 import service_account

import boto3

from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/mpenicaud/Documents/Credentials/Google/Sandbox_service_account_manon.json'

load_dotenv()

## GOOGLE UTILITIES

# Create logger client
try:
    log_client = cloudlogging.Client()
 
except:
    key_path = 'C:/Users/mpenicaud/Documents/Credentials/Google/Sandbox_service_account_manon.json'
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes = ["https://www.googleapis.com/auth/cloud-platform"],
    )
    log_client = cloudlogging.Client(credentials=credentials,project=credentials.project_id,)
finally:
    log_handler = log_client.get_default_handler()
    cloud_logger = logging.getLogger("PAYBOX_PAYMENTS")
    cloud_logger.setLevel(logging.INFO)
    cloud_logger.addHandler(log_handler)

# Create storage client
try:
    storage_client = storage.Client()
except:
    key_path = 'C:/Users/mpenicaud/Documents/Credentials/Google/Sandbox_service_account_manon.json'
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes = ["https://www.googleapis.com/auth/cloud-platform"],
    )
    storage_client = storage.Client(credentials=credentials,project=credentials.project_id,)

# Create bigquery client
try:
    bigquery_client = bigquery.Client()
except:
    key_path = 'C:/Users/mpenicaud/Documents/Credentials/Google/Sandbox_service_account_manon.json'
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes = ["https://www.googleapis.com/auth/cloud-platform"],
    )
    bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# # Create error reporting client
# try:
#     error_reporting_client = error_reporting.Client(service='paybox_payments_loader',)
# except:
#     key_path = credentials_sandbox
#     credentials = service_account.Credentials.from_service_account_file(
#     key_path, scopes = ["https://www.googleapis.com/auth/cloud-platform"],
#     )
#     error_reporting_client = error_reporting.Client(credentials=credentials, project=credentials.project_id, service='paybox_payments_loader')

def get_env_variables():
    
    cloud_logger.info("Setting Environment")

    # GCP ENV
    global GCS_BUCKET
    GCS_BUCKET = os.environ.get('GCS_BUCKET','Specified environment variable is not set')
    global GCS_OBJECT
    GCS_OBJECT = os.environ.get('GCS_OBJECT','Specified environment variable is not set')

    # FTP ENV
    global FTP_URL
    FTP_URL = os.environ.get('FTP_URL','Specified environment variable is not set')
    global FTP_PORT
    FTP_PORT = os.environ.get('FTP_PORT','Specified environment variable is not set')
    global FTP_USER
    FTP_USER = os.environ.get('FTP_USER','Specified environment variable is not set')
    global private_key_path
    private_key_path = os.environ.get('private_key_path', 'Specified environment variable is not set')

    # BQ ENV
    global project_id
    project_id = os.environ.get('project_id','Specified environment variable is not set')
    global dataset_id
    dataset_id = os.environ.get('dataset_id','Specified environment variable is not set')
    global table_id
    table_id = os.environ.get('table_id','Specified environment variable is not set')

    return

get_env_variables()