import os
import paramiko
import pandas as pd

from config import *
from utils.functions import *
from utils.paybox_payments_schema import *


email_address = os.getenv("email_address") #os.environ['email_address']
email_password = os.getenv("email_password") #os.environ['email_password']

# Configurer le logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Établir une connexion SFTP en utilisant une clé privée (DSA)
try:
    client = paramiko.SSHClient()
    print("valid 1")
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("valid 2")
    client.connect(hostname=FTP_URL, port=FTP_PORT, username=FTP_USER, key_filename=private_key_path)
    print("valid 3")
    logger.info("Connexion SFTP établie avec succès.")

except Exception as error:
    send_email(email_address, email_password)
    logger.error(f"Erreur lors de l'établissement de la connexion SFTP : {error}")

transport = client.get_transport()
sftp = paramiko.SFTPClient.from_transport(transport)

# Récupérer la liste des fichiers CSV sur le SFTP
try:
    remote_directory = '/home/remises/'
    file_list = sftp.listdir(remote_directory)
except Exception as error:
    logger.error(f"Erreur lors de la récupération de la liste des fichiers CSV : {error}")
    raise

# Initialiser une liste pour stocker les df
dataframes = []

# Initialiser une liste pour stocker les noms des fichiers copiés
copied_files = []

# Parcourir la liste des fichiers CSV
for filename in file_list:
    try:
        if filename.endswith('.csv'):
            remote_path = remote_directory + '/' + filename

            # Copier le fichier CSV vers le stockage Cloud Storage
            try:
                destination_blob_name = f'{GCS_OBJECT}/{filename}'
                storage_client = storage.Client()
                bucket = storage_client.bucket(GCS_BUCKET)
                blob = bucket.blob(destination_blob_name)
                with sftp.open(remote_path) as file:
                    blob.upload_from_file(file)
            except Exception as error:
                send_email(email_address, email_password)
                logger.error(f"Erreur lors de la copie du fichier CSV vers Cloud Storage : {error}")
                #raise

            # Ajouter le nom du fichier copié à la liste
            copied_files.append(destination_blob_name)

            # Télécharger le CSV et le lire en tant que df
            try:
                with sftp.open(remote_path) as file:
                    df = pd.read_csv(file, sep=";")
            except Exception as error:
                send_email(email_address, email_password)
                logger.error(f"Erreur lors du téléchargement et de la lecture du fichier CSV : {error}")
                #raise

            # Traiter les différentes colonnes
            try:
                df = df.astype(paybox_dtypes)
                df['Amount'] = df['Amount'].apply(lambda x: x * 0.01).round(2)
                df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
                df['DateOfIssue'] = pd.to_datetime(df['DateOfIssue'], format='%d/%m/%Y')
                df['DateOfExpiry'] = pd.to_datetime(df['DateOfExpiry'], format='%d/%m/%Y')
            except Exception as error:
                send_email(email_address, email_password)
                logger.error(f"Erreur lors du traitement des colonnes du df : {error}")
                #raise

            # Ajouter le df à la liste
            dataframes.append(df)

            # Supprimer le CSV du SFTP
            try:
                sftp.remove(remote_path)
            except Exception as e:
                send_email(email_address, email_password)
                logger.error(f"Erreur lors de la suppression du fichier CSV sur le SFTP : {e}")
                #raise
    except Exception as error:
        send_email(email_address, email_password)
        logger.error(f"Erreur lors du traitement du fichier CSV : {error}")
        #raise

logger.info("Suppression du fichier dans le SFTP.")

# Fermer la connexion SFTP
try:
    logger.info("Fermeture de la connexion SFTP.")
    sftp.close()
    transport.close()
except Exception as error:
    send_email(email_address, email_password)
    logger.error(f"Erreur lors de la fermeture de la connexion SFTP : {error}")
    #raise

# Concat des df
combined_df = pd.concat(dataframes)

# Charger les df dans BQ
# Initialiser le client BQ
try:
    client = bigquery.Client(project=project_id)
except Exception as error:
    send_email(email_address, email_password)
    logger.error(f"Erreur lors de l'initialisation du client BigQuery : {error}")
    #raise

# Charger le df dans BQ
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
try:
    job = client.load_table_from_dataframe(combined_df, f'{dataset_id}.{table_id}', job_config=job_config)
except Exception as error:
    send_email(email_address, email_password)
    logger.error(f"Erreur lors du chargement du df dans BigQuery : {error}")
    #raise

logger.info("Chargement des fichiers dans BigQuery.")

# Attendre la fin du chargement
try:
    job.result()
except Exception as error:
    send_email(email_address, email_password)
    logger.error(f"Erreur lors de l'attente de la fin du chargement dans BigQuery : {error}")
    #raise

logger.info('Tous les fichiers ont été chargés dans BigQuery.')
