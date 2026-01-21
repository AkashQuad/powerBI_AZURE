import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER, EMPTY_PBIX_NAME

def download_empty_pbix():
    """Downloads the template PBIX from blob storage."""
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = blob_service.get_container_client(BLOB_CONTAINER)
    blob = container.get_blob_client(EMPTY_PBIX_NAME)
    return blob.download_blob().readall()import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER, EMPTY_PBIX_NAME

def download_empty_pbix():
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = blob_service.get_container_client(BLOB_CONTAINER)
    blob = container.get_blob_client(EMPTY_PBIX_NAME)
    return blob.download_blob().readall()

def get_dataframes_from_blob(container_name: str, folder_path: str):
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = blob_service.get_container_client(container_name)
    
    prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
    blobs = container.list_blobs(name_starts_with=prefix)
    
    dataframes = []
    for b in blobs:
        if b.name.endswith(('.csv', '.xlsx', '.xls')):
            blob_client = container.get_blob_client(b.name)
            content = blob_client.download_blob().readall()
            
            # Match table names from your raju/ folder
            file_name = b.name.split('/')[-1]
            table_name = file_name.rsplit('.', 1)[0]
            
            if b.name.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(content))
            else:
                df = pd.read_excel(io.BytesIO(content))
            
            # CLEANING: Replace NaN with None (null in JSON)
            df = df.where(pd.notnull(df), None)
            
            # OPTIONAL: Force column names to match PBIX (removing spaces/special chars)
            df.columns = [c.replace(" ", "_") for c in df.columns]
            
            dataframes.append((table_name, df))
            
    return dataframes

def get_dataframes_from_blob(container_name: str, folder_path: str):
    """
    Traverses the specific blob folder (e.g., 'raju'), downloads CSV files, 
    and returns a list of (table_name, dataframe).
    """
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = blob_service.get_container_client(container_name)
    
    # Ensure folder_path ends with a slash to correctly list sub-items
    prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
    blobs = container.list_blobs(name_starts_with=prefix)
    
    dataframes = []
    for b in blobs:
        # Targeting the CSV files identified in your storage screenshot
        if b.name.endswith(('.csv', '.xlsx', '.xls')):
            blob_client = container.get_blob_client(b.name)
            content = blob_client.download_blob().readall()
            
            # Clean Table Name: "raju/fact_sales.csv" -> "fact_sales"
            file_name = b.name.split('/')[-1]
            table_name = file_name.rsplit('.', 1)[0].replace(" ", "_")
            
            if b.name.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(content))
            else:
                df = pd.read_excel(io.BytesIO(content))
            
            # Replace NaNs with None so they become valid JSON nulls for Power BI
            df = df.where(pd.notnull(df), None)
            dataframes.append((table_name, df))
            
    return dataframes
