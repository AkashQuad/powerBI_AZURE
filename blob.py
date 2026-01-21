import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from app.config import AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER, EMPTY_PBIX_NAME

def download_empty_pbix():
    """Downloads the template PBIX from blob storage."""
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = blob_service.get_container_client(BLOB_CONTAINER)
    blob = container.get_blob_client(EMPTY_PBIX_NAME)
    return blob.download_blob().readall()

def get_dataframes_from_blob(container_name: str, folder_path: str):
    """
    Reads CSVs from the raju/ folder and prepares them for Power BI.
    """
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = blob_service.get_container_client(container_name)
    
    # Target files like 'raju/Extract_Extract.csv'
    prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
    blobs = container.list_blobs(name_starts_with=prefix)
    
    dataframes = []
    for b in blobs:
        if b.name.endswith(('.csv', '.xlsx', '.xls')):
            blob_client = container.get_blob_client(b.name)
            content = blob_client.download_blob().readall()
            
            # Clean table name (e.g., 'Extract_Extract')
            file_name = b.name.split('/')[-1]
            table_name = file_name.rsplit('.', 1)[0]
            
            if b.name.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(content))
            else:
                df = pd.read_excel(io.BytesIO(content))
            
            # CRITICAL: Replace NaN/Inf with None for Power BI JSON compatibility
            df = df.replace([float('inf'), float('-inf')], None)
            df = df.where(pd.notnull(df), None)
            
            dataframes.append((table_name, df))
            
    return dataframes
