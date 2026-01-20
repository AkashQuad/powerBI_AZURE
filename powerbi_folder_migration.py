import io

import time

import pandas as pd

import requests

from fastapi import APIRouter, HTTPException, Request, Body

from azure.storage.blob import BlobServiceClient

from config import (

    POWERBI_API,

    AZURE_STORAGE_CONNECTION_STRING,

    TABLEAU_BLOB_CONTAINER,

    TABLEAU_FOLDER,

    EMPTY_PBIX_NAME

)
 
router = APIRouter()
 
# --------------------------------------------------

# UTILS & BLOB OPERATIONS

# --------------------------------------------------
 
def get_user_token(request: Request):

    token = request.session.get("access_token")

    if not token:

        raise HTTPException(status_code=401, detail="Not logged in")

    return token
 
def get_data_from_blob():

    """Reads all CSV/Excel files from the specified folder and returns a list of (table_name, dataframe)."""

    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

    container = blob_service.get_container_client(TABLEAU_BLOB_CONTAINER)

    prefix = TABLEAU_FOLDER if TABLEAU_FOLDER.endswith('/') else f"{TABLEAU_FOLDER}/"

    blobs = container.list_blobs(name_starts_with=prefix)

    data_list = []

    for b in blobs:

        if b.name.lower().endswith(('.csv', '.xlsx', '.xls')):

            blob_client = container.get_blob_client(b.name)

            content = blob_client.download_blob().readall()

            # Clean Table Name: "raju/fact_sales.csv" -> "fact_sales"

            file_name = b.name.split('/')[-1]

            table_name = file_name.rsplit('.', 1)[0].replace(" ", "_")

            if b.name.lower().endswith('.csv'):

                df = pd.read_csv(io.BytesIO(content))

            else:

                df = pd.read_excel(io.BytesIO(content))

            # Convert all to string for 'Push' dataset compatibility and handle NaNs

            df = df.astype(str).where(pd.notnull(df), None)

            data_list.append((table_name, df))

    if not data_list:

        raise Exception(f"No data files found in folder: {prefix}")

    return data_list
 
# --------------------------------------------------

# POWER BI OPERATIONS

# --------------------------------------------------
 
def create_push_dataset(token, data_list, workspace_id, dataset_name):

    """Creates a multi-table Push dataset schema based on the dataframes."""

    tables_schema = []

    for table_name, df in data_list:

        columns = [{"name": col, "dataType": "string"} for col in df.columns]

        tables_schema.append({"name": table_name, "columns": columns})
 
    payload = {

        "name": dataset_name,

        "defaultMode": "Push",

        "tables": tables_schema

    }
 
    r = requests.post(

        f"{POWERBI_API}/groups/{workspace_id}/datasets",

        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},

        json=payload

    )

    r.raise_for_status()

    return r.json()["id"]
 
def push_all_data(token, workspace_id, dataset_id, data_list):

    """Iterates through all dataframes and pushes rows in chunks."""

    for table_name, df in data_list:

        rows_url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows"

        rows = df.to_dict(orient="records")

        # Batching (10,000 rows limit per request)

        for i in range(0, len(rows), 10000):

            chunk = {"rows": rows[i : i + 10000]}

            r = requests.post(

                rows_url, 

                headers={"Authorization": f"Bearer {token}"}, 

                json=chunk

            )

            r.raise_for_status()
 
# --------------------------------------------------

# API ENDPOINT

# --------------------------------------------------
 
@router.post("/workspaces/{workspace_id}/folder-migrate")

def folder_migrate(workspace_id: str, request: Request, payload: dict = Body(...)):

    try:

        print(f"=== Starting Migration for Workspace: {workspace_id} ===")

        token = get_user_token(request)

        report_name = payload.get("report_name", "Migrated_Report")
 
        # 1. Fetch all data from Blob

        print("Fetching data from Azure Blob...")

        data_list = get_data_from_blob()

        # 2. Create Push Dataset with all identified tables

        print("Creating Push Dataset in Power BI...")

        dataset_id = create_push_dataset(token, data_list, workspace_id, report_name)

        # 3. Push data to each table

        print("Pushing data to Power BI tables...")

        push_all_data(token, workspace_id, dataset_id, data_list)
 
        print("=== Migration Completed Successfully ===")

        return {

            "success": True,

            "datasetId": dataset_id,

            "reportName": report_name,

            "synced_tables": [item[0] for item in data_list]

        }
 
    except Exception as e:

        print(f"CRITICAL ERROR: {str(e)}")

        raise HTTPException(status_code=500, detail=str(e))
 
