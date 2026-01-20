from fastapi import APIRouter, HTTPException, Request, Body
from azure.storage.blob import BlobServiceClient
import pandas as pd
import requests
import os
from app.config import (
    POWERBI_API,
    TARGET_WORKSPACE_ID,
    TEMPLATE_WORKSPACE_ID,
    TEMPLATE_REPORT_ID,
    DATASET_NAME,
    TABLE_NAME,
    AZURE_STORAGE_CONNECTION_STRING,
    TABLEAU_BLOB_CONTAINER,
    TABLEAU_FOLDER
)


router = APIRouter()

# --------------------------------------------------
# UTILS
# --------------------------------------------------

def get_user_token(request: Request):
    token = request.session.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not logged in")
    return token


# --------------------------------------------------
# READ DATA FROM BLOB FOLDER
# --------------------------------------------------

# def read_folder_data():
#     blob_service = BlobServiceClient.from_connection_string(
#         AZURE_STORAGE_CONNECTION_STRING
#     )
#     container = blob_service.get_container_client(TABLEAU_BLOB_CONTAINER)

#     dfs = []

#     for blob in container.list_blobs(name_starts_with=f"{TABLEAU_FOLDER}/"):
#         blob_client = container.get_blob_client(blob.name)
#         stream = blob_client.download_blob()

#         if blob.name.endswith(".csv"):
#             dfs.append(pd.read_csv(stream))
#         elif blob.name.endswith(".xlsx"):
#             dfs.append(pd.read_excel(stream))

#     if not dfs:
#         raise Exception("No data files found in folder")

#     return pd.concat(dfs, ignore_index=True)

# def read_folder_data():
#     blob_service = BlobServiceClient.from_connection_string(
#         AZURE_STORAGE_CONNECTION_STRING
#     )
#     container = blob_service.get_container_client(TABLEAU_BLOB_CONTAINER)

#     print("Container:", TABLEAU_BLOB_CONTAINER)
#     print("Folder prefix:", f"{TABLEAU_FOLDER}/")

#     for blob in container.list_blobs(name_starts_with=f"{TABLEAU_FOLDER}/"):
#         print("FOUND:", blob.name)

#         if blob.name.lower().endswith(".csv"):
#             blob_client = container.get_blob_client(blob.name)
#             stream = blob_client.download_blob()

#             df = pd.read_csv(stream)
#             return df   # Directly return the single CSV

#     raise Exception("No CSV file found in folder")

def read_folder_data():
    blob_service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    container = blob_service.get_container_client(TABLEAU_BLOB_CONTAINER)

    target_file = f"{TABLEAU_FOLDER}/Extract_Extract.csv"

    print("Reading file:", target_file)

    blob_client = container.get_blob_client(target_file)
    stream = blob_client.download_blob()

    return pd.read_csv(stream)


# --------------------------------------------------
# CREATE PUSH DATASET
# --------------------------------------------------

def create_push_dataset(token, df, workspace_id):
    columns = [{"name": col, "dataType": "string"} for col in df.columns]

    payload = {
        "name": DATASET_NAME,
        "defaultMode": "Push",
        "tables": [
            {
                "name": TABLE_NAME,
                "columns": columns
            }
        ]
    }

    r = requests.post(
        f"{POWERBI_API}/groups/{workspace_id}/datasets",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json=payload
    )

    r.raise_for_status()
    return r.json()["id"]


# --------------------------------------------------
# PUSH DATA INTO DATASET
# --------------------------------------------------

def push_rows(token, workspace_id, dataset_id, df):
    rows = df.astype(str).to_dict(orient="records")

    r = requests.post(
        f"{POWERBI_API}/groups/{workspace_id}/datasets/"
        f"{dataset_id}/tables/{TABLE_NAME}/rows",
        headers={"Authorization": f"Bearer {token}"},
        json={"rows": rows}
    )

    r.raise_for_status()


# --------------------------------------------------
# CLONE REPORT + REBIND
# --------------------------------------------------

def clone_and_rebind(token, target_workspace_id, dataset_id, report_name):
    clone_payload = {
        "name": report_name,
        "targetWorkspaceId": target_workspace_id,
        "targetModelId": dataset_id
    }

    r = requests.post(
        f"{POWERBI_API}/groups/{TEMPLATE_WORKSPACE_ID}/reports/"
        f"{TEMPLATE_REPORT_ID}/Clone",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json=clone_payload
    )

    r.raise_for_status()
    report_id = r.json()["id"]

    # Explicit rebind
    requests.post(
        f"{POWERBI_API}/groups/{target_workspace_id}/reports/{report_id}/Rebind",
        headers={"Authorization": f"Bearer {token}"},
        json={"datasetId": dataset_id}
    )

    return report_id


# --------------------------------------------------
# API ENDPOINT
# --------------------------------------------------
@router.post("/workspaces/{workspace_id}/folder-migrate")
def folder_migrate(workspace_id: str, request: Request, payload: dict = Body(...)):
    try:
        print("=== Folder migration started ===")
        print("Workspace:", workspace_id)

        token = get_user_token(request)
        report_name = payload.get("report_name")

        print("Report name:", report_name)

        print("Reading Tableau files from Blob...")
        df = read_folder_data()
        print("Rows loaded:", len(df))

        print("Creating Power BI dataset...")
        dataset_id = create_push_dataset(token, df, workspace_id)
        print("Dataset created:", dataset_id)

        print("Pushing rows to dataset...")
        push_rows(token, workspace_id, dataset_id, df)
        print("Data pushed successfully")

        print("Cloning and rebinding report...")
        report_id = clone_and_rebind(token, workspace_id, dataset_id, report_name)
        print("Report created:", report_id)

        print("=== Folder migration completed ===")

        return {
            "success": True,
            "workspaceId": workspace_id,
            "datasetId": dataset_id,
            "reportId": report_id,
            "reportName": report_name,
            "folder": TABLEAU_FOLDER
        }

    except Exception as e:
        print("ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# @router.post("/workspaces/{workspace_id}/folder-migrate")
# def folder_migrate(
#     workspace_id: str,
#     request: Request,
#     payload: dict = Body(...)
# ):
#     try:
#         token = get_user_token(request)
#         report_name = payload.get("report_name")

#         if not report_name:
#             raise HTTPException(status_code=400, detail="Report name missing")

#         df = read_folder_data()

#         dataset_id = create_push_dataset(token, df, workspace_id)

#         push_rows(token, workspace_id, dataset_id, df)

#         report_id = clone_and_rebind(
#             token,
#             workspace_id,
#             dataset_id,
#             report_name
#         )

#         return {
#             "success": True,
#             "workspaceId": workspace_id,
#             "datasetId": dataset_id,
#             "reportId": report_id,
#             "reportName": report_name,
#             "folder": TABLEAU_FOLDER

#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
