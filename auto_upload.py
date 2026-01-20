from fastapi import APIRouter, Request, HTTPException, Body
import requests
import time
import pandas as pd
from config import POWERBI_API
from blob import download_empty_pbix, get_dataframes_from_blob

router = APIRouter()

@router.post("/workspaces/{workspace_id}/auto-upload")
def auto_upload(
    workspace_id: str,
    request: Request,
    payload: dict = Body(...)
):
    # 1. Auth Validation
    access_token = request.session.get("access_token")
    if not access_token:
        raise HTTPException(status_code=401, detail="Not logged in")

    report_name = payload.get("report_name")
    container_name = payload.get("container_name", "tableau-datasources")
    folder_path = payload.get("folder_path", "raju") 
    
    if not report_name:
        raise HTTPException(status_code=400, detail="Report name missing")

    headers = {"Authorization": f"Bearer {access_token}"}

    # 2. Upload PBIX Template
    pbix_bytes = download_empty_pbix()
    files = {"file": (f"{report_name}.pbix", pbix_bytes, "application/vnd.ms-powerbi.pbix")}
    
    upload_url = (
        f"{POWERBI_API}/groups/{workspace_id}/imports"
        f"?datasetDisplayName={report_name}"
        "&nameConflict=CreateOrOverwrite"
    )
    
    resp = requests.post(upload_url, headers=headers, files=files)
    if resp.status_code not in (200, 201, 202):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    # 3. Get Dataset ID (Retry loop for async processing)
    dataset_id = None
    for _ in range(10): 
        time.sleep(2)
        ds_resp = requests.get(f"{POWERBI_API}/groups/{workspace_id}/datasets", headers=headers)
        if ds_resp.ok:
            datasets = ds_resp.json().get("value", [])
            target = next((d for d in datasets if d["name"].lower() == report_name.lower()), None)
            if target:
                dataset_id = target["id"]
                break

    if not dataset_id:
        raise HTTPException(status_code=404, detail="Dataset not found after upload")

    # 4. Sync Blob Data to Power BI Tables
    data_list = get_dataframes_from_blob(container_name, folder_path)
    synced_tables = []

    for table_name, df in data_list:
        rows_url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows"
        
        # DELETE existing rows
        requests.delete(rows_url, headers=headers)
        
        # PUSH new rows (10,000 row chunks)
        rows = df.to_dict(orient='records')
        for i in range(0, len(rows), 10000):
            chunk = {"rows": rows[i : i + 10000]}
            requests.post(rows_url, headers=headers, json=chunk)
        
        synced_tables.append(table_name)

    return {
        "message": "Migration completed successfully",
        "dataset_id": dataset_id,
        "synced_tables": synced_tables
    }
