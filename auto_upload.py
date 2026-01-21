from fastapi import APIRouter, Request, HTTPException, Body
import requests
import time
import traceback
from config import POWERBI_API
from blob import download_empty_pbix, get_dataframes_from_blob

router = APIRouter()

@router.post("/workspaces/{workspace_id}/auto-upload")
def auto_upload(workspace_id: str, request: Request, payload: dict = Body(...)):
    try:
        access_token = request.session.get("access_token")
        if not access_token:
            raise HTTPException(status_code=401, detail="Not logged in")

        report_name = payload.get("report_name")
        container_name = payload.get("container_name", "tableau-datasources")
        folder_path = payload.get("folder_path", "raju")

        headers = {"Authorization": f"Bearer {access_token}"}

        # 1. Upload Template
        pbix_bytes = download_empty_pbix()
        files = {"file": (f"{report_name}.pbix", pbix_bytes, "application/vnd.ms-powerbi.pbix")}
        upload_url = f"{POWERBI_API}/groups/{workspace_id}/imports?datasetDisplayName={report_name}&nameConflict=CreateOrOverwrite"
        
        resp = requests.post(upload_url, headers=headers, files=files)
        if not resp.ok:
            raise HTTPException(status_code=resp.status_code, detail=f"Upload error: {resp.text}")

        # 2. Find Dataset ID
        dataset_id = None
        for _ in range(10):
            time.sleep(2)
            ds_resp = requests.get(f"{POWERBI_API}/groups/{workspace_id}/datasets", headers=headers)
            datasets = ds_resp.json().get("value", [])
            target = next((d for d in datasets if d["name"].lower() == report_name.lower()), None)
            if target:
                dataset_id = target["id"]
                break

        if not dataset_id:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # 3. Sync Tables
        data_list = get_dataframes_from_blob(container_name, folder_path)
        synced_tables = []

        for table_name, df in data_list:
            url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows"
            
            # TRUNCATE: Remove old data
            requests.delete(url, headers=headers)
            
            # PUSH: Insert in chunks
            rows = df.to_dict(orient="records")
            for i in range(0, len(rows), 5000): # Smaller chunks are safer
                chunk = {"rows": rows[i : i + 5000]}
                post_resp = requests.post(url, headers=headers, json=chunk)
                
                if not post_resp.ok:
                    # Provide specific error from Power BI
                    error_info = post_resp.json() if post_resp.text else post_resp.text
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Table '{table_name}' failed. Error: {error_info}"
                    )

            synced_tables.append(table_name)

        return {"status": "success", "synced": synced_tables}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
