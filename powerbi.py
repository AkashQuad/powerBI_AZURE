from fastapi import APIRouter, HTTPException
import requests
from config import POWERBI_API
from blob import download_empty_pbix

router = APIRouter()

@router.post("/workspaces/{workspace_id}/upload")
def upload_report(workspace_id: str, access_token: str):

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        pbix_bytes = download_empty_pbix()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Blob download failed: {str(e)}")

    files = {
        "file": ("empty.pbix", pbix_bytes, "application/vnd.ms-powerbi.pbix")
    }

    url = (
        f"{POWERBI_API}/groups/{workspace_id}/imports"
        "?datasetDisplayName=EmptyReport"
        "&nameConflict=CreateOrOverwrite"
    )

    resp = requests.post(url, headers=headers, files=files)

    if resp.status_code not in (200, 201, 202):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()
