
from fastapi import APIRouter, Request, HTTPException
import requests
from config import POWERBI_API

router = APIRouter()

@router.get("/workspaces")
def get_workspaces(request: Request):
    access_token = request.session.get("access_token")

    if not access_token:
        raise HTTPException(status_code=401, detail="Not logged in")

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # 1. Get all workspaces
    ws_resp = requests.get(f"{POWERBI_API}/groups", headers=headers)

    if ws_resp.status_code != 200:
        raise HTTPException(status_code=ws_resp.status_code, detail=ws_resp.text)

    workspaces = ws_resp.json().get("value", [])

    # 2. For each workspace, get reports + datasets
    for ws in workspaces:
        workspace_id = ws["id"]

        reports_resp = requests.get(
            f"{POWERBI_API}/groups/{workspace_id}/reports",
            headers=headers
        )

        datasets_resp = requests.get(
            f"{POWERBI_API}/groups/{workspace_id}/datasets",
            headers=headers
        )

        ws["reports"] = (
            reports_resp.json().get("value", [])
            if reports_resp.status_code == 200
            else []
        )

        ws["datasets"] = (
            datasets_resp.json().get("value", [])
            if datasets_resp.status_code == 200
            else []
        )

    return {
        "count": len(workspaces),
        "workspaces": workspaces
    }
