from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
import os

# --------------------------------------------------

# App initialization

# --------------------------------------------------

app = FastAPI()

# --------------------------------------------------

# Session Middleware (REQUIRED for cookies)

# --------------------------------------------------

app.add_middleware(
SessionMiddleware,
secret_key=os.getenv("SESSION_SECRET_KEY", "dev-secret-key"),
same_site="none",
https_only=True,
)

# --------------------------------------------------

# CORS Configuration (Lovable + Azure safe)

# --------------------------------------------------

app.add_middleware(
CORSMiddleware,
allow_origins=[
# Lovable preview (EXACT origin)
"[https://id-preview--1115fb10-6ea8-4052-8d1b-31238016c02e.lovable.app](https://id-preview--1115fb10-6ea8-4052-8d1b-31238016c02e.lovable.app)",
],
allow_origin_regex=r"https://.*.lovable.app",
allow_credentials=True,
allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
allow_headers=["*"],
expose_headers=["*"],
)

# --------------------------------------------------

# Routers (direct imports)

# --------------------------------------------------

from auth import router as auth_router
from workspaces import router as workspace_router
from auto_upload import router as auto_upload_router
from powerbi_folder_migration import router as folder_router

app.include_router(auth_router)
app.include_router(workspace_router)
app.include_router(auto_upload_router)
app.include_router(folder_router)

# --------------------------------------------------

# Health Check

# --------------------------------------------------

@app.get("/")
def root():
return {"status": "Backend running"}
