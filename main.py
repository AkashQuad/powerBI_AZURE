from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware
import os

# ✅ DIRECT imports
from auth import router as auth_router
from workspaces import router as workspace_router
from auto_upload import router as auto_upload_router
from powerbi_folder_migration import router as folder_router

app = FastAPI()

# -------------------------
# Session middleware (MUST BE FIRST)
# -------------------------
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET_KEY", "dev-secret-key"),
    same_site="none",
    https_only=True
)

# -------------------------
# CORS
# -------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://id-preview--1115fb10-6ea8-4052-8d1b-31238016c02e.lovable.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Routers
# -------------------------
app.include_router(auth_router)
app.include_router(workspace_router)
app.include_router(auto_upload_router)
app.include_router(folder_router)

# -------------------------
# Health check
# -------------------------
@app.get("/")
def root():
    return {"status": "Backend running"}

