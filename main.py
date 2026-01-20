from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware

# ✅ DIRECT imports (same directory)
from auth import router as auth_router
from workspaces import router as workspace_router
from auto_upload import router as auto_upload_router
from powerbi_folder_migration import router as folder_router


app = FastAPI()

# -------------------------
# CORS
# -------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://1115fb10-6ea8-4052-8d1b-31238016c02e.lovableproject.com",
        "https://lovable.dev",
        "https://id-preview--1115fb10-6ea8-4052-8d1b-31238016c02e.lovable.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Session middleware
# -------------------------
app.add_middleware(
    SessionMiddleware,
    secret_key="super-secret-key",  # ⚠️ move to env var in prod
    same_site="none",
    https_only=True
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
