from fastapi import APIRouter

from app.services.auth import SchwabAuthService

router = APIRouter(tags=["auth"])

auth_service = SchwabAuthService()


@router.get("/auth/status")
async def auth_status():
    return auth_service.status()
