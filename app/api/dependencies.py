import os
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional

from app.core.azure_client import AzureClient
from config.settings import settings

# Azure client singleton
_azure_client = None

def get_azure_client() -> AzureClient:
    """
    Get or create the Azure client singleton
    """
    global _azure_client
    if _azure_client is None:
        _azure_client = AzureClient()
    return _azure_client


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

async def verify_api_key(api_key: Optional[str] = Depends(oauth2_scheme)):
    """
    Verify the API key if security is enabled
    """
    
    if settings.DEBUG:
        return True
        
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Validate against a database or secure storage
    valid_api_keys = os.environ.get("API_KEYS", "").split(",")
    if api_key not in valid_api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return True