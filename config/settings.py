import os
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv


load_dotenv()

class Settings(BaseSettings):
    """Application settings"""
    
    
    AZURE_BLOB_CONNECTION_STRING: str = Field(..., env="AZURE_BLOB_CONNECTION_STRING")
    AZURE_EVENTHUB_CONNECTION_STRING: str = Field(..., env="AZURE_EVENTHUB_CONNECTION_STRING")
    AZURE_TABLE_CONNECTION_STRING: str = Field(..., env="AZURE_TABLE_CONNECTION_STRING")
    AZURE_COSMOS_ENDPOINT: str = Field(..., env="AZURE_COSMOS_ENDPOINT")
    AZURE_COSMOS_KEY: str = Field(..., env="AZURE_COSMOS_KEY")
    
    
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    API_KEY: str = Field(..., env="API_KEY")
    MAX_WORKERS: int = Field(4, env="MAX_WORKERS")
    BATCH_SIZE: int = Field(1000, env="BATCH_SIZE")
    
    
    HOST: str = Field("0.0.0.0", env="HOST")
    PORT: int = Field(8000, env="PORT")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

# instance
settings = Settings()