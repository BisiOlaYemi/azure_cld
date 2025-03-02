import datetime
import logging
import uuid
import asyncio
import aiohttp
import os
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.storage.blob import ContentSettings
from azure.identity import DefaultAzureCredential
from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient
from azure.data.tables.aio import TableServiceClient as AsyncTableServiceClient
from azure.cosmos.aio import CosmosClient as AsyncCosmosClient
from config.settings import settings
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class AzureClient:
    """Client for interacting with various Azure services"""
    
    def __init__(self):
        
        self.credential = DefaultAzureCredential()
        
        
        self.blob_connection_string = settings.AZURE_BLOB_CONNECTION_STRING
        self.eventhub_connection_string = settings.AZURE_EVENTHUB_CONNECTION_STRING
        self.table_connection_string = settings.AZURE_TABLE_CONNECTION_STRING
        self.cosmos_endpoint = settings.AZURE_COSMOS_ENDPOINT
        self.cosmos_key = settings.AZURE_COSMOS_KEY
        
        
        self.jobs_table_name = "datapipelinejobs"
        self._init_job_tracking()
        
        logger.info("Azure client initialized")
    
    def _init_job_tracking(self):
        """Initialize the table for tracking jobs"""
        try:
            table_service = TableServiceClient.from_connection_string(self.table_connection_string)
            try:
                table_service.create_table(self.jobs_table_name)
                logger.info(f"Created job tracking table {self.jobs_table_name}")
            except Exception:
                logger.info(f"Using existing job tracking table {self.jobs_table_name}")
        except Exception as e:
            logger.error(f"Failed to initialize job tracking: {str(e)}")
    
    def generate_job_id(self) -> str:
        """Generate a unique job ID"""
        return str(uuid.uuid4())

    async def upload_file(self, job_id: str, file_contents: bytes, filename: str, content_type: str, destination: str):
        """Upload a file to Azure Blob Storage"""
        try:
            await self.update_job_status(job_id, "uploading")

            
            parts = destination.strip('/').split('/', 1)
            container_name = parts[0]
            blob_path = parts[1] if len(parts) > 1 else filename

            async with AsyncBlobServiceClient.from_connection_string(self.blob_connection_string) as blob_service_client:
                container_client = blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob_path)

                
                await blob_client.upload_blob(
                    file_contents,
                    overwrite=True,
                    content_settings=ContentSettings(content_type=content_type or "application/octet-stream")
                )

            logger.info(f"File {filename} uploaded to {destination}")
            await self.update_job_status(job_id, "completed", {
                "container": container_name,
                "blob_path": blob_path,
                "size_bytes": len(file_contents),
                "content_type": content_type
            })
            return True

        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            await self.update_job_status(job_id, "failed", {"error": str(e)})
            return False

    async def update_job_status(self, job_id: str, status: str, details: Optional[Dict[str, Any]] = None):
        """Update the status of a job in Azure Table Storage"""
        try:
            async with AsyncTableServiceClient.from_connection_string(self.table_connection_string) as table_service:
                table_client = table_service.get_table_client(self.jobs_table_name)
                
                
                import json
                
                entity = {
                    "PartitionKey": "job",
                    "RowKey": job_id,
                    "Status": status,
                    "LastUpdated": datetime.datetime.utcnow().isoformat(),
                    "Details": json.dumps(details) if details else ""
                }
                
                await table_client.upsert_entity(entity)
                logger.info(f"Updated job {job_id} status to {status}")
                return True
                
        except Exception as e:
            logger.error(f"Error updating job status: {str(e)}")
            return False

    async def get_job_status(self, job_id: str):
        """Get the status of a job from Azure Table Storage"""
        try:
            async with AsyncTableServiceClient.from_connection_string(self.table_connection_string) as table_service:
                table_client = table_service.get_table_client(self.jobs_table_name)
                
                entity = await table_client.get_entity("job", job_id)
                
                import json
                details = json.loads(entity.get("Details", "{}")) if entity.get("Details") else {}
                
                return {
                    "job_id": job_id,
                    "status": entity.get("Status"),
                    "last_updated": entity.get("LastUpdated"),
                    "details": details
                }
                
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}")
            return None

    async def cancel_job(self, job_id: str):
        """Cancel a job if it's still running"""
        job_info = await self.get_job_status(job_id)
        
        if not job_info:
            return False
            
        if job_info["status"] in ["completed", "failed", "cancelled"]:
            logger.info(f"Job {job_id} already in final state: {job_info['status']}")
            return False
        
        return await self.update_job_status(job_id, "cancelled", {
            "cancelled_at": datetime.datetime.utcnow().isoformat(),
            "previous_status": job_info["status"]
        })
