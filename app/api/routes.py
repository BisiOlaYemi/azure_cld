from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile, File, Form, Request
from typing import List, Optional
import logging
from app.schemas.models import DataSourceConfig, ProcessingStatus, JobStatus
from app.core.data_processor import process_data, check_job_status
from app.core.azure_client import AzureClient
from app.api.dependencies import get_azure_client

router = APIRouter(prefix="/api/v1")
logger = logging.getLogger(__name__)

@router.post("/ingest", response_model=JobStatus)
async def ingest_data(
    background_tasks: BackgroundTasks,
    config: DataSourceConfig,
    azure_client: AzureClient = Depends(get_azure_client)
):
    """
    Endpoint to start data ingestion job to Azure
    """
    try:
        
        job_id = azure_client.generate_job_id()
        
        
        background_tasks.add_task(
            process_data,
            job_id=job_id,
            config=config,
            azure_client=azure_client
        )
        
        logger.info(f"Started ingestion job {job_id}")
        return JobStatus(job_id=job_id, status="processing")
    
    except Exception as e:
        logger.error(f"Failed to start ingestion job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingest/file", response_model=JobStatus)
async def ingest_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    destination: str = Form(...),
    azure_client: AzureClient = Depends(get_azure_client)
):
    """
    Endpoint to upload a file directly to Azure
    """
    try:
        job_id = azure_client.generate_job_id()
        
        
        file_contents = await file.read()
        
        
        background_tasks.add_task(
            azure_client.upload_file,
            job_id=job_id,
            file_contents=file_contents,  
            filename=file.filename,
            content_type=file.content_type,
            destination=destination
        )
        
        logger.info(f"Started file upload job {job_id}")
        return JobStatus(job_id=job_id, status="processing")
    
    except Exception as e:
        logger.error(f"Failed to start file upload: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=ProcessingStatus)
async def get_job_status(job_id: str):
    """
    Check the status of a processing job
    """
    try:
        status = await check_job_status(job_id)
        return status
    except Exception as e:
        logger.error(f"Failed to get job status: {str(e)}")
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

@router.post("/cancel/{job_id}", response_model=JobStatus)
async def cancel_job(
    job_id: str,
    azure_client: AzureClient = Depends(get_azure_client)
):
    """
    Cancel a running job
    """
    try:
        success = await azure_client.cancel_job(job_id)
        if success:
            return JobStatus(job_id=job_id, status="cancelled")
        else:
            raise HTTPException(status_code=400, detail="Failed to cancel job")
    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))