import logging
import asyncio
import pandas as pd
import json
import aiohttp
import tempfile
import os
from io import StringIO, BytesIO
from typing import Dict, Any, List, Optional, Tuple, Union
from app.core.azure_client import AzureClient
from app.schemas.models import DataSourceConfig, ProcessingStatus
from config.settings import settings
from sqlalchemy.ext.asyncio import create_async_engine
import pandas as pd
from fastapi import UploadFile
import shutil

logger = logging.getLogger(__name__)

async def process_data(job_id: str, config: DataSourceConfig, azure_client: AzureClient):
    """
    Process data from source and upload to Azure
    """
    try:
        
        await azure_client.update_job_status(job_id, "started", {
            "source_type": config.source_type,
            "destination": config.destination
        })
        
        
        data = await fetch_data(config)
        if not data:
            await azure_client.update_job_status(job_id, "failed", {"error": "Failed to fetch data from source"})
            return False
        
        
        if config.transformations:
            data = await transform_data(data, config.transformations)
        
        
        if config.destination.startswith("blob:"):
            
            container_path = config.destination[5:]  
            success = await upload_to_blob(azure_client, job_id, data, container_path, config.file_format)
        
        elif config.destination.startswith("eventhub:"):
            
            event_hub_name = config.destination[9:]  
            success = await azure_client.send_to_event_hub(event_hub_name, data)
        
        else:
            await azure_client.update_job_status(job_id, "failed", {"error": f"Unsupported destination: {config.destination}"})
            return False
        
        
        if success:
            await azure_client.update_job_status(job_id, "completed", {
                "records_processed": len(data) if isinstance(data, list) else "unknown",
                "destination": config.destination
            })
            return True
        else:
            await azure_client.update_job_status(job_id, "failed", {"error": "Failed to send data to destination"})
            return False
            
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        await azure_client.update_job_status(job_id, "failed", {"error": str(e)})
        return False

async def fetch_data(config: DataSourceConfig) -> Union[List[Dict[str, Any]], pd.DataFrame, None]:
    """
    Fetch data from the configured source
    """
    try:
        if config.source_type == "api":
            # Fetch from API
            return await fetch_from_api(config.source_url, config.source_params)
            
        elif config.source_type == "database":
            # Fetch from database
            return await fetch_from_database(
                config.source_url, 
                config.source_query, 
                config.source_params
            )
            
        elif config.source_type == "file":
            # Fetch from file
            return await fetch_from_file(config.source_url, config.file_format)
            
        else:
            logger.error(f"Unsupported source type: {config.source_type}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return None

async def fetch_from_api(url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch data from an API endpoint
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"API returned status code {response.status}")
                
            
            data = await response.json()
            
            
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "data" in data:
                return data["data"] if isinstance(data["data"], list) else [data["data"]]
            elif isinstance(data, dict) and "results" in data:
                return data["results"] if isinstance(data["results"], list) else [data["results"]]
            else:
                return [data]

async def fetch_from_database(connection_string: str, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Fetch data from a database
    """
        
    engine = create_async_engine(connection_string)
    
    async with engine.connect() as conn:
        result = await conn.execute(query, params or {})
        rows = result.fetchall()
        
        # DataFrame
        df = pd.DataFrame(rows, columns=result.keys())
        return df

async def fetch_from_file(file_path: str, file_format: str) -> pd.DataFrame:
    """
    Fetch data from a file
    """
    
    if file_path.startswith(("http://", "https://")):
        async with aiohttp.ClientSession() as session:
            async with session.get(file_path) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download file: status {response.status}")
                
                content = await response.read()
                
                
                if file_format.lower() == "csv":
                    return pd.read_csv(BytesIO(content))
                elif file_format.lower() == "json":
                    return pd.read_json(BytesIO(content))
                elif file_format.lower() == "parquet":
                    return pd.read_parquet(BytesIO(content))
                elif file_format.lower() == "excel":
                    return pd.read_excel(BytesIO(content))
                else:
                    raise ValueError(f"Unsupported file format: {file_format}")
    else:
        
        if file_format.lower() == "csv":
            return pd.read_csv(file_path)
        elif file_format.lower() == "json":
            return pd.read_json(file_path)
        elif file_format.lower() == "parquet":
            return pd.read_parquet(file_path)
        elif file_format.lower() == "excel":
            return pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

async def transform_data(data: Union[List[Dict[str, Any]], pd.DataFrame], transformations: List[Dict[str, Any]]) -> Union[List[Dict[str, Any]], pd.DataFrame]:
    """
    Apply transformations to the data
    """
    # Convert to DataFrame if it's a list of dicts
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data
    
    
    for transform in transformations:
        transform_type = transform.get("type")
        
        if transform_type == "filter":
            
            condition = transform.get("condition")
            df = df.query(condition)
            
        elif transform_type == "select":
            
            columns = transform.get("columns", [])
            df = df[columns]
            
        elif transform_type == "rename":
            
            mapping = transform.get("mapping", {})
            df = df.rename(columns=mapping)
            
        elif transform_type == "aggregate":
            
            group_by = transform.get("group_by", [])
            aggs = transform.get("aggregations", {})
            df = df.groupby(group_by).agg(aggs).reset_index()
            
        elif transform_type == "custom":
            
            # needs finetunning for security
            code = transform.get("code", "")
            
            locals_dict = {"df": df}
            exec(code, {"pd": pd}, locals_dict)
            df = locals_dict["df"]
    
    
    if isinstance(data, list):
        return df.to_dict(orient="records")
    
    return df

async def upload_to_blob(azure_client: AzureClient, job_id: str, data: Union[List[Dict[str, Any]], pd.DataFrame], container_path: str, file_format: str) -> bool:
    """
    Upload data to Azure Blob Storage
    """
    try:
        
        parts = container_path.strip('/').split('/', 1)
        container_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else f"data_{job_id}.{file_format}"
        
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_format}") as tmp:
            
            if isinstance(data, pd.DataFrame):
                if file_format.lower() == "csv":
                    data.to_csv(tmp.name, index=False)
                elif file_format.lower() == "json":
                    data.to_json(tmp.name, orient="records")
                elif file_format.lower() == "parquet":
                    data.to_parquet(tmp.name, index=False)
                elif file_format.lower() == "excel":
                    data.to_excel(tmp.name, index=False)
                else:
                    
                    data.to_csv(tmp.name, index=False)
                    file_format = "csv"
            else:
                
                if file_format.lower() == "csv":
                    pd.DataFrame(data).to_csv(tmp.name, index=False)
                elif file_format.lower() == "json":
                    with open(tmp.name, 'w') as f:
                        json.dump(data, f)
                elif file_format.lower() == "parquet":
                    pd.DataFrame(data).to_parquet(tmp.name, index=False)
                else:
                    
                    with open(tmp.name, 'w') as f:
                        json.dump(data, f)
                    file_format = "json"
        
        
        
        class TempUploadFile:
            def __init__(self, path, filename, content_type):
                self.path = path
                self.filename = filename
                self.content_type = content_type
                self._file = open(path, "rb")
            
            async def read(self):
                return self._file.read()
            
            def close(self):
                self._file.close()
        
        
        content_types = {
            "csv": "text/csv",
            "json": "application/json",
            "parquet": "application/octet-stream",
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        
        upload_file = TempUploadFile(
            tmp.name,
            os.path.basename(blob_path),
            content_types.get(file_format.lower(), "application/octet-stream")
        )
        
        
        success = await azure_client.upload_file(
            job_id=job_id,
            file=upload_file,
            destination=f"{container_name}/{blob_path}"
        )
        
        
        upload_file.close()
        os.unlink(tmp.name)
        
        return success
        
    except Exception as e:
        logger.error(f"Error uploading to blob: {str(e)}")
        return False

async def check_job_status(job_id: str) -> ProcessingStatus:
    """
    Check the status of a data processing job
    """
    from app.api.dependencies import get_azure_client
    
    azure_client = get_azure_client()
    status = await azure_client.get_job_status(job_id)
    
    if not status:
        raise Exception(f"Job {job_id} not found")
    
    return ProcessingStatus(
        job_id=job_id,
        status=status["status"],
        last_updated=status["last_updated"],
        details=status["details"]
    )