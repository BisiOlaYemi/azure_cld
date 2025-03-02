from pydantic import BaseModel, Field, HttpUrl, validator
from typing import List, Dict, Any, Optional, Union
from enum import Enum
import datetime

class SourceType(str, Enum):
    API = "api"
    DATABASE = "database"
    FILE = "file"

class FileFormat(str, Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    EXCEL = "excel"
    AVRO = "avro"

class TransformationType(str, Enum):
    FILTER = "filter"
    SELECT = "select"
    RENAME = "rename"
    AGGREGATE = "aggregate"
    CUSTOM = "custom"

class Transformation(BaseModel):
    type: TransformationType
    condition: Optional[str] = None  
    columns: Optional[List[str]] = None  
    mapping: Optional[Dict[str, str]] = None  
    group_by: Optional[List[str]] = None  
    aggregations: Optional[Dict[str, str]] = None  
    code: Optional[str] = None  

class DataSourceConfig(BaseModel):
    source_type: SourceType
    source_url: str
    source_params: Optional[Dict[str, Any]] = None
    source_query: Optional[str] = None  
    file_format: Optional[FileFormat] = None  
    transformations: Optional[List[Transformation]] = None
    destination: str  
    
    @validator('destination')
    def validate_destination(cls, v):
        if not (v.startswith("blob:") or v.startswith("eventhub:")):
            raise ValueError("Destination must start with 'blob:' or 'eventhub:'")
        return v
    
    @validator('source_query')
    def validate_source_query(cls, v, values):
        if values.get('source_type') == SourceType.DATABASE and not v:
            raise ValueError("source_query is required for database sources")
        return v
    
    @validator('file_format')
    def validate_file_format(cls, v, values):
        if values.get('source_type') == SourceType.FILE and not v:
            raise ValueError("file_format is required for file sources")
        return v

class JobStatus(BaseModel):
    job_id: str
    status: str

class ProcessingStatus(BaseModel):
    job_id: str
    status: str
    last_updated: Optional[str] = None
    details: Optional[Dict[str, Any]] = None