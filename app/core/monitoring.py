# app/core/monitoring.py
import time
import logging
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Histogram, start_http_server
import psutil
import asyncio
from typing import Callable
import os


logger = logging.getLogger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter(
    'app_request_count', 
    'Application Request Count',
    ['method', 'endpoint', 'http_status']
)
REQUEST_LATENCY = Histogram(
    'app_request_latency_seconds', 
    'Application Request Latency',
    ['method', 'endpoint']
)
ACTIVE_JOBS = Counter(
    'app_active_jobs',
    'Active Data Processing Jobs',
    ['status']
)
DATA_VOLUME = Counter(
    'app_data_volume_bytes',
    'Data Volume Processed in Bytes',
    ['destination_type']
)

class MonitoringMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        method = request.method
        path = request.url.path
        
        if path == "/metrics":
            return await call_next(request)
        
        start_time = time.time()
        
        try:
            response = await call_next(request)
            status_code = response.status_code
            
            REQUEST_COUNT.labels(method, path, status_code).inc()
            REQUEST_LATENCY.labels(method, path).observe(time.time() - start_time)
            
            return response
        except Exception as e:
            status_code = 500
            REQUEST_COUNT.labels(method, path, status_code).inc()
            REQUEST_LATENCY.labels(method, path).observe(time.time() - start_time)
            raise e

async def monitor_system_resources():
    """
    Periodically log system resource usage
    """
    while True:
        
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage('/').percent
        
        
        if cpu_percent > 80 or memory_percent > 80 or disk_percent > 80:
            logger.warning(
                f"High resource usage: CPU: {cpu_percent}%, "
                f"Memory: {memory_percent}%, "
                f"Disk: {disk_percent}%"
            )
        
        
        logger.info(
            f"System resources: CPU: {cpu_percent}%, "
            f"Memory: {memory_percent}%, "
            f"Disk: {disk_percent}%"
        )
        
        await asyncio.sleep(60)  

def setup_monitoring(app: FastAPI):
    """
    Monitoring for the application
    """
    
    app.add_middleware(MonitoringMiddleware)
    
    
    metrics_port = int(os.environ.get("METRICS_PORT", "9090"))
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics available at http://localhost:{metrics_port}")
    
    
    @app.on_event("startup")
    async def start_monitoring():
        asyncio.create_task(monitor_system_resources())