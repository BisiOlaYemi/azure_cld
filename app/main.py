from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import router
from app.core.monitoring import setup_monitoring
import logging
from config.logging_config import setup_logging


setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Azure Data Pipeline",
    description="Real-time data processing and migration to Azure cloud",
    version="1.0.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


setup_monitoring(app)


app.include_router(router)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting Azure Data Pipeline service")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down Azure Data Pipeline service")