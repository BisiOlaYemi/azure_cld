import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from config.settings import settings

def setup_logging():
    """Configure logging for the application"""
    
    os.makedirs("logs", exist_ok=True)
    
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    
    # root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            
            logging.StreamHandler(sys.stdout),
            
            RotatingFileHandler(
                "logs/azure_data_pipeline.log",
                maxBytes=10*1024*1024,  
                backupCount=5
            )
        ]
    )
    
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logger = logging.getLogger("azure_data_pipeline")
    logger.setLevel(log_level)
    
    return logger