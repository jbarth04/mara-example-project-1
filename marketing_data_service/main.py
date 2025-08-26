import asyncio
import logging
import sys
import uvicorn
from .api import app
from .config import load_config


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('marketing_data_service.log')
        ]
    )


def main():
    config = load_config()
    setup_logging(config.log_level)
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Marketing Data Loading Microservice")
    
    uvicorn.run(
        "marketing_data_service.api:app",
        host="0.0.0.0",
        port=8000,
        log_level=config.log_level.lower(),
        reload=False
    )


if __name__ == "__main__":
    main()
