"""
Example integration with existing Mara pipeline

This module demonstrates how to integrate the marketing data microservice
with the existing Mara pipeline during the transition period.
"""

import requests
import logging
from mara_pipelines.commands.python import RunFunction
from mara_pipelines.pipelines import Task

logger = logging.getLogger(__name__)


def call_marketing_microservice(service_url: str = "http://localhost:8000") -> bool:
    """
    Call the marketing data microservice from within a Mara pipeline.
    
    Args:
        service_url: URL of the marketing data microservice
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        response = requests.post(f"{service_url}/load", timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Marketing data load completed successfully: {result}")
            
            if result.get('failed_tables', 0) > 0:
                logger.warning(f"Some tables failed to load: {result}")
                return False
                
            return True
        else:
            logger.error(f"Marketing data load failed: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call marketing microservice: {str(e)}")
        return False


def create_microservice_task() -> Task:
    """
    Create a Mara task that calls the marketing data microservice.
    
    This can be used to replace the existing load_marketing_data pipeline
    during the transition period.
    """
    return Task(
        id="load_marketing_data_microservice",
        description="Load marketing data using the microservice",
        commands=[
            RunFunction(function=call_marketing_microservice)
        ]
    )


def health_check_microservice(service_url: str = "http://localhost:8000") -> bool:
    """
    Check the health of the marketing data microservice.
    
    Args:
        service_url: URL of the marketing data microservice
        
    Returns:
        bool: True if healthy, False otherwise
    """
    try:
        response = requests.get(f"{service_url}/health", timeout=30)
        
        if response.status_code == 200:
            health = response.json()
            logger.info(f"Microservice health check: {health}")
            return health.get('status') == 'healthy'
        else:
            logger.error(f"Health check failed: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check microservice health: {str(e)}")
        return False
