import asyncio
import logging
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from .loader import MarketingDataLoader
from .config import load_config

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Marketing Data Loading Microservice",
    description="Event-driven microservice for loading marketing data from olist to data warehouse",
    version="1.0.0"
)

loader: MarketingDataLoader = None


@app.on_event("startup")
async def startup_event():
    global loader
    config = load_config()
    loader = MarketingDataLoader(config)
    await loader.initialize()
    logger.info("Marketing data service started")


@app.on_event("shutdown")
async def shutdown_event():
    global loader
    if loader:
        await loader.close()
    logger.info("Marketing data service stopped")


@app.get("/health")
async def health_check():
    if not loader:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    health_status = await loader.health_check()
    
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=503, detail=health_status)
    
    return health_status


@app.post("/load")
async def trigger_load(background_tasks: BackgroundTasks):
    if not loader:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        result = await loader.load_marketing_data()
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Load request failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/load-async")
async def trigger_load_async(background_tasks: BackgroundTasks):
    if not loader:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    background_tasks.add_task(run_load_in_background)
    return {"message": "Load started in background", "status": "accepted"}


async def run_load_in_background():
    try:
        result = await loader.load_marketing_data()
        logger.info(f"Background load completed: {result}")
    except Exception as e:
        logger.error(f"Background load failed: {str(e)}")


@app.get("/")
async def root():
    return {
        "service": "marketing-data-loader",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "load": "/load",
            "load_async": "/load-async"
        }
    }
