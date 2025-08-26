import json
import redis
import logging
from typing import Dict, Any, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

class EventPublisher:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
    
    def publish_event(self, event_type: str, data: Dict[str, Any]):
        event_payload = {
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data
        }
        
        message = json.dumps(event_payload)
        self.redis_client.publish(event_type, message)
        logger.info(f"Published event: {event_type} with data: {data}")

class EventSubscriber:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.handlers: Dict[str, Callable] = {}
    
    def subscribe(self, event_type: str, handler: Callable[[Dict[str, Any]], None]):
        self.handlers[event_type] = handler
        self.pubsub.subscribe(event_type)
        logger.info(f"Subscribed to event: {event_type}")
    
    def listen(self):
        logger.info("Starting event listener...")
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    event_payload = json.loads(message['data'])
                    event_type = event_payload['event_type']
                    
                    if event_type in self.handlers:
                        logger.info(f"Processing event: {event_type}")
                        self.handlers[event_type](event_payload['data'])
                    else:
                        logger.warning(f"No handler for event type: {event_type}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode event message: {e}")
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
