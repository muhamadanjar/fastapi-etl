"""
Messaging decorators for easy event handling and publishing.
"""

import asyncio
import functools
import logging
from typing import Callable, Any, Optional, Dict, List, Union
from datetime import datetime

from .manager import messaging_manager, MessagingType
from .base import Message, MessageHandler, MessageFilter

logger = logging.getLogger(__name__)


def message_handler(
    topic: str,
    messaging_type: MessagingType = MessagingType.REDIS,
    filters: Optional[List[MessageFilter]] = None
):
    """
    Decorator to register a function as a message handler.
    
    Args:
        topic: Topic to listen to
        messaging_type: Which messaging system to use
        filters: Optional message filters
    """
    def decorator(func: Callable[[Message], Any]):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        
        # Register handler on startup
        async def register_handler():
            messaging = messaging_manager.get_messaging(messaging_type)
            if messaging:
                await messaging.subscribe(topic, wrapper, filters)
                logger.info(f"Registered message handler for topic {topic}")
        
        # Store registration function for later use
        wrapper._register_handler = register_handler
        wrapper._topic = topic
        wrapper._messaging_type = messaging_type
        
        return wrapper
    
    return decorator


def publish_event(
    topic: str,
    messaging_type: MessagingType = MessagingType.ALL,
    extract_payload: Optional[Callable] = None,
    headers: Optional[Dict[str, Any]] = None
):
    """
    Decorator to automatically publish an event after function execution.
    
    Args:
        topic: Topic to publish to
        messaging_type: Which messaging systems to use
        extract_payload: Function to extract payload from function result
        headers: Static headers to include
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Execute function
            result = await func(*args, **kwargs)
            
            # Extract payload
            if extract_payload:
                payload = extract_payload(result)
            else:
                payload = result
            
            # Publish event
            try:
                await messaging_manager.publish(
                    topic=topic,
                    payload=payload,
                    messaging_type=messaging_type,
                    headers=headers,
                    source=func.__name__
                )
                logger.debug(f"Published event to topic {topic}")
            except Exception as e:
                logger.error(f"Failed to publish event: {e}")
            
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            # For sync functions, we need to handle async publishing
            async def publish_async():
                if extract_payload:
                    payload = extract_payload(result)
                else:
                    payload = result
                
                try:
                    await messaging_manager.publish(
                        topic=topic,
                        payload=payload,
                        messaging_type=messaging_type,
                        headers=headers,
                        source=func.__name__
                    )
                except Exception as e:
                    logger.error(f"Failed to publish event: {e}")
            
            # Schedule async publish
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(publish_async())
            except RuntimeError:
                logger.warning("No event loop available for sync function event publishing")
            
            return result
        
        # Return appropriate wrapper
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def subscribe_to(*topics: str, messaging_type: MessagingType = MessagingType.REDIS):
    """
    Decorator to subscribe a function to multiple topics.
    
    Args:
        topics: Topics to subscribe to
        messaging_type: Which messaging system to use
    """
    def decorator(func: Callable[[Message], Any]):
        @functools.wraps(func)
        async def wrapper(message: Message):
            return await func(message)
        
        # Register multiple subscriptions
        async def register_subscriptions():
            messaging = messaging_manager.get_messaging(messaging_type)
            if messaging:
                for topic in topics:
                    await messaging.subscribe(topic, wrapper)
                    logger.info(f"Subscribed to topic {topic}")
        
        wrapper._register_subscriptions = register_subscriptions
        wrapper._topics = topics
        wrapper._messaging_type = messaging_type
        
        return wrapper
    
    return decorator


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator to retry message handler on failure.
    
    Args:
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds
    """
    def decorator(func: MessageHandler):
        @functools.wraps(func)
        async def wrapper(message: Message):
            last_error = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(message)
                except Exception as e:
                    last_error = e
                    
                    if attempt < max_retries:
                        logger.warning(
                            f"Message handler failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
                        )
                        await asyncio.sleep(delay * (attempt + 1))  # Exponential backoff
                    else:
                        logger.error(f"Message handler failed after {max_retries + 1} attempts: {e}")
                        # Mark message as failed
                        message.mark_failed(str(e))
                        raise last_error
            
            raise last_error
        
        return wrapper
    
    return decorator