"""
Decorators for email system functionality.
"""

import asyncio
import functools
import time
import logging
from typing import Any, Callable, Dict, List, Optional, Union, TypeVar, cast
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from .base import EmailMessage, EmailStatus, EmailPriority

# Type variables for generic decorators
F = TypeVar('F', bound=Callable[..., Any])
T = TypeVar('T')

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry decorator."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    exceptions: tuple = (Exception,)


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    max_calls: int = 100
    time_window: int = 60  # seconds
    per_recipient: bool = False


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    recovery_timeout: int = 60
    expected_exception: tuple = (Exception,)


class CircuitBreakerState:
    """Circuit breaker state management."""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "closed"  # closed, open, half-open
    
    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == "closed":
            return True
        
        if self.state == "open":
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.config.recovery_timeout:
                    self.state = "half-open"
                    return True
            return False
        
        if self.state == "half-open":
            return True
        
        return False
    
    def record_success(self):
        """Record successful execution."""
        self.failure_count = 0
        self.state = "closed"
        self.last_failure_time = None
    
    def record_failure(self):
        """Record failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.failure_count >= self.config.failure_threshold:
            self.state = "open"


def retry_on_failure(config: Optional[RetryConfig] = None):
    """
    Retry decorator for email operations.
    
    Args:
        config: Retry configuration
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                        
                except config.exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts - 1:
                        logger.error(f"Max retry attempts ({config.max_attempts}) reached for {func.__name__}: {e}")
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        config.base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    if config.jitter:
                        import random
                        delay *= (0.5 + random.random() * 0.5)
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}, retrying in {delay:.2f}s: {e}")
                    await asyncio.sleep(delay)
            
            if last_exception:
                raise last_exception
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                        
                except config.exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts - 1:
                        logger.error(f"Max retry attempts ({config.max_attempts}) reached for {func.__name__}: {e}")
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        config.base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    if config.jitter:
                        import random
                        delay *= (0.5 + random.random() * 0.5)
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}, retrying in {delay:.2f}s: {e}")
                    time.sleep(delay)
            
            if last_exception:
                raise last_exception
        
        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        else:
            return cast(F, sync_wrapper)
    
    return decorator


def rate_limit(config: Optional[RateLimitConfig] = None):
    """
    Rate limiting decorator for email operations.
    
    Args:
        config: Rate limiting configuration
    """
    if config is None:
        config = RateLimitConfig()
    
    # Store call history
    call_history: Dict[str, List[float]] = {}
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            current_time = time.time()
            
            # Determine key for rate limiting
            if config.per_recipient and len(args) > 0:
                # Try to extract email from first argument (EmailMessage)
                if hasattr(args[0], 'to') and args[0].to:
                    key = f"{func.__name__}:{args[0].to[0]}"
                else:
                    key = func.__name__
            else:
                key = func.__name__
            
            # Initialize history for this key
            if key not in call_history:
                call_history[key] = []
            
            # Clean old entries
            cutoff_time = current_time - config.time_window
            call_history[key] = [t for t in call_history[key] if t > cutoff_time]
            
            # Check rate limit
            if len(call_history[key]) >= config.max_calls:
                oldest_call = min(call_history[key])
                wait_time = config.time_window - (current_time - oldest_call)
                
                if wait_time > 0:
                    logger.warning(f"Rate limit reached for {key}, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
            
            # Record this call
            call_history[key].append(current_time)
            
            # Execute function
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            current_time = time.time()
            
            # Determine key for rate limiting
            if config.per_recipient and len(args) > 0:
                # Try to extract email from first argument (EmailMessage)
                if hasattr(args[0], 'to') and args[0].to:
                    key = f"{func.__name__}:{args[0].to[0]}"
                else:
                    key = func.__name__
            else:
                key = func.__name__
            
            # Initialize history for this key
            if key not in call_history:
                call_history[key] = []
            
            # Clean old entries
            cutoff_time = current_time - config.time_window
            call_history[key] = [t for t in call_history[key] if t > cutoff_time]
            
            # Check rate limit
            if len(call_history[key]) >= config.max_calls:
                oldest_call = min(call_history[key])
                wait_time = config.time_window - (current_time - oldest_call)
                
                if wait_time > 0:
                    logger.warning(f"Rate limit reached for {key}, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
            
            # Record this call
            call_history[key].append(current_time)
            
            # Execute function
            return func(*args, **kwargs)
        
        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        else:
            return cast(F, sync_wrapper)
    
    return decorator


def circuit_breaker(config: Optional[CircuitBreakerConfig] = None):
    """
    Circuit breaker decorator for email operations.
    
    Args:
        config: Circuit breaker configuration
    """
    if config is None:
        config = CircuitBreakerConfig()
    
    state = CircuitBreakerState(config)
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not state.can_execute():
                raise Exception(f"Circuit breaker is OPEN for {func.__name__}")
            
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                state.record_success()
                return result
                
            except config.expected_exception as e:
                state.record_failure()
                logger.error(f"Circuit breaker recorded failure for {func.__name__}: {e}")
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not state.can_execute():
                raise Exception(f"Circuit breaker is OPEN for {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                state.record_success()
                return result
                
            except config.expected_exception as e:
                state.record_failure()
                logger.error(f"Circuit breaker recorded failure for {func.__name__}: {e}")
                raise
        
        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        else:
            return cast(F, sync_wrapper)
    
    return decorator


def validate_email_message(func: F) -> F:
    """
    Validate email message before processing.
    
    Args:
        func: Function to decorate
    """
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        # Find EmailMessage in arguments
        message = None
        for arg in args:
            if isinstance(arg, EmailMessage):
                message = arg
                break
        
        if message is None:
            # Check kwargs
            for value in kwargs.values():
                if isinstance(value, EmailMessage):
                    message = value
                    break
        
        if message:
            # Validate message
            if not message.to:
                raise ValueError("Email message must have at least one recipient")
            
            if not message.subject and not message.body and not message.html_body:
                raise ValueError("Email message must have subject or body content")
            
            if message.is_expired():
                raise ValueError("Email message has expired")
            
            # Validate email addresses
            import re
            email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            
            all_recipients = message.get_all_recipients()
            for email in all_recipients:
                if not email_pattern.match(email):
                    raise ValueError(f"Invalid email address: {email}")
        
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)
    
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        # Find EmailMessage in arguments
        message = None
        for arg in args:
            if isinstance(arg, EmailMessage):
                message = arg
                break
        
        if message is None:
            # Check kwargs
            for value in kwargs.values():
                if isinstance(value, EmailMessage):
                    message = value
                    break
        
        if message:
            # Validate message
            if not message.to:
                raise ValueError("Email message must have at least one recipient")
            
            if not message.subject and not message.body and not message.html_body:
                raise ValueError("Email message must have subject or body content")
            
            if message.is_expired():
                raise ValueError("Email message has expired")
            
            # Validate email addresses
            import re
            email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            
            all_recipients = message.get_all_recipients()
            for email in all_recipients:
                if not email_pattern.match(email):
                    raise ValueError(f"Invalid email address: {email}")
        
        return func(*args, **kwargs)
    
    if asyncio.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    else:
        return cast(F, sync_wrapper)


def track_email_metrics(func: F) -> F:
    """
    Track email sending metrics.
    
    Args:
        func: Function to decorate
    """
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        
        # Extract message info for tracking
        message = None
        for arg in args:
            if isinstance(arg, EmailMessage):
                message = arg
                break
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Track success metrics
            execution_time = time.time() - start_time
            logger.info(f"Email operation {func.__name__} completed in {execution_time:.3f}s")
            
            if message:
                logger.info(f"Email sent successfully: {message.id} to {len(message.get_all_recipients())} recipients")
            
            return result
            
        except Exception as e:
            # Track failure metrics
            execution_time = time.time() - start_time
            logger.error(f"Email operation {func.__name__} failed after {execution_time:.3f}s: {e}")
            
            if message:
                logger.error(f"Email failed: {message.id} - {str(e)}")
            
            raise
    
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.time()
        
        # Extract message info for tracking
        message = None
        for arg in args:
            if isinstance(arg, EmailMessage):
                message = arg
                break
        
        try:
            result = func(*args, **kwargs)
            
            # Track success metrics
            execution_time = time.time() - start_time
            logger.info(f"Email operation {func.__name__} completed in {execution_time:.3f}s")
            
            if message:
                logger.info(f"Email sent successfully: {message.id} to {len(message.get_all_recipients())} recipients")
            
            return result
            
        except Exception as e:
            # Track failure metrics
            execution_time = time.time() - start_time
            logger.error(f"Email operation {func.__name__} failed after {execution_time:.3f}s: {e}")
            
            if message:
                logger.error(f"Email failed: {message.id} - {str(e)}")
            
            raise
    
    if asyncio.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    else:
        return cast(F, sync_wrapper)


def priority_queue_handler(func: F) -> F:
    """
    Handle email priority in queue processing.
    
    Args:
        func: Function to decorate
    """
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        # Extract messages and sort by priority
        messages = []
        
        for arg in args:
            if isinstance(arg, list) and all(isinstance(item, EmailMessage) for item in arg):
                messages = arg
                break
        
        if messages:
            # Sort by priority (higher priority first)
            messages.sort(key=lambda msg: msg.priority.value, reverse=True)
            
            # Log priority handling
            priority_counts = {}
            for msg in messages:
                priority_counts[msg.priority] = priority_counts.get(msg.priority, 0) + 1
            
            logger.info(f"Processing {len(messages)} emails by priority: {priority_counts}")
        
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)
    
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        # Extract messages and sort by priority
        messages = []
        
        for arg in args:
            if isinstance(arg, list) and all(isinstance(item, EmailMessage) for item in arg):
                messages = arg
                break
        
        if messages:
            # Sort by priority (higher priority first)
            messages.sort(key=lambda msg: msg.priority.value, reverse=True)
            
            # Log priority handling
            priority_counts = {}
            for msg in messages:
                priority_counts[msg.priority] = priority_counts.get(msg.priority, 0) + 1
            
            logger.info(f"Processing {len(messages)} emails by priority: {priority_counts}")
        
        return func(*args, **kwargs)
    
    if asyncio.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    else:
        return cast(F, sync_wrapper)


# Composite decorators for common use cases
def robust_email_sender(
    retry_config: Optional[RetryConfig] = None,
    rate_limit_config: Optional[RateLimitConfig] = None,
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None
):
    """
    Composite decorator for robust email sending.
    
    Args:
        retry_config: Retry configuration
        rate_limit_config: Rate limiting configuration
        circuit_breaker_config: Circuit breaker configuration
    """
    def decorator(func: F) -> F:
        # Apply decorators in order
        decorated_func = func
        
        if circuit_breaker_config:
            decorated_func = circuit_breaker(circuit_breaker_config)(decorated_func)
        
        if rate_limit_config:
            decorated_func = rate_limit(rate_limit_config)(decorated_func)
        
        if retry_config:
            decorated_func = retry_on_failure(retry_config)(decorated_func)
        
        decorated_func = validate_email_message(decorated_func)
        decorated_func = track_email_metrics(decorated_func)
        
        return decorated_func
    
    return decorator